import { Injectable } from '@nestjs/common';
import { createRequire } from 'module';
import OpenAI from 'openai';
import { Subject } from 'rxjs';
import { v4 as uuid } from 'uuid';
import {
  DIRECT_QA_API_KEY,
  DIRECT_QA_BASE_URL,
  DIRECT_QA_EMBEDDING_DIMENSIONS,
  DIRECT_QA_EMBEDDING_MODEL,
  DIRECT_QA_MODEL,
  OPENCLAW_QA_MODE,
} from './config.js';
import { OpenClawService } from './openclaw.service.js';
import { QaSessionSourcesService } from './qa-session-sources.service.js';
import { ResearchKeysService } from './research-keys.service.js';
import type { ServerEvent } from './types.js';

type PgPool = {
  query: (text: string, params?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
  end: () => Promise<void>;
};

interface ChatRequest {
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>;
  stream?: boolean;
  sessionId?: string;
}

const require = createRequire(import.meta.url);
const PG_SOURCE_TABLE = process.env.PGVECTOR_NEWS_TABLE || 'vector_materials_text_embedding_v4';
const VECTOR_RECALL_TIMEOUT_MS = Math.max(3000, Number(process.env.DIRECT_QA_VECTOR_TIMEOUT_MS || 8000));
const KEYWORD_RECALL_TIMEOUT_MS = Math.max(1500, Number(process.env.DIRECT_QA_KEYWORD_TIMEOUT_MS || 3500));
const SOURCE_RECALL_CACHE_TTL_MS = Math.max(30_000, Number(process.env.DIRECT_QA_SOURCE_CACHE_TTL_MS || 600_000));
const SOURCE_RECALL_CACHE_MAX = Math.max(10, Number(process.env.DIRECT_QA_SOURCE_CACHE_MAX || 200));
const VECTOR_CANDIDATE_LIMIT = Math.max(32, Math.min(120, Number(process.env.DIRECT_QA_VECTOR_CANDIDATE_LIMIT || 72)));
const KEYWORD_CANDIDATE_LIMIT = Math.max(40, Math.min(160, Number(process.env.DIRECT_QA_KEYWORD_CANDIDATE_LIMIT || 80)));

@Injectable()
export class ChatService {
  private readonly streams = new Map<string, Subject<ServerEvent>>();
  private readonly history = new Map<string, ServerEvent[]>();
  private directClient: OpenAI | null = null;
  private directClientKey = '';
  private pgPool: PgPool | null = null;
  private readonly embeddingCache = new Map<string, number[]>();
  private readonly sourceRecallCache = new Map<string, { expiresAt: number; sources: Record<string, unknown>[] }>();
  private readonly pendingSourceRecalls = new Map<string, Promise<Record<string, unknown>[]>>();

  constructor(
    private readonly openClaw: OpenClawService,
    private readonly qaSources: QaSessionSourcesService,
    private readonly researchKeys: ResearchKeysService,
  ) {}

  async complete(body: ChatRequest) {
    if (body.stream) {
      const streamId = uuid();
      this.streams.set(streamId, new Subject<ServerEvent>());
      this.history.set(streamId, []);
      setImmediate(() => void this.runStream(streamId, body.messages, body.sessionId));
      return { streamId, eventsUrl: `/api/chat/streams/${streamId}` };
    }

    const events: ServerEvent[] = [];
    const text = await this.completeQa(body.messages, (event) => events.push(event), body.sessionId);
    return {
      choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
      events,
    };
  }

  stream(streamId: string) {
    return {
      events: this.history.get(streamId),
      subject: this.streams.get(streamId),
    };
  }

  private async runStream(streamId: string, messages: ChatRequest['messages'], sessionId?: string) {
    try {
      await this.completeQa(messages, (event) => this.push(streamId, event), sessionId);
      this.push(streamId, { type: 'done', jobId: streamId });
      this.streams.get(streamId)?.complete();
    } catch (error) {
      this.push(streamId, { type: 'error', message: error instanceof Error ? error.message : String(error) });
      this.streams.get(streamId)?.complete();
    }
  }

  private push(streamId: string, event: ServerEvent) {
    this.history.get(streamId)?.push(event);
    this.streams.get(streamId)?.next(event);
  }

  private async completeQa(
    messages: ChatRequest['messages'],
    onEvent: (event: ServerEvent) => void,
    sessionId?: string,
  ): Promise<string> {
    if (OPENCLAW_QA_MODE === 'direct_pg') {
      try {
        return await this.streamQaWithPgContext(messages, onEvent, sessionId);
      } catch (error) {
        onEvent({
          type: 'status',
          status: 'fallback',
          message: 'PG 检索直连链路暂不可用，已切换备用问答链路。',
        });
        return this.streamQaViaAgent(messages, onEvent, sessionId);
      }
    }
    return this.streamQaViaAgent(messages, onEvent, sessionId);
  }

  private async streamQaViaAgent(
    messages: ChatRequest['messages'],
    onEvent: (event: ServerEvent) => void,
    sessionId?: string,
  ): Promise<string> {
    const text = await this.openClaw.streamQa(messages, onEvent, sessionId);
    const sourceEvent = await this.buildQaSourcesEvent(sessionId, messages);
    if (sourceEvent) onEvent(sourceEvent);
    return text;
  }

  private async streamQaWithPgContext(
    messages: ChatRequest['messages'],
    onEvent: (event: ServerEvent) => void,
    sessionId?: string,
  ): Promise<string> {
    const client = await this.getDirectClient();
    const question = this.lastUserMessage(messages);
    const topic = this.buildQuestionEmbeddingText(messages);
    onEvent({ type: 'stage', stage: 'retrieval_started', message: '正在执行 PG 向量召回' });
    const sources = await this.recallPgSources(question, topic);
    if (sources.length) {
      onEvent({ type: 'sources', sources });
      if (sessionId) await this.qaSources.upsertSources(sessionId, { sources, merge: true });
    }
    onEvent({
      type: 'stage',
      stage: 'retrieval_done',
      message: sources.length ? `已向量召回 ${sources.length} 条 PG 信源，正在生成回答` : 'PG 向量召回未命中足够材料，正在生成回答',
    });

    const stream = await client.chat.completions.create({
      model: DIRECT_QA_MODEL,
      messages: this.buildPgGroundedMessages(messages, sources),
      stream: true,
      temperature: 0.2,
      max_tokens: this.directAnswerMaxTokens(messages),
    });

    let text = '';
    let started = false;
    for await (const chunk of stream) {
      for (const choice of chunk.choices || []) {
        const content = typeof choice.delta?.content === 'string' ? choice.delta.content : '';
        if (!content) continue;
        if (!started) {
          started = true;
          onEvent({ type: 'stage', stage: 'synthesis_started', message: '正在生成回答' });
        }
        text += content;
        onEvent({ type: 'text_delta', content });
        onEvent({ type: 'token', content });
      }
    }
    return text.trim();
  }

  private buildPgGroundedMessages(messages: ChatRequest['messages'], sources: Record<string, unknown>[]): ChatRequest['messages'] {
    const originalSystem = messages.find((item) => item.role === 'system')?.content || '';
    const conversation = messages.filter((item) => item.role !== 'system').slice(-6);
    const answerLength = this.directAnswerMaxTokens(messages) > 900 ? '900-1400 字' : '400-800 字';
    const sourceBlock = sources.length
      ? sources.map((source, index) => {
          return [
            `[${index + 1}] ${source.title || '未命名信源'}`,
            `来源：${source.websiteName || '未知'} ${source.publishTime || ''}`,
            `摘要：${this.clean(String(source.summary || source.contentExcerpt || '暂无摘要'), 260)}`,
            source.url ? `链接：${source.url}` : '',
          ].filter(Boolean).join('\n');
        }).join('\n\n')
      : '本次检索未找到足够匹配材料。';

    return [
      {
        role: 'system',
        content: [
          originalSystem,
          '你是热点事件动态感知助手。必须优先依据下方参考材料回答；材料不足时要明确说明“现有材料不足以确认”，不要编造事实。',
          `回答要求：使用 Markdown；默认控制在 ${answerLength}；先给结论，再列关键依据和影响判断；“关键依据”中的每条事实必须标注来源编号，如 [1]；不要提及 SQL、表名、MCP、向量、模型、接口、系统实现或检索过程。`,
          '结构建议：**结论**、**关键依据**、**影响判断**。如果用户问题很简单，可以合并为更短的段落。',
          '',
          '参考材料：',
          sourceBlock,
        ].filter(Boolean).join('\n'),
      },
      ...conversation,
    ];
  }

  private directAnswerMaxTokens(messages: ChatRequest['messages']): number {
    const question = this.lastUserMessage(messages);
    return /详细|深入|展开|全面|完整|系统|长篇|多角度/.test(question) ? 1400 : 900;
  }

  private buildQuestionEmbeddingText(messages: ChatRequest['messages']): string {
    const question = this.lastUserMessage(messages);
    const recent = messages
      .filter((item) => item.role === 'user' || item.role === 'assistant')
      .slice(-5)
      .map((item) => `${item.role === 'user' ? '用户' : '回答'}: ${String(item.content || '').replace(/\s+/g, ' ').trim().slice(0, 220)}`)
      .filter((line) => line.length > 8)
      .join('\n');
    const terms = this.extractFocusedPgSearchTerms(`${recent}\n${question}`).slice(0, 18).join(' ');
    return [
      `主题：${question}`,
      terms ? `关键词：${terms}` : '',
      recent ? `近期上下文：\n${recent}` : '',
    ].filter(Boolean).join('\n').slice(0, 900);
  }

  private async recallPgSources(question: string, embeddingText = question): Promise<Record<string, unknown>[]> {
    const cacheKey = this.sourceRecallCacheKey(question, embeddingText);
    if (!cacheKey) return [];

    const cached = this.sourceRecallCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return cached.sources;

    const pending = this.pendingSourceRecalls.get(cacheKey);
    if (pending) return pending;

    const recall = this.recallPgSourcesUncached(question, embeddingText)
      .then((sources) => {
        if (sources.length) this.rememberSourceRecall(cacheKey, sources);
        return sources;
      })
      .finally(() => this.pendingSourceRecalls.delete(cacheKey));
    this.pendingSourceRecalls.set(cacheKey, recall);
    return recall;
  }

  private async recallPgSourcesUncached(question: string, embeddingText: string): Promise<Record<string, unknown>[]> {
    const [vectorResult, keywordResult] = await Promise.allSettled([
      this.withTimeout(
        this.searchPgVectorSources(question, embeddingText),
        VECTOR_RECALL_TIMEOUT_MS,
        'PG vector recall timed out',
      ),
      this.withTimeout(
        this.searchPgKeywordSources(question),
        KEYWORD_RECALL_TIMEOUT_MS,
        'PG keyword recall timed out',
      ),
    ]);

    const vectorSources = vectorResult.status === 'fulfilled' ? vectorResult.value : [];
    const keywordSources = keywordResult.status === 'fulfilled' ? keywordResult.value : [];
    return this.mergeRecallSources(vectorSources, keywordSources);
  }

  private async searchPgVectorSources(question: string, embeddingText: string): Promise<Record<string, unknown>[]> {
    const embedding = await this.embedQuestion(embeddingText);
    if (!embedding.length) return [];
    const vector = this.toVectorLiteral(embedding);
    const pool = await this.getPgPool();
    const terms = this.extractFocusedPgSearchTerms(question);
    const vectorQueries: Array<Promise<{ rows: Array<Record<string, unknown>> }>> = [];
    const topicTerms = terms.slice(0, 8);
    if (topicTerms.length) {
      const params: unknown[] = [vector];
      const clauses: string[] = [];
      for (const term of topicTerms) {
        params.push(`%${term}%`);
        const placeholder = `$${params.length}`;
        clauses.push(`(ch_title ILIKE ${placeholder} OR entitle ILIKE ${placeholder} OR summary ILIKE ${placeholder} OR content ILIKE ${placeholder} OR embedding_text ILIKE ${placeholder})`);
      }
      vectorQueries.push(pool.query(
        `SELECT ch_title, entitle, data_source_url, website_name, publish_time, summary, content_excerpt, content,
                1 - (embedding_vector <=> $1::vector) AS similarity
           FROM ${this.qi(PG_SOURCE_TABLE)}
          WHERE embedding_vector IS NOT NULL
            AND (${clauses.join(' OR ')})
          ORDER BY embedding_vector <=> $1::vector
          LIMIT ${Math.min(48, VECTOR_CANDIDATE_LIMIT)}`,
        params,
      ));
    }
    vectorQueries.push(pool.query(
      `SELECT ch_title, entitle, data_source_url, website_name, publish_time, summary, content_excerpt, content,
              1 - (embedding_vector <=> $1::vector) AS similarity
         FROM ${this.qi(PG_SOURCE_TABLE)}
        WHERE embedding_vector IS NOT NULL
        ORDER BY embedding_vector <=> $1::vector
        LIMIT ${VECTOR_CANDIDATE_LIMIT}`,
      [vector],
    ));
    const settled = await Promise.allSettled(vectorQueries);
    const candidateRows = settled.flatMap((result) => result.status === 'fulfilled' ? result.value.rows : []);
    const scored = candidateRows
      .map((row) => this.normalizePgRow(row, terms, 'pg_vector'))
      .filter((item) => item.title || item.summary || item.url)
      .sort((a, b) => Number(b.relevance || 0) - Number(a.relevance || 0));
    return this.dedupeSources(scored);
  }

  private async searchPgKeywordSources(question: string): Promise<Record<string, unknown>[]> {
    const terms = this.extractFocusedPgSearchTerms(question);
    if (!terms.length) return [];
    const pool = await this.getPgPool();
    const clauses: string[] = [];
    const params: unknown[] = [];
    for (const term of terms.slice(0, 12)) {
      params.push(`%${term}%`);
      const placeholder = `$${params.length}`;
      clauses.push(`(ch_title ILIKE ${placeholder} OR entitle ILIKE ${placeholder} OR summary ILIKE ${placeholder} OR content ILIKE ${placeholder} OR embedding_text ILIKE ${placeholder})`);
    }
    const scoreExpression = clauses.map((clause) => `(CASE WHEN ${clause} THEN 1 ELSE 0 END)`).join(' + ');
    const minimumHits = terms.length >= 4 ? 2 : 1;
    const rows = await this.withTimeout(
      pool.query(
        `SELECT ch_title, entitle, data_source_url, website_name, publish_time, summary, content_excerpt, content,
                (${scoreExpression}) AS keyword_score
           FROM ${this.qi(PG_SOURCE_TABLE)}
          WHERE (${clauses.join(' OR ')})
            AND (${scoreExpression}) >= ${minimumHits}
          ORDER BY keyword_score DESC, publish_time DESC NULLS LAST, indexed_at DESC NULLS LAST
          LIMIT ${KEYWORD_CANDIDATE_LIMIT}`,
        params,
      ),
      4500,
      'PG source query timed out',
    );
    const scored = rows.rows
      .map((row) => this.normalizePgRow(row, terms, 'pg_keyword_supplement'))
      .filter((item) => item.title || item.summary || item.url)
      .sort((a, b) => Number(b.relevance || 0) - Number(a.relevance || 0));
    return this.dedupeSources(scored);
  }

  private async embedQuestion(question: string): Promise<number[]> {
    const client = await this.getDirectClient();
    const text = String(question || '').replace(/\s+/g, ' ').trim().slice(0, 900);
    if (!text) return [];
    const cacheKey = `${DIRECT_QA_EMBEDDING_MODEL}:${DIRECT_QA_EMBEDDING_DIMENSIONS}:${text}`;
    const cached = this.embeddingCache.get(cacheKey);
    if (cached) return cached;
    const response = await client.embeddings.create({
      model: DIRECT_QA_EMBEDDING_MODEL,
      input: [text],
      ...(DIRECT_QA_EMBEDDING_DIMENSIONS ? { dimensions: DIRECT_QA_EMBEDDING_DIMENSIONS } : {}),
    }, { timeout: VECTOR_RECALL_TIMEOUT_MS });
    const vector = response.data[0]?.embedding || [];
    if (vector.length) {
      this.embeddingCache.set(cacheKey, vector);
      if (this.embeddingCache.size > 100) {
        const firstKey = this.embeddingCache.keys().next().value;
        if (firstKey) this.embeddingCache.delete(firstKey);
      }
    }
    return vector;
  }

  private async getDirectClient(): Promise<OpenAI> {
    const apiKey = DIRECT_QA_API_KEY || await this.researchKeys.getEffectiveKey('openaiEmbeddingApiKey');
    if (!apiKey) throw new Error('DIRECT_QA_API_KEY is not configured');
    if (!this.directClient || this.directClientKey !== apiKey) {
      this.directClient = new OpenAI({ apiKey, baseURL: DIRECT_QA_BASE_URL });
      this.directClientKey = apiKey;
    }
    return this.directClient;
  }

  private normalizePgRow(row: Record<string, unknown>, terms: string[], method: 'pg_vector' | 'pg_keyword_supplement'): Record<string, unknown> {
    const title = this.clean(String(row.ch_title || row.entitle || ''), 300);
    const summary = this.clean(String(row.summary || row.content_excerpt || row.content || ''), 800);
    const contentExcerpt = this.clean(String(row.content_excerpt || row.content || ''), 800);
    const websiteName = this.clean(String(row.website_name || ''), 120);
    const url = this.clean(String(row.data_source_url || ''), 500);
    const haystack = `${title} ${summary} ${contentExcerpt} ${websiteName}`.toLowerCase();
    const hits = terms.filter((term) => haystack.includes(term.toLowerCase())).length;
    const titleHits = terms.filter((term) => title.toLowerCase().includes(term.toLowerCase())).length;
    const similarity = Number(row.similarity || 0);
    const vectorRelevance = similarity + Math.min(hits, 5) * 0.08 + Math.min(titleHits, 3) * 0.05 - (terms.length >= 3 && hits === 0 ? 0.18 : 0);
    return {
      id: `${method}-${Buffer.from(url || title || summary).toString('base64url').slice(0, 18)}`,
      title,
      url,
      summary,
      contentExcerpt,
      websiteName,
      publishTime: this.dateString(row.publish_time),
      relevance: method === 'pg_vector' ? Number(vectorRelevance.toFixed(4)) : hits + titleHits * 2,
      similarity: method === 'pg_vector' ? Number(similarity.toFixed(4)) : undefined,
      sourceType: 'PG信源库',
      sourceOrigin: 'database_recall',
      method,
      status: 'hit',
    };
  }

  private extractFocusedPgSearchTerms(question: string): string[] {
    const text = String(question || '').normalize('NFKC').replace(/\s+/g, ' ').trim();
    const terms = new Set<string>();
    const stopwords = /^(请|简要|说明|分析|影响|什么|如何|怎么|为什么|是否|有关|关于|近期|最近|今天|一个|一下|以及|还有|这个|那个|问题|回答|用户)$/;
    const addTerm = (value: string) => {
      const item = value.replace(/^[的了和与及对在从把被将是为就都而或、，。；：！？\s]+|[的了和与及对在从把被将是为就都而或、，。；：！？\s]+$/g, '').trim();
      if (item.length < 2 || item.length > 24 || stopwords.test(item)) return;
      terms.add(item);
    };

    const parts = text
      .split(/[^\p{Script=Han}a-zA-Z0-9]+|以及|还有|关于|有关|请|简要|说明|分析|影响|什么|如何|怎么|为什么|是否|对|和|与|及|的/gu)
      .map((part) => part.trim())
      .filter(Boolean);
    for (const part of parts) addTerm(part);

    const compact = text.replace(/\s+/g, '');
    for (const match of compact.matchAll(/[\p{Script=Han}A-Za-z0-9]{2,12}(?:调查|服务费|造船业|航运|港口|关税|制裁|法案|政策|风险|产业链|供应链)/gu)) {
      addTerm(match[0]);
    }
    for (const match of text.matchAll(/[A-Za-z0-9][A-Za-z0-9-]{1,}/g)) {
      addTerm(match[0]);
    }

    for (const part of parts) {
      if (!/[\p{Script=Han}]/u.test(part) || part.length <= 4) continue;
      for (let size = Math.min(6, part.length); size >= 2 && terms.size < 32; size -= 1) {
        for (let index = 0; index <= part.length - size && terms.size < 32; index += 1) {
          addTerm(part.slice(index, index + size));
        }
      }
    }
    return Array.from(terms).slice(0, 32);
  }

  private extractPgSearchTerms(question: string): string[] {
    const text = String(question || '').replace(/\s+/g, ' ').trim();
    const terms = new Set<string>();
    for (const part of text.split(/[^\p{Script=Han}a-zA-Z0-9]+/u)) {
      const item = part.trim();
      if (item.length >= 2 && item.length <= 24) terms.add(item);
      if (/[\p{Script=Han}]/u.test(item) && item.length > 4) {
        for (let size = 2; size <= 4; size += 1) {
          for (let index = 0; index <= item.length - size && terms.size < 24; index += 1) {
            terms.add(item.slice(index, index + size));
          }
        }
      }
    }
    return Array.from(terms).filter((term) => !/^(请|一下|近期|今天|什么|怎么|如何|概括|关注)$/.test(term)).slice(0, 24);
  }

  private dedupeSources(items: Record<string, unknown>[]): Record<string, unknown>[] {
    const seen = new Set<string>();
    const result: Record<string, unknown>[] = [];
    for (const item of items) {
      const key = String(item.url || item.title || item.summary || '');
      if (!key || seen.has(key)) continue;
      seen.add(key);
      result.push(item);
    }
    return result;
  }

  private mergeRecallSources(
    vectorSources: Record<string, unknown>[],
    keywordSources: Record<string, unknown>[],
  ): Record<string, unknown>[] {
    const merged: Record<string, unknown>[] = [];
    const seen = new Set<string>();
    const add = (items: Record<string, unknown>[], limit: number) => {
      for (const item of items) {
        if (merged.length >= limit) break;
        const key = String(item.url || item.title || item.summary || '');
        if (!key || seen.has(key)) continue;
        seen.add(key);
        merged.push(item);
      }
    };

    const keywordLead = Math.min(4, keywordSources.length);
    add(keywordSources, keywordLead);
    add(vectorSources, keywordLead + vectorSources.length);
    add(keywordSources.slice(keywordLead), keywordLead + vectorSources.length + keywordSources.length);
    return merged;
  }

  private sourceRecallCacheKey(question: string, embeddingText = question): string {
    const normalized = String(embeddingText || question || '')
      .normalize('NFKC')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase()
      .slice(0, 900);
    return normalized ? `${DIRECT_QA_EMBEDDING_MODEL}:${DIRECT_QA_EMBEDDING_DIMENSIONS}:${PG_SOURCE_TABLE}:${normalized}` : '';
  }

  private rememberSourceRecall(cacheKey: string, sources: Record<string, unknown>[]): void {
    this.sourceRecallCache.set(cacheKey, {
      expiresAt: Date.now() + SOURCE_RECALL_CACHE_TTL_MS,
      sources,
    });
    while (this.sourceRecallCache.size > SOURCE_RECALL_CACHE_MAX) {
      const firstKey = this.sourceRecallCache.keys().next().value;
      if (!firstKey) break;
      this.sourceRecallCache.delete(firstKey);
    }
  }

  private async getPgPool(): Promise<PgPool> {
    if (this.pgPool) return this.pgPool;
    const connectionString = process.env.PGVECTOR_DATABASE_URL || process.env.POSTGRES_DATABASE_URL || process.env.DATABASE_URL;
    if (!connectionString) throw new Error('PGVECTOR_DATABASE_URL is not configured');
    const { Pool } = require('pg') as { Pool: new (config: Record<string, unknown>) => PgPool };
    this.pgPool = new Pool({
      connectionString,
      max: 4,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 2500,
      query_timeout: 6500,
      statement_timeout: 6500,
    });
    return this.pgPool;
  }

  private withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), timeoutMs);
      timer.unref?.();
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }

  private qi(value: string): string {
    return `"${String(value).replace(/"/g, '""')}"`;
  }

  private toVectorLiteral(vector: number[]): string {
    return `[${vector.map((value) => Number(value).toFixed(8)).join(',')}]`;
  }

  private clean(value: string, maxLength: number): string {
    const text = String(value || '').replace(/\s+/g, ' ').trim();
    return text.length > maxLength ? `${text.slice(0, maxLength - 1)}…` : text;
  }

  private dateString(value: unknown): string {
    if (!value) return '';
    const date = value instanceof Date ? value : new Date(String(value));
    return Number.isNaN(date.getTime()) ? String(value) : date.toISOString();
  }

  private lastUserMessage(messages: ChatRequest['messages']): string {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const item = messages[index];
      if (item?.role === 'user' && item.content?.trim()) return item.content.trim();
    }
    return messages.map((item) => item.content).filter(Boolean).join('\n').slice(-1000);
  }

  private async buildQaSourcesEvent(sessionId?: string, messages: ChatRequest['messages'] = []): Promise<ServerEvent | null> {
    if (!sessionId) return null;
    let sources = this.openClaw.extractQaSessionSources(sessionId);
    if (!sources.length) {
      const question = this.lastUserMessage(messages);
      sources = await this.recallPgSources(question, this.buildQuestionEmbeddingText(messages));
    }
    if (!sources.length) return null;
    await this.qaSources.upsertSources(sessionId, { sources, merge: true });
    return { type: 'sources', sources };
  }
}
