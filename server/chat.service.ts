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

@Injectable()
export class ChatService {
  private readonly streams = new Map<string, Subject<ServerEvent>>();
  private readonly history = new Map<string, ServerEvent[]>();
  private directClient: OpenAI | null = null;
  private directClientKey = '';
  private pgPool: PgPool | null = null;
  private readonly embeddingCache = new Map<string, number[]>();

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
    onEvent({ type: 'stage', stage: 'retrieval_started', message: '正在执行 PG 向量召回' });
    const sources = await this.recallPgSources(question);
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
    const conversation = messages.filter((item) => item.role !== 'system').slice(-8);
    const sourceBlock = sources.length
      ? sources.map((source, index) => {
          return [
            `[${index + 1}] ${source.title || '未命名信源'}`,
            `来源：${source.websiteName || '未知'} ${source.publishTime || ''}`,
            `摘要：${source.summary || source.contentExcerpt || '暂无摘要'}`,
            source.url ? `链接：${source.url}` : '',
          ].filter(Boolean).join('\n');
        }).join('\n\n')
      : '本次 PG 向量召回未检索到足够匹配材料。';

    return [
      {
        role: 'system',
        content: [
          originalSystem,
          '你是热点事件动态感知助手。必须优先依据下方 PG 向量召回材料回答；如果材料不足，要明确说明“PG 信源库未检索到足够信息”，不要编造事实。',
          '回答要求：先给结论，再列关键依据；用中文，简洁直接；不要提及 SQL、表名、MCP、向量、模型、接口、系统实现或检索过程。',
          '',
          'PG 向量召回材料：',
          sourceBlock,
        ].filter(Boolean).join('\n'),
      },
      ...conversation,
    ];
  }

  private async recallPgSources(question: string): Promise<Record<string, unknown>[]> {
    const vectorSources = await this.withTimeout(
      this.searchPgVectorSources(question),
      VECTOR_RECALL_TIMEOUT_MS,
      'PG vector recall timed out',
    );
    if (vectorSources.length >= 6) return vectorSources;
    const keywordSources = await this.searchPgKeywordSources(question);
    return this.dedupeSources([...vectorSources, ...keywordSources]).slice(0, 8);
  }

  private async searchPgVectorSources(question: string): Promise<Record<string, unknown>[]> {
    const embedding = await this.embedQuestion(question);
    if (!embedding.length) return [];
    const vector = this.toVectorLiteral(embedding);
    const pool = await this.getPgPool();
    const rows = await pool.query(
      `SELECT ch_title, entitle, data_source_url, website_name, publish_time, summary, content_excerpt, content,
              1 - (embedding_vector <=> $1::vector) AS similarity
         FROM ${this.qi(PG_SOURCE_TABLE)}
        WHERE embedding_vector IS NOT NULL
        ORDER BY embedding_vector <=> $1::vector
        LIMIT 24`,
      [vector],
    );
    return rows.rows
      .map((row) => this.normalizePgRow(row, [], 'pg_vector'))
      .filter((item) => item.title || item.summary || item.url)
      .sort((a, b) => Number(b.relevance || 0) - Number(a.relevance || 0))
      .slice(0, 8);
  }

  private async searchPgKeywordSources(question: string): Promise<Record<string, unknown>[]> {
    const terms = this.extractPgSearchTerms(question);
    if (!terms.length) return [];
    const pool = await this.getPgPool();
    const clauses: string[] = [];
    const params: unknown[] = [];
    for (const term of terms.slice(0, 10)) {
      params.push(`%${term}%`);
      const placeholder = `$${params.length}`;
      clauses.push(`(ch_title ILIKE ${placeholder} OR entitle ILIKE ${placeholder} OR summary ILIKE ${placeholder} OR content ILIKE ${placeholder} OR embedding_text ILIKE ${placeholder})`);
    }
    const rows = await this.withTimeout(
      pool.query(
        `SELECT ch_title, entitle, data_source_url, website_name, publish_time, summary, content_excerpt, content
           FROM ${this.qi(PG_SOURCE_TABLE)}
          WHERE (${clauses.join(' OR ')})
          ORDER BY publish_time DESC NULLS LAST, indexed_at DESC NULLS LAST
          LIMIT 40`,
        params,
      ),
      4500,
      'PG source query timed out',
    );
    const scored = rows.rows
      .map((row) => this.normalizePgRow(row, terms, 'pg_keyword_supplement'))
      .filter((item) => item.title || item.summary || item.url)
      .sort((a, b) => Number(b.relevance || 0) - Number(a.relevance || 0));
    return this.dedupeSources(scored).slice(0, 8);
  }

  private async embedQuestion(question: string): Promise<number[]> {
    const client = await this.getDirectClient();
    const text = String(question || '').replace(/\s+/g, ' ').trim().slice(0, 600);
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
    const similarity = Number(row.similarity || 0);
    return {
      id: `${method}-${Buffer.from(url || title || summary).toString('base64url').slice(0, 18)}`,
      title,
      url,
      summary,
      contentExcerpt,
      websiteName,
      publishTime: this.dateString(row.publish_time),
      relevance: method === 'pg_vector' ? Number(similarity.toFixed(4)) : hits,
      similarity: method === 'pg_vector' ? Number(similarity.toFixed(4)) : undefined,
      sourceType: 'PG信源库',
      sourceOrigin: 'database_recall',
      method,
      status: 'hit',
    };
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

  private async getPgPool(): Promise<PgPool> {
    if (this.pgPool) return this.pgPool;
    const connectionString = process.env.PGVECTOR_DATABASE_URL || process.env.POSTGRES_DATABASE_URL || process.env.DATABASE_URL;
    if (!connectionString) throw new Error('PGVECTOR_DATABASE_URL is not configured');
    const { Pool } = require('pg') as { Pool: new (config: Record<string, unknown>) => PgPool };
    this.pgPool = new Pool({
      connectionString,
      max: 2,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 2500,
      query_timeout: 4500,
      statement_timeout: 4500,
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
      sources = await this.recallPgSources(this.lastUserMessage(messages));
    }
    if (!sources.length) return null;
    await this.qaSources.upsertSources(sessionId, { sources, merge: true });
    return { type: 'sources', sources };
  }
}
