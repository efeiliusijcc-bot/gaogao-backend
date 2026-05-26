import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import crypto from 'crypto';
import { createRequire } from 'module';
import OpenAI from 'openai';
import { ResearchKeysService } from './research-keys.service.js';

type PgPool = {
  query: (text: string, params?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
  end: () => Promise<void>;
};

type NewsColumns = Record<string, string>;

export interface VectorSourceItem {
  title: string;
  url: string;
  summary: string;
  websiteName: string;
  publishTime: string;
  similarity: number;
  relevanceScore: number;
  retrievalMode: 'vector';
}

export interface VectorQueryPlan {
  enabled: boolean;
  available: boolean;
  storageMode: 'pgvector_chunks' | 'legacy_vector_materials' | 'unavailable';
  embeddingModel: string;
  indexTable: string;
  activeTable: string;
  sourceTable: string;
  embeddingColumnType: string;
  pgvectorAvailable: boolean;
  indexedRows: number;
  vectorHits: number;
  keywordBoostedHits: number;
  returnedSources: number;
  broadeningApplied: boolean;
  lastIndexedAt: string | null;
  fallbackReason: string;
}

export interface VectorSearchResult {
  status: 'hit' | 'empty' | 'fallback' | 'unavailable';
  sources: VectorSourceItem[];
  totalHits: number;
  queryPlan: VectorQueryPlan;
  updatedAt: string | null;
}

interface VectorSearchInput {
  topic: string;
  knownContext: Record<string, unknown>;
  maxRows: number;
  lookbackDays: number;
}

const require = createRequire(import.meta.url);
const EMBEDDING_MODEL = process.env.PGVECTOR_EMBEDDING_MODEL || 'text-embedding-3-small';
const EMBEDDING_DIMENSIONS = Math.max(1, Number(process.env.PGVECTOR_EMBEDDING_DIMENSIONS || 1536));
const EMBEDDING_BASE_URL = process.env.PGVECTOR_EMBEDDING_BASE_URL || process.env.OPENAI_BASE_URL || '';
const SOURCE_TABLE = process.env.PGVECTOR_NEWS_TABLE || 'news';
const INDEX_TABLE = process.env.PGVECTOR_INDEX_TABLE || 'news_vector_chunks';
const INDEX_INTERVAL_MS = Math.max(60_000, Number(process.env.PGVECTOR_INDEX_INTERVAL_MS || 600_000));
const INDEX_BATCH_SIZE = Math.max(1, Math.min(500, Number(process.env.PGVECTOR_INDEX_BATCH_SIZE || 100)));

@Injectable()
export class VectorSourceService implements OnModuleInit, OnModuleDestroy {
  private pool: PgPool | null = null;
  private initPromise: Promise<boolean> | null = null;
  private indexTimer: NodeJS.Timeout | null = null;
  private supportsPgVector = false;
  private supportsSourceEmbeddingText = false;
  private storageMode: VectorQueryPlan['storageMode'] = 'unavailable';
  private embeddingColumnType = '';
  private pgvectorAvailable = false;
  private legacyFallbackReason = '';
  private lastError = '';
  private lastIndexedAt: string | null = null;
  private lastIndexStats = { indexed: 0, skipped: 0 };

  constructor(private readonly researchKeys: ResearchKeysService) {}

  onModuleInit() {
    if (!this.databaseUrl()) return;
    this.indexTimer = setInterval(() => {
      void this.indexPendingNews({ limit: INDEX_BATCH_SIZE }).catch((error) => {
        this.lastError = this.safeError(error);
      });
    }, INDEX_INTERVAL_MS);
    void this.indexPendingNews({ limit: INDEX_BATCH_SIZE }).catch((error) => {
      this.lastError = this.safeError(error);
    });
  }

  async onModuleDestroy() {
    if (this.indexTimer) clearInterval(this.indexTimer);
    if (this.pool) await this.pool.end();
  }

  async status(): Promise<VectorQueryPlan> {
    const available = await this.ensureReady();
    const stats = available ? await this.indexStats() : { indexedRows: 0, lastIndexedAt: null };
    return {
      enabled: Boolean(this.databaseUrl()),
      available,
      storageMode: available ? this.storageMode : 'unavailable',
      embeddingModel: EMBEDDING_MODEL,
      indexTable: this.storageMode === 'pgvector_chunks' ? INDEX_TABLE : '',
      activeTable: this.storageMode === 'pgvector_chunks' ? INDEX_TABLE : SOURCE_TABLE,
      sourceTable: SOURCE_TABLE,
      embeddingColumnType: this.embeddingColumnType,
      pgvectorAvailable: this.pgvectorAvailable,
      indexedRows: stats.indexedRows,
      vectorHits: 0,
      keywordBoostedHits: 0,
      returnedSources: 0,
      broadeningApplied: false,
      lastIndexedAt: stats.lastIndexedAt || this.lastIndexedAt,
      fallbackReason: available ? this.legacyFallbackReason : this.lastError || 'PGVECTOR_DATABASE_URL is not configured',
    };
  }

  async reindex(limit = INDEX_BATCH_SIZE): Promise<VectorQueryPlan> {
    await this.indexPendingNews({ limit: Math.max(1, Math.min(1000, limit)) });
    return this.status();
  }

  async search(input: VectorSearchInput): Promise<VectorSearchResult> {
    const available = await this.ensureReady();
    if (!available) return this.emptyResult('unavailable', this.lastError || 'Vector database is unavailable');

    const apiKey = await this.researchKeys.getEffectiveKey('openaiEmbeddingApiKey');
    if (!apiKey) return this.emptyResult('fallback', 'OpenAI embedding key is not configured');

    const stats = await this.indexStats();
    if (stats.indexedRows === 0) {
      await this.indexPendingNews({ limit: INDEX_BATCH_SIZE });
    }

    const refreshedStats = await this.indexStats();
    if (refreshedStats.indexedRows === 0) return this.emptyResult('empty', 'Vector index has no rows');

    try {
      const queryText = this.buildQueryText(input.topic, input.knownContext);
      const embedding = await this.embedTexts(apiKey, [queryText]);
      if (!this.supportsPgVector) {
        return this.searchSourceEmbeddingText(input, embedding[0] || [], queryText);
      }
      const vector = this.toVectorLiteral(embedding[0] || []);
      const terms = this.extractTerms(queryText);
      const pool = await this.getPool();
      const params: unknown[] = [vector, Math.max(input.maxRows * 2, 20)];
      let where = '';
      const freshness = this.safeLookbackDays(input.lookbackDays);
      if (freshness > 0) {
        params.push(freshness);
        where = `WHERE source_time IS NULL OR source_time >= now() - ($${params.length}::int * interval '1 day')`;
      }
      const rows = await pool.query(
        `SELECT source_key, source_url, ch_title, entitle, website_name, publish_time, summary,
                1 - (embedding <=> $1::vector) AS similarity,
                source_time, indexed_at
           FROM ${this.qi(INDEX_TABLE)}
           ${where}
           ORDER BY embedding <=> $1::vector
           LIMIT $2`,
        params,
      );

      const scored = rows.rows
        .map((row) => this.rowToSource(row, terms))
        .filter((item) => item.url || item.title || item.summary)
        .sort((a, b) => b.relevanceScore - a.relevanceScore);
      const sources = this.dedupeSources(scored).slice(0, input.maxRows);
      const keywordBoostedHits = scored.filter((item) => item.relevanceScore - item.similarity > 0.05).length;
      const plan = await this.status();
      plan.vectorHits = scored.length;
      plan.keywordBoostedHits = keywordBoostedHits;
      plan.returnedSources = sources.length;
      plan.broadeningApplied = keywordBoostedHits > 0;
      plan.fallbackReason = '';
      return {
        status: sources.length ? 'hit' : 'empty',
        sources,
        totalHits: scored.length,
        queryPlan: plan,
        updatedAt: new Date().toISOString(),
      };
    } catch (error) {
      this.lastError = this.safeError(error);
      return this.emptyResult('fallback', this.lastError);
    }
  }

  async indexPendingNews(options: { limit?: number } = {}): Promise<{ indexed: number; skipped: number }> {
    const available = await this.ensureReady();
    if (!available) return { indexed: 0, skipped: 0 };
    if (!this.supportsPgVector) {
      this.lastIndexStats = { indexed: 0, skipped: 0 };
      return this.lastIndexStats;
    }
    const apiKey = await this.researchKeys.getEffectiveKey('openaiEmbeddingApiKey');
    if (!apiKey) {
      this.lastError = 'OpenAI embedding key is not configured';
      return { indexed: 0, skipped: 0 };
    }

    const columns = await this.discoverNewsColumns();
    const sourceKeyExpr = this.sourceKeyExpression(columns);
    const sourceHashExpr = this.sourceHashExpression(columns);
    const freshnessExpr = this.freshnessExpression(columns);
    const selectList = this.newsSelectList(columns, sourceKeyExpr, sourceHashExpr, freshnessExpr);
    const pool = await this.getPool();
    const limit = Math.max(1, Math.min(1000, options.limit || INDEX_BATCH_SIZE));
    const rows = await pool.query(
      `SELECT ${selectList}
         FROM ${this.qi(SOURCE_TABLE)} n
        WHERE NOT EXISTS (
          SELECT 1 FROM ${this.qi(INDEX_TABLE)} v
           WHERE v.source_key = ${sourceKeyExpr}
             AND v.source_hash = ${sourceHashExpr}
             AND v.embedding_model = $1
        )
        ORDER BY ${freshnessExpr} DESC NULLS LAST
        LIMIT $2`,
      [EMBEDDING_MODEL, limit],
    );

    const candidates = rows.rows
      .map((row) => this.normalizeNewsRow(row))
      .filter((row) => row.sourceKey && row.sourceHash && row.chunkText.length >= 12);
    if (!candidates.length) {
      this.lastIndexStats = { indexed: 0, skipped: rows.rows.length };
      return this.lastIndexStats;
    }

    const embeddings = await this.embedTexts(apiKey, candidates.map((row) => row.chunkText));
    let indexed = 0;
    for (let index = 0; index < candidates.length; index += 1) {
      const item = candidates[index];
      const embedding = embeddings[index];
      if (!embedding?.length) continue;
      await pool.query(
        `INSERT INTO ${this.qi(INDEX_TABLE)}
          (source_key, source_hash, source_url, ch_title, entitle, website_name, publish_time, source_time,
           summary, chunk_text, embedding, embedding_model, indexed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::vector,$12,now())
         ON CONFLICT (source_key, embedding_model)
         DO UPDATE SET
           source_hash = EXCLUDED.source_hash,
           source_url = EXCLUDED.source_url,
           ch_title = EXCLUDED.ch_title,
           entitle = EXCLUDED.entitle,
           website_name = EXCLUDED.website_name,
           publish_time = EXCLUDED.publish_time,
           source_time = EXCLUDED.source_time,
           summary = EXCLUDED.summary,
           chunk_text = EXCLUDED.chunk_text,
           embedding = EXCLUDED.embedding,
           indexed_at = now()`,
        [
          item.sourceKey,
          item.sourceHash,
          item.url,
          item.chTitle,
          item.entitle,
          item.websiteName,
          item.publishTime || null,
          item.sourceTime || null,
          item.summary,
          item.chunkText,
          this.toVectorLiteral(embedding),
          EMBEDDING_MODEL,
        ],
      );
      indexed += 1;
    }
    this.lastIndexedAt = new Date().toISOString();
    this.lastError = '';
    this.lastIndexStats = { indexed, skipped: rows.rows.length - indexed };
    return this.lastIndexStats;
  }

  private async ensureReady(): Promise<boolean> {
    if (!this.databaseUrl()) {
      this.lastError = 'PGVECTOR_DATABASE_URL is not configured';
      return false;
    }
    if (!this.initPromise) {
      this.initPromise = this.initialize().catch((error) => {
        this.lastError = this.safeError(error);
        return false;
      });
    }
    return this.initPromise;
  }

  private async initialize(): Promise<boolean> {
    const pool = await this.getPool();
    const columns = await this.discoverNewsColumns();
    const embeddingMeta = await this.discoverColumnMeta(SOURCE_TABLE, columns.embedding);
    this.embeddingColumnType = embeddingMeta
      ? [embeddingMeta.dataType, embeddingMeta.udtName].filter(Boolean).join('/')
      : '';
    const available = await pool.query(`SELECT 1 FROM pg_available_extensions WHERE name = 'vector' LIMIT 1`);
    this.pgvectorAvailable = Boolean(available.rows.length);

    if (SOURCE_TABLE === 'vector_materials' && columns.embedding) {
      this.supportsPgVector = false;
      this.supportsSourceEmbeddingText = true;
      this.storageMode = 'legacy_vector_materials';
      this.legacyFallbackReason = this.pgvectorAvailable
        ? ''
        : 'pgvector extension is unavailable; using legacy_vector_materials text embeddings';
      await this.ensureLegacyVectorMaterialsSchema();
      if (this.pgvectorAvailable && embeddingMeta?.udtName === 'vector') {
        await this.ensureLegacyVectorIndex();
      }
      this.lastError = '';
      return true;
    }

    if (!available.rows.length) {
      this.supportsSourceEmbeddingText = Boolean(columns.embedding);
      this.supportsPgVector = false;
      this.storageMode = this.supportsSourceEmbeddingText ? 'legacy_vector_materials' : 'unavailable';
      this.legacyFallbackReason = 'pgvector extension is unavailable; using source embedding text column when present';
      if (!this.supportsSourceEmbeddingText) {
        throw new Error('PostgreSQL vector extension is unavailable and source embedding column was not found');
      }
      this.lastError = '';
      return true;
    }

    await pool.query('CREATE EXTENSION IF NOT EXISTS vector');
    this.supportsPgVector = true;
    this.storageMode = 'pgvector_chunks';
    this.legacyFallbackReason = '';
    await pool.query(
      `CREATE TABLE IF NOT EXISTS ${this.qi(INDEX_TABLE)} (
        id bigserial PRIMARY KEY,
        source_key text NOT NULL,
        source_hash text NOT NULL,
        source_url text,
        ch_title text,
        entitle text,
        website_name text,
        publish_time timestamptz,
        source_time timestamptz,
        summary text,
        chunk_text text NOT NULL,
        embedding vector(${EMBEDDING_DIMENSIONS}) NOT NULL,
        embedding_model text NOT NULL,
        indexed_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (source_key, embedding_model)
      )`,
    );
    await pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.qi(`${INDEX_TABLE}_embedding_idx`)}
         ON ${this.qi(INDEX_TABLE)}
      USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)`,
    );
    await pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.qi(`${INDEX_TABLE}_source_time_idx`)}
         ON ${this.qi(INDEX_TABLE)} (source_time DESC NULLS LAST)`,
    );
    this.lastError = '';
    return true;
  }

  private async getPool(): Promise<PgPool> {
    if (this.pool) return this.pool;
    const { Pool } = require('pg') as { Pool: new (config: Record<string, unknown>) => PgPool };
    this.pool = new Pool({
      connectionString: this.databaseUrl(),
      max: 4,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    });
    return this.pool;
  }

  private databaseUrl(): string {
    return process.env.PGVECTOR_DATABASE_URL || process.env.POSTGRES_DATABASE_URL || process.env.DATABASE_URL || '';
  }

  private async discoverNewsColumns(): Promise<NewsColumns> {
    const pool = await this.getPool();
    const result = await pool.query(
      `SELECT column_name
         FROM information_schema.columns
        WHERE table_name = $1
          AND table_schema = ANY (current_schemas(false))`,
      [SOURCE_TABLE],
    );
    const available = new Set(result.rows.map((row) => String(row.column_name)));
    const pick = (...names: string[]) => names.find((name) => available.has(name)) || '';
    return {
      id: pick('id', 'news_id'),
      url: pick('data_source_url', 'url', 'source_url'),
      chTitle: pick('ch_title', 'title', 'headline'),
      entitle: pick('entitle', 'en_title', 'english_title'),
      summary: pick('summary', 'abstract', 'description'),
      tag: pick('tag', 'tags'),
      designatedTag: pick('designated_tag', 'designated_tags'),
      websiteName: pick('website_name', 'site_name', 'source_name'),
      publishTime: pick('publish_time', 'published_at', 'pub_time'),
      sourceTime: pick('crawl_time', 'crawled_at', 'created_at', 'updated_at', 'inserted_at', 'publish_time'),
      content: pick('content', 'body', 'text'),
      embedding: pick('embedding'),
    };
  }

  private async discoverColumnMeta(tableName: string, columnName: string): Promise<{ dataType: string; udtName: string } | null> {
    if (!columnName) return null;
    const pool = await this.getPool();
    const result = await pool.query(
      `SELECT data_type, udt_name
         FROM information_schema.columns
        WHERE table_name = $1
          AND column_name = $2
          AND table_schema = ANY (current_schemas(false))
        LIMIT 1`,
      [tableName, columnName],
    );
    const row = result.rows[0];
    if (!row) return null;
    return { dataType: String(row.data_type || ''), udtName: String(row.udt_name || '') };
  }

  private async ensureLegacyVectorMaterialsSchema(): Promise<void> {
    const pool = await this.getPool();
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS mysql_database text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS mysql_table_name text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS entitle text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS data_source_url text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS website_name text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS tag text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS content_hash text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS embedding_model text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS indexed_at timestamptz`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS vector_status text`);
    await pool.query(`ALTER TABLE ${this.qi(SOURCE_TABLE)} ADD COLUMN IF NOT EXISTS error_message text`);
    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${this.qi(`${SOURCE_TABLE}_mysql_source_uidx`)}
         ON ${this.qi(SOURCE_TABLE)} (mysql_database, mysql_table_name, mysql_id, embedding_model)`,
    );
    await pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.qi(`${SOURCE_TABLE}_publish_time_idx`)}
         ON ${this.qi(SOURCE_TABLE)} (publish_time DESC NULLS LAST)`,
    );
    await pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.qi(`${SOURCE_TABLE}_indexed_at_idx`)}
         ON ${this.qi(SOURCE_TABLE)} (indexed_at DESC NULLS LAST)`,
    );
  }

  private async ensureLegacyVectorIndex(): Promise<void> {
    const pool = await this.getPool();
    try {
      await pool.query(
        `CREATE INDEX IF NOT EXISTS ${this.qi(`${SOURCE_TABLE}_embedding_hnsw_idx`)}
           ON ${this.qi(SOURCE_TABLE)}
        USING hnsw (embedding vector_cosine_ops)`,
      );
    } catch (error) {
      this.legacyFallbackReason = `legacy vector index was not created: ${this.safeError(error)}`;
    }
  }

  private sourceKeyExpression(columns: NewsColumns): string {
    const parts = [columns.id, columns.url, columns.chTitle, columns.entitle, columns.summary]
      .filter(Boolean)
      .map((column) => `NULLIF(n.${this.qi(column)}::text, '')`);
    if (!parts.length) return this.sourceHashExpression(columns);
    return `COALESCE(${parts.join(', ')}, ${this.sourceHashExpression(columns)})`;
  }

  private sourceHashExpression(columns: NewsColumns): string {
    const parts = [columns.url, columns.chTitle, columns.entitle, columns.summary, columns.publishTime, columns.content]
      .filter(Boolean)
      .map((column) => `n.${this.qi(column)}::text`);
    if (!parts.length) return `md5(random()::text)`;
    return `md5(concat_ws('|', ${parts.join(', ')}))`;
  }

  private freshnessExpression(columns: NewsColumns): string {
    const column = columns.sourceTime || columns.publishTime;
    return column ? `n.${this.qi(column)}::timestamptz` : 'NULL::timestamptz';
  }

  private newsSelectList(columns: NewsColumns, sourceKeyExpr: string, sourceHashExpr: string, freshnessExpr: string): string {
    const field = (alias: string, column: string) => column ? `n.${this.qi(column)}::text AS ${this.qi(alias)}` : `NULL::text AS ${this.qi(alias)}`;
    return [
      `${sourceKeyExpr} AS source_key`,
      `${sourceHashExpr} AS source_hash`,
      `${freshnessExpr} AS source_time`,
      field('url', columns.url),
      field('ch_title', columns.chTitle),
      field('entitle', columns.entitle),
      field('summary', columns.summary),
      field('tag', columns.tag),
      field('designated_tag', columns.designatedTag),
      field('website_name', columns.websiteName),
      columns.publishTime ? `n.${this.qi(columns.publishTime)}::timestamptz AS publish_time` : 'NULL::timestamptz AS publish_time',
      field('content', columns.content),
    ].join(', ');
  }

  private normalizeNewsRow(row: Record<string, unknown>) {
    const summary = this.clean(String(row.summary ?? ''), 1800);
    const content = this.clean(String(row.content ?? ''), summary.length < 100 ? 1200 : 0);
    const text = [
      this.clean(String(row.ch_title ?? ''), 300),
      this.clean(String(row.entitle ?? ''), 300),
      this.clean(String(row.tag ?? ''), 300),
      this.clean(String(row.designated_tag ?? ''), 300),
      summary,
      content,
    ].filter(Boolean).join('\n');
    return {
      sourceKey: this.clean(String(row.source_key ?? ''), 500),
      sourceHash: this.clean(String(row.source_hash ?? ''), 64),
      url: this.clean(String(row.url ?? ''), 1000),
      chTitle: this.clean(String(row.ch_title ?? ''), 500),
      entitle: this.clean(String(row.entitle ?? ''), 500),
      websiteName: this.clean(String(row.website_name ?? ''), 300),
      publishTime: this.dateString(row.publish_time),
      sourceTime: this.dateString(row.source_time),
      summary,
      chunkText: this.clean(text, 3000),
    };
  }

  private rowToSource(row: Record<string, unknown>, terms: string[]): VectorSourceItem {
    const title = this.clean(String(row.ch_title || row.entitle || ''), 500);
    const summary = this.clean(String(row.summary || ''), 1200);
    const websiteName = this.clean(String(row.website_name || ''), 300);
    const url = this.clean(String(row.source_url || ''), 1000);
    const similarity = Math.max(0, Math.min(1, Number(row.similarity || 0)));
    const haystack = `${title} ${summary} ${websiteName}`.toLowerCase();
    const termHits = terms.filter((term) => haystack.includes(term.toLowerCase())).length;
    return {
      title,
      url,
      summary,
      websiteName,
      publishTime: this.dateString(row.publish_time),
      similarity,
      relevanceScore: similarity + Math.min(0.25, termHits * 0.03),
      retrievalMode: 'vector',
    };
  }

  private dedupeSources(items: VectorSourceItem[]): VectorSourceItem[] {
    const seen = new Set<string>();
    const result: VectorSourceItem[] = [];
    for (const item of items) {
      const key = item.url || crypto.createHash('sha1').update(`${item.title}|${item.summary}`).digest('hex');
      if (seen.has(key)) continue;
      seen.add(key);
      result.push(item);
    }
    return result;
  }

  private async searchSourceEmbeddingText(input: VectorSearchInput, queryEmbedding: number[], queryText: string): Promise<VectorSearchResult> {
    const columns = await this.discoverNewsColumns();
    if (!columns.embedding) return this.emptyResult('fallback', 'Source embedding column was not found');

    const pool = await this.getPool();
    const sourceKeyExpr = this.sourceKeyExpression(columns);
    const freshnessExpr = this.freshnessExpression(columns);
    const field = (alias: string, column: string) => column ? `n.${this.qi(column)}::text AS ${this.qi(alias)}` : `NULL::text AS ${this.qi(alias)}`;
    const rows = await pool.query(
      `SELECT ${sourceKeyExpr} AS source_key,
              ${field('url', columns.url)},
              ${field('ch_title', columns.chTitle)},
              ${field('entitle', columns.entitle)},
              ${field('summary', columns.summary)},
              ${field('website_name', columns.websiteName)},
              ${columns.publishTime ? `n.${this.qi(columns.publishTime)}::timestamptz` : 'NULL::timestamptz'} AS publish_time,
              ${freshnessExpr} AS source_time,
              n.${this.qi(columns.embedding)}::text AS embedding
         FROM ${this.qi(SOURCE_TABLE)} n
        WHERE n.${this.qi(columns.embedding)} IS NOT NULL
        ORDER BY ${freshnessExpr} DESC NULLS LAST
        LIMIT $1`,
      [Math.max(input.maxRows * 20, 300)],
    );

    const terms = this.extractTerms(queryText);
    const scored = rows.rows
      .map((row) => {
        const source = this.rowToSource({ ...row, source_url: row.url, similarity: this.cosine(queryEmbedding, this.parseVectorText(String(row.embedding || ''))) }, terms);
        return source;
      })
      .filter((item) => item.similarity > 0)
      .sort((a, b) => b.relevanceScore - a.relevanceScore);
    const sources = this.dedupeSources(scored).slice(0, input.maxRows);
    const keywordBoostedHits = scored.filter((item) => item.relevanceScore - item.similarity > 0.05).length;
    const plan = await this.status();
    plan.available = true;
    plan.vectorHits = scored.length;
    plan.keywordBoostedHits = keywordBoostedHits;
    plan.returnedSources = sources.length;
    plan.broadeningApplied = keywordBoostedHits > 0;
    plan.fallbackReason = '';
    return {
      status: sources.length ? 'hit' : 'empty',
      sources,
      totalHits: scored.length,
      queryPlan: plan,
      updatedAt: new Date().toISOString(),
    };
  }

  private parseVectorText(value: string): number[] {
    const normalized = value.trim().replace(/^\[/, '').replace(/\]$/, '');
    if (!normalized) return [];
    return normalized
      .split(',')
      .map((part) => Number(part.trim()))
      .filter((part) => Number.isFinite(part));
  }

  private cosine(a: number[], b: number[]): number {
    const length = Math.min(a.length, b.length);
    if (!length) return 0;
    let dot = 0;
    let normA = 0;
    let normB = 0;
    for (let index = 0; index < length; index += 1) {
      dot += a[index] * b[index];
      normA += a[index] * a[index];
      normB += b[index] * b[index];
    }
    if (!normA || !normB) return 0;
    return dot / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  private buildQueryText(topic: string, context: Record<string, unknown>): string {
    const selectedSearchQueries = Array.isArray(context.selectedSearchQueries) ? context.selectedSearchQueries : [];
    const supplement = String(context.supplement || context.freeTextContext || '');
    const modules = Array.isArray(context.selectedModules) ? context.selectedModules : [];
    const directions = modules.flatMap((item) => {
      if (!item || typeof item !== 'object') return [];
      const selectedDirections = (item as Record<string, unknown>).selectedDirections;
      return Array.isArray(selectedDirections) ? selectedDirections : [];
    });
    return [topic, ...selectedSearchQueries, ...directions, supplement]
      .map((item) => this.clean(String(item || ''), 500))
      .filter(Boolean)
      .join('\n');
  }

  private extractTerms(text: string): string[] {
    const terms = new Set<string>();
    for (const match of text.match(/[A-Za-z][A-Za-z0-9&.+/-]{1,}/g) || []) terms.add(match);
    for (const match of text.match(/[\p{Script=Han}]{2,}/gu) || []) {
      const token = match.trim();
      if (token.length <= 8) terms.add(token);
      else {
        for (let index = 0; index <= token.length - 4; index += 2) terms.add(token.slice(index, index + 4));
      }
    }
    return Array.from(terms).slice(0, 80);
  }

  private async embedTexts(apiKey: string, texts: string[]): Promise<number[][]> {
    const client = new OpenAI({ apiKey, ...(EMBEDDING_BASE_URL ? { baseURL: EMBEDDING_BASE_URL } : {}) });
    const response = await client.embeddings.create({
      model: EMBEDDING_MODEL,
      input: texts.map((text) => text.slice(0, 8000)),
      ...(EMBEDDING_DIMENSIONS ? { dimensions: EMBEDDING_DIMENSIONS } : {}),
    });
    return response.data.map((item) => item.embedding);
  }

  private toVectorLiteral(vector: number[]): string {
    return `[${vector.map((value) => Number(value).toFixed(8)).join(',')}]`;
  }

  private async indexStats(): Promise<{ indexedRows: number; lastIndexedAt: string | null }> {
    try {
      const pool = await this.getPool();
      if (!this.supportsPgVector) {
        const columns = await this.discoverNewsColumns();
        if (!columns.embedding) return { indexedRows: 0, lastIndexedAt: this.lastIndexedAt };
        const freshness = this.freshnessExpression(columns).replace(/\bn\./g, '');
        const result = await pool.query(
          `SELECT count(*)::int AS count, max(${freshness}) AS last_indexed_at
             FROM ${this.qi(SOURCE_TABLE)}
            WHERE ${this.qi(columns.embedding)} IS NOT NULL`,
        );
        return {
          indexedRows: Number(result.rows[0]?.count || 0),
          lastIndexedAt: this.dateString(result.rows[0]?.last_indexed_at) || this.lastIndexedAt,
        };
      }
      const result = await pool.query(
        `SELECT count(*)::int AS count, max(indexed_at) AS last_indexed_at
           FROM ${this.qi(INDEX_TABLE)}
          WHERE embedding_model = $1`,
        [EMBEDDING_MODEL],
      );
      return {
        indexedRows: Number(result.rows[0]?.count || 0),
        lastIndexedAt: this.dateString(result.rows[0]?.last_indexed_at) || this.lastIndexedAt,
      };
    } catch {
      return { indexedRows: 0, lastIndexedAt: this.lastIndexedAt };
    }
  }

  private emptyResult(status: VectorSearchResult['status'], reason: string): VectorSearchResult {
    return {
      status,
      sources: [],
      totalHits: 0,
      updatedAt: null,
      queryPlan: {
        enabled: Boolean(this.databaseUrl()),
        available: status !== 'unavailable',
        storageMode: status !== 'unavailable' ? this.storageMode : 'unavailable',
        embeddingModel: EMBEDDING_MODEL,
        indexTable: this.storageMode === 'pgvector_chunks' ? INDEX_TABLE : '',
        activeTable: this.storageMode === 'pgvector_chunks' ? INDEX_TABLE : SOURCE_TABLE,
        sourceTable: SOURCE_TABLE,
        embeddingColumnType: this.embeddingColumnType,
        pgvectorAvailable: this.pgvectorAvailable,
        indexedRows: 0,
        vectorHits: 0,
        keywordBoostedHits: 0,
        returnedSources: 0,
        broadeningApplied: false,
        lastIndexedAt: this.lastIndexedAt,
        fallbackReason: reason,
      },
    };
  }

  private safeLookbackDays(value: number): number {
    if (!Number.isFinite(value)) return 30;
    return Math.max(0, Math.min(365, Math.floor(value)));
  }

  private qi(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  private dateString(value: unknown): string {
    if (!value) return '';
    const date = value instanceof Date ? value : new Date(String(value));
    return Number.isFinite(date.getTime()) ? date.toISOString() : '';
  }

  private clean(value: string, limit: number): string {
    if (limit <= 0) return '';
    return value.replace(/\s+/g, ' ').trim().slice(0, limit);
  }

  private safeError(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error);
    return message
      .replace(/postgres(?:ql)?:\/\/[^@\s]+@/gi, 'postgres://***@')
      .replace(/api[_-]?key[=:]\s*[^,\s]+/gi, 'api_key=***')
      .slice(0, 300);
  }
}
