import crypto from 'crypto';
import fs from 'fs/promises';
import { execFile as execFileCallback } from 'child_process';
import { promisify } from 'util';
import { createRequire } from 'module';
import OpenAI from 'openai';

type PgPool = {
  query: (text: string, params?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
  end: () => Promise<void>;
};

interface Args {
  days: number;
  maxRows: number;
  batchSize: number;
  mysqlContainer: string;
  mysqlDatabase: string;
  mysqlUser: string;
  mysqlPassword: string;
  pgTable: string;
  embeddingModel: string;
  embeddingBaseUrl: string;
  embeddingDimensions: number;
  dryRun: boolean;
}

interface MysqlRow {
  mysql_id: number;
  entitle: string;
  ch_title: string;
  publish_time: string;
  content: string;
  data_source_url: string;
  website_name: string;
  summary: string;
  designated_tag: string;
  tag: string;
  data_type: string;
  mysql_table_name: string;
}

const require = createRequire(import.meta.url);
const execFile = promisify(execFileCallback);
const DEFAULT_EMBEDDING_DIMENSIONS = 1536;

async function main() {
  const args = await loadArgs();
  const pgPool = await getPgPool();
  try {
    const openaiKey = await effectiveOpenAiKey();
    if (!openaiKey && !args.dryRun) {
      throw new Error('OpenAI embedding key is not configured. Set OPENAI_API_KEY or save openaiEmbeddingApiKey first.');
    }

    const schema = await ensureVectorMaterialsSchema(pgPool, args.pgTable);
    const tables = await discoverMysqlDailyTables(args);
    if (!tables.length) {
      console.log(JSON.stringify({ status: 'empty', reason: 'No recent MySQL daily tables were found', days: args.days }, null, 2));
      return;
    }

    let fetched = 0;
    let indexed = 0;
    let skipped = 0;
    const openai = openaiKey ? new OpenAI({ apiKey: openaiKey, ...(args.embeddingBaseUrl ? { baseURL: args.embeddingBaseUrl } : {}) }) : null;
    for (const table of tables) {
      if (fetched >= args.maxRows) break;
      const remaining = args.maxRows - fetched;
      const rows = await fetchMysqlRows(args, table, remaining);
      fetched += rows.length;
      const candidates = rows
        .map((row) => ({ row, text: buildEmbeddingText(row) }))
        .filter((item) => item.row.mysql_id && item.text.length >= 12);
      skipped += rows.length - candidates.length;
      if (args.dryRun || !openai) continue;

      for (let offset = 0; offset < candidates.length; offset += args.batchSize) {
        const batch = candidates.slice(offset, offset + args.batchSize);
        const embeddings = await embedTexts(openai, args.embeddingModel, batch.map((item) => item.text), args.embeddingDimensions);
        for (let index = 0; index < batch.length; index += 1) {
          const item = batch[index];
          const embedding = embeddings[index];
          if (!embedding?.length) {
            skipped += 1;
            continue;
          }
          await upsertVectorMaterial(pgPool, args, schema.embeddingStorage, item.row, item.text, embedding);
          indexed += 1;
        }
      }
    }

    const stats = await pgPool.query(
      `SELECT count(*)::int AS rows, count(embedding)::int AS embedding_rows FROM ${qi(args.pgTable)}`,
    );
    console.log(JSON.stringify({
      status: args.dryRun ? 'dry_run' : 'ok',
      mode: 'legacy_vector_materials',
      pgvectorAvailable: schema.pgvectorAvailable,
      embeddingStorage: schema.embeddingStorage,
      days: args.days,
      fetched,
      indexed,
      skipped,
      totalRows: Number(stats.rows[0]?.rows || 0),
      totalEmbeddingRows: Number(stats.rows[0]?.embedding_rows || 0),
      fallbackReason: schema.fallbackReason,
    }, null, 2));
  } finally {
    await pgPool.end();
  }
}

async function loadArgs(): Promise<Args> {
  const parsed = parseCliArgs(process.argv.slice(2));
  const mysqlContainer = String(parsed.mysqlContainer || process.env.MYSQL_DOCKER_CONTAINER || 'my_mysql');
  const inspected = await inspectMysqlEnv(mysqlContainer).catch(() => ({} as Record<string, string>));
  return {
    days: positiveInt(parsed.days, Number(process.env.VECTOR_BACKFILL_DAYS || 1), 30),
    maxRows: positiveInt(parsed.maxRows || parsed.limit, Number(process.env.VECTOR_BACKFILL_MAX_ROWS || 10_000), 100_000),
    batchSize: positiveInt(parsed.batchSize || parsed.batch, Number(process.env.VECTOR_BACKFILL_BATCH_SIZE || 50), 200),
    mysqlContainer,
    mysqlDatabase: String(parsed.mysqlDatabase || process.env.MYSQL_DATABASE || 'news'),
    mysqlUser: String(parsed.mysqlUser || process.env.MYSQL_USER || 'root'),
    mysqlPassword: String(parsed.mysqlPassword || process.env.MYSQL_PASSWORD || inspected.MYSQL_ROOT_PASSWORD || inspected.MYSQL_PASSWORD || ''),
    pgTable: String(parsed.pgTable || process.env.PGVECTOR_NEWS_TABLE || 'vector_materials'),
    embeddingModel: String(parsed.embeddingModel || process.env.PGVECTOR_EMBEDDING_MODEL || 'text-embedding-3-small'),
    embeddingBaseUrl: String(parsed.embeddingBaseUrl || process.env.PGVECTOR_EMBEDDING_BASE_URL || process.env.OPENAI_BASE_URL || ''),
    embeddingDimensions: positiveInt(parsed.embeddingDimensions || process.env.PGVECTOR_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_DIMENSIONS, 4096),
    dryRun: Boolean(parsed.dryRun || process.env.VECTOR_BACKFILL_DRY_RUN === '1'),
  };
}

function parseCliArgs(argv: string[]): Record<string, string | boolean> {
  const result: Record<string, string | boolean> = {};
  for (const arg of argv) {
    if (!arg.startsWith('--')) continue;
    const body = arg.slice(2);
    const eq = body.indexOf('=');
    if (eq === -1) result[toCamel(body)] = true;
    else result[toCamel(body.slice(0, eq))] = body.slice(eq + 1);
  }
  return result;
}

function toCamel(value: string): string {
  return value.replace(/-([a-z])/g, (_, letter: string) => letter.toUpperCase());
}

function positiveInt(value: unknown, fallback: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.max(1, Math.min(max, Math.floor(parsed)));
}

async function getPgPool(): Promise<PgPool> {
  const url = process.env.PGVECTOR_DATABASE_URL || process.env.POSTGRES_DATABASE_URL || process.env.DATABASE_URL;
  if (!url) throw new Error('PGVECTOR_DATABASE_URL is not configured');
  const { Pool } = require('pg') as { Pool: new (config: Record<string, unknown>) => PgPool };
  return new Pool({ connectionString: url, max: 4, idleTimeoutMillis: 30_000, connectionTimeoutMillis: 10_000 });
}

async function effectiveOpenAiKey(): Promise<string> {
  if (process.env.OPENAI_API_KEY) return process.env.OPENAI_API_KEY;
  const configPaths = [
    process.env.RESEARCH_KEYS_JSON,
    '/home/node/.openclaw/workspace/report-agent/config/research-keys.json',
    '/usr/docker/openclaw/workspace/report-agent/config/research-keys.json',
  ].filter(Boolean) as string[];
  for (const configPath of configPaths) {
    try {
      const parsed = JSON.parse(await fs.readFile(configPath, 'utf-8')) as { openaiEmbeddingApiKey?: string };
      if (parsed.openaiEmbeddingApiKey) return parsed.openaiEmbeddingApiKey;
    } catch {
      // Try the next known deployment path.
    }
  }
  return '';
}

async function inspectMysqlEnv(container: string): Promise<Record<string, string>> {
  const { stdout } = await execFile('docker', ['inspect', container, '--format', '{{range .Config.Env}}{{println .}}{{end}}'], { maxBuffer: 1024 * 1024 });
  const env: Record<string, string> = {};
  for (const line of stdout.split(/\r?\n/)) {
    const index = line.indexOf('=');
    if (index > 0) env[line.slice(0, index)] = line.slice(index + 1);
  }
  return env;
}

async function ensureVectorMaterialsSchema(pool: PgPool, table: string): Promise<{ pgvectorAvailable: boolean; embeddingStorage: 'text' | 'vector'; fallbackReason: string }> {
  const available = await pool.query(`SELECT 1 FROM pg_available_extensions WHERE name = 'vector' LIMIT 1`);
  let pgvectorAvailable = Boolean(available.rows.length);
  if (pgvectorAvailable) {
    try {
      await pool.query('CREATE EXTENSION IF NOT EXISTS vector');
    } catch {
      pgvectorAvailable = false;
    }
  }

  await pool.query(
    `CREATE TABLE IF NOT EXISTS ${qi(table)} (
      id serial PRIMARY KEY,
      mysql_id integer,
      ch_title varchar,
      summary text,
      publish_time timestamp,
      designated_tag varchar,
      data_type varchar,
      embedding text
    )`,
  );

  const addColumns = [
    ['mysql_database', 'text'],
    ['mysql_table_name', 'text'],
    ['entitle', 'text'],
    ['data_source_url', 'text'],
    ['website_name', 'text'],
    ['tag', 'text'],
    ['content_hash', 'text'],
    ['embedding_model', 'text'],
    ['embedding_dimensions', 'integer'],
    ['indexed_at', 'timestamptz'],
    ['vector_status', 'text'],
    ['error_message', 'text'],
  ];
  for (const [name, type] of addColumns) {
    await pool.query(`ALTER TABLE ${qi(table)} ADD COLUMN IF NOT EXISTS ${qi(name)} ${type}`);
  }
  if (pgvectorAvailable) {
    await pool.query(`ALTER TABLE ${qi(table)} ADD COLUMN IF NOT EXISTS embedding_vector vector(1536)`);
  }
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${qi(`${table}_mysql_source_uidx`)} ON ${qi(table)} (mysql_database, mysql_table_name, mysql_id, embedding_model)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS ${qi(`${table}_publish_time_idx`)} ON ${qi(table)} (publish_time DESC NULLS LAST)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS ${qi(`${table}_indexed_at_idx`)} ON ${qi(table)} (indexed_at DESC NULLS LAST)`);

  const meta = await pool.query(
    `SELECT data_type, udt_name
       FROM information_schema.columns
      WHERE table_schema = ANY (current_schemas(false))
        AND table_name = $1
        AND column_name = 'embedding'
      LIMIT 1`,
    [table],
  );
  const embeddingStorage = String(meta.rows[0]?.udt_name || '') === 'vector' ? 'vector' : 'text';
  let fallbackReason = '';
  if (pgvectorAvailable) {
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS ${qi(`${table}_embedding_hnsw_idx`)} ON ${qi(table)} USING hnsw (embedding_vector vector_cosine_ops)`);
    } catch (error) {
      fallbackReason = safeError(error);
    }
  } else if (!pgvectorAvailable) {
    fallbackReason = 'pgvector extension is unavailable; embeddings are stored as text in legacy_vector_materials mode';
  }
  return { pgvectorAvailable, embeddingStorage, fallbackReason };
}

async function discoverMysqlDailyTables(args: Args): Promise<string[]> {
  const output = await runMysql(args, `SHOW TABLES LIKE 'data\\_%'`);
  const tableDates = output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /^data_\d{8}$/.test(line))
    .map((table) => ({ table, day: table.slice('data_'.length) }))
    .sort((a, b) => b.day.localeCompare(a.day));
  if (!tableDates.length) return [];
  const newest = parseDay(tableDates[0].day);
  const earliest = new Date(newest);
  earliest.setUTCDate(earliest.getUTCDate() - (args.days - 1));
  return tableDates
    .filter((item) => parseDay(item.day).getTime() >= earliest.getTime())
    .map((item) => item.table);
}

async function fetchMysqlRows(args: Args, table: string, limit: number): Promise<MysqlRow[]> {
  const columns = new Set((await runMysql(args, `SHOW COLUMNS FROM ${mysqlIdentifier(table)}`))
    .split(/\r?\n/)
    .map((line) => line.split('\t')[0])
    .filter(Boolean));
  const value = (name: string, expression = mysqlIdentifier(name)) => columns.has(name) ? expression : 'CAST(NULL AS CHAR)';
  const idExpr = columns.has('id') ? mysqlIdentifier('id') : 'NULL';
  const freshness = columns.has('publish_time') ? mysqlIdentifier('publish_time') : columns.has('creat_time') ? mysqlIdentifier('creat_time') : idExpr;
  const searchable = ['ch_title', 'entitle', 'summary', 'content'].filter((name) => columns.has(name)).map(mysqlIdentifier);
  const where = searchable.length ? `WHERE COALESCE(${searchable.join(', ')}) IS NOT NULL` : '';
  const sql = `
    SELECT JSON_OBJECT(
      'mysql_id', ${idExpr},
      'entitle', ${value('entitle')},
      'ch_title', ${value('ch_title')},
      'publish_time', ${value('publish_time')},
      'content', ${value('content', `LEFT(${mysqlIdentifier('content')}, 4000)`)},
      'data_source_url', ${value('data_source_url')},
      'website_name', ${value('website_name')},
      'summary', ${value('summary', `LEFT(${mysqlIdentifier('summary')}, 2000)`)},
      'designated_tag', ${value('designated_tag')},
      'tag', ${value('tag')},
      'data_type', ${value('data_type')}
    )
      FROM ${mysqlIdentifier(table)}
      ${where}
      ORDER BY ${freshness} DESC
      LIMIT ${Math.max(1, Math.floor(limit))}
  `;
  const output = await runMysql(args, sql);
  const rows: MysqlRow[] = [];
  for (const line of output.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const parsed = JSON.parse(trimmed) as Omit<MysqlRow, 'mysql_table_name'>;
      rows.push({
        mysql_id: Number(parsed.mysql_id || 0),
        entitle: clean(parsed.entitle),
        ch_title: clean(parsed.ch_title),
        publish_time: clean(parsed.publish_time),
        content: clean(parsed.content),
        data_source_url: clean(parsed.data_source_url),
        website_name: clean(parsed.website_name),
        summary: clean(parsed.summary),
        designated_tag: clean(parsed.designated_tag),
        tag: clean(parsed.tag),
        data_type: clean(parsed.data_type),
        mysql_table_name: table,
      });
    } catch {
      // Ignore malformed rows from mysql CLI output.
    }
  }
  return rows;
}

async function runMysql(args: Args, sql: string): Promise<string> {
  const dockerArgs = ['exec'];
  if (args.mysqlPassword) dockerArgs.push('-e', `MYSQL_PWD=${args.mysqlPassword}`);
  dockerArgs.push(
    args.mysqlContainer,
    'mysql',
    '-u',
    args.mysqlUser,
    '-D',
    args.mysqlDatabase,
    '-N',
    '-B',
    '--raw',
    '--default-character-set=utf8mb4',
    '-e',
    sql,
  );
  const { stdout } = await execFile('docker', dockerArgs, { maxBuffer: 256 * 1024 * 1024 });
  return stdout;
}

async function embedTexts(openai: OpenAI, model: string, texts: string[], dimensions: number): Promise<number[][]> {
  const response = await openai.embeddings.create({
    model,
    input: texts.map((text) => text.slice(0, 8000)),
    ...(dimensions ? { dimensions } : {}),
  });
  return response.data.map((item) => item.embedding);
}

async function upsertVectorMaterial(
  pool: PgPool,
  args: Args,
  embeddingStorage: 'text' | 'vector',
  row: MysqlRow,
  text: string,
  embedding: number[],
): Promise<void> {
  const embeddingValue = toVectorLiteral(embedding);
  const vectorPlaceholder = embeddingStorage === 'vector' ? '$15::vector' : '$15';
  const vectorColumnValue = '$16::vector';
  await pool.query(
    `INSERT INTO ${qi(args.pgTable)}
      (mysql_database, mysql_table_name, mysql_id, ch_title, entitle, summary, publish_time,
       designated_tag, data_type, data_source_url, website_name, tag, content_hash, embedding_model,
       embedding, embedding_vector, embedding_dimensions, indexed_at, vector_status, error_message)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,${vectorPlaceholder},${vectorColumnValue},$17,now(),'ready',NULL)
     ON CONFLICT (mysql_database, mysql_table_name, mysql_id, embedding_model)
     DO UPDATE SET
       ch_title = EXCLUDED.ch_title,
       entitle = EXCLUDED.entitle,
       summary = EXCLUDED.summary,
       publish_time = EXCLUDED.publish_time,
       designated_tag = EXCLUDED.designated_tag,
       data_type = EXCLUDED.data_type,
       data_source_url = EXCLUDED.data_source_url,
       website_name = EXCLUDED.website_name,
       tag = EXCLUDED.tag,
       content_hash = EXCLUDED.content_hash,
       embedding = EXCLUDED.embedding,
       embedding_vector = EXCLUDED.embedding_vector,
       embedding_dimensions = EXCLUDED.embedding_dimensions,
       indexed_at = now(),
       vector_status = 'ready',
       error_message = NULL`,
    [
      args.mysqlDatabase,
      row.mysql_table_name,
      row.mysql_id,
      row.ch_title || null,
      row.entitle || null,
      row.summary || null,
      parseDate(row.publish_time),
      row.designated_tag || null,
      row.data_type || null,
      row.data_source_url || null,
      row.website_name || null,
      row.tag || null,
      crypto.createHash('sha256').update(text).digest('hex'),
      args.embeddingModel,
      embeddingValue,
      embeddingValue,
      embedding.length,
    ],
  );
}

function buildEmbeddingText(row: MysqlRow): string {
  return [
    row.ch_title,
    row.entitle,
    row.website_name,
    row.tag,
    row.designated_tag,
    row.summary,
    row.content,
  ]
    .filter(Boolean)
    .join('\n')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 6000);
}

function parseDay(value: string): Date {
  return new Date(Date.UTC(Number(value.slice(0, 4)), Number(value.slice(4, 6)) - 1, Number(value.slice(6, 8))));
}

function parseDate(value: string): string | null {
  if (!value) return null;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function toVectorLiteral(vector: number[]): string {
  return `[${vector.map((value) => Number(value).toFixed(8)).join(',')}]`;
}

function mysqlIdentifier(value: string): string {
  if (!/^[A-Za-z0-9_]+$/.test(value)) throw new Error('Unsafe MySQL identifier');
  return `\`${value}\``;
}

function qi(value: string): string {
  if (!/^[A-Za-z0-9_]+$/.test(value)) throw new Error('Unsafe PostgreSQL identifier');
  return `"${value.replace(/"/g, '""')}"`;
}

function clean(value: unknown): string {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

function safeError(error: unknown): string {
  return (error instanceof Error ? error.message : String(error))
    .replace(/postgres(?:ql)?:\/\/[^@\s]+@/gi, 'postgres://***@')
    .replace(/api[_-]?key[=:]\s*[^,\s]+/gi, 'api_key=***')
    .slice(0, 300);
}

main().catch((error) => {
  console.error(JSON.stringify({ status: 'failed', error: safeError(error) }, null, 2));
  process.exitCode = 1;
});
