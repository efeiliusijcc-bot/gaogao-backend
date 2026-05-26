import crypto from 'crypto';
import fs from 'fs/promises';
import https from 'https';
import { execFile as execFileCallback } from 'child_process';
import { promisify } from 'util';
import { createRequire } from 'module';
import { gunzipSync } from 'zlib';

type PgPool = {
  query: (text: string, params?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
  end: () => Promise<void>;
};

interface Args {
  days: number;
  maxRows: number;
  mysqlContainer: string;
  mysqlDatabase: string;
  mysqlUser: string;
  mysqlPassword: string;
  pgTable: string;
  embeddingModel: string;
  embeddingDimensions: number;
  dryRun: boolean;
  pollIntervalSec: number;
  pollTimeoutSec: number;
  ossAccessKeyId: string;
  ossAccessKeySecret: string;
  ossBucket: string;
  ossEndpoint: string;
  ossPrefix: string;
  dashscopeApiKey: string;
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

interface Candidate {
  row: MysqlRow;
  text: string;
}

interface SchemaState {
  pgvectorAvailable: boolean;
  sourceConflictKey: 'source' | 'source_model';
  fallbackReason: string;
}

interface DashScopeTask {
  taskId: string;
}

interface AsyncEmbeddingResult {
  index: number;
  embedding: number[];
  error?: string;
}

const require = createRequire(import.meta.url);
const execFile = promisify(execFileCallback);
const DEFAULT_EMBEDDING_DIMENSIONS = 1536;

async function main() {
  const args = await loadArgs();
  const pool = await getPgPool();
  try {
    if (!args.dryRun) {
      for (const [name, value] of Object.entries({
        ALI_OSS_ACCESS_KEY_ID: args.ossAccessKeyId,
        ALI_OSS_ACCESS_KEY_SECRET: args.ossAccessKeySecret,
        ALI_OSS_BUCKET: args.ossBucket,
        DASHSCOPE_API_KEY: args.dashscopeApiKey,
      })) {
        if (!value) throw new Error(`${name} is not configured`);
      }
    }

    const schema = await ensureVectorMaterialsSchema(pool, args);
    const tables = await discoverMysqlDailyTables(args);
    if (!tables.length) {
      console.log(JSON.stringify({ status: 'empty', reason: 'No recent MySQL daily tables were found', days: args.days }, null, 2));
      return;
    }

    const candidates: Candidate[] = [];
    let fetched = 0;
    let skipped = 0;
    for (const table of tables) {
      if (fetched >= args.maxRows) break;
      const remaining = args.maxRows - fetched;
      const existingIds = await existingMysqlIds(pool, args, table);
      const rows = (await fetchMysqlRows(args, table, remaining + existingIds.size))
        .filter((row) => !existingIds.has(row.mysql_id))
        .slice(0, remaining);
      fetched += rows.length;
      for (const row of rows) {
        const text = buildEmbeddingText(row);
        if (!row.mysql_id || text.length < 12) {
          skipped += 1;
          continue;
        }
        candidates.push({ row, text });
      }
    }

    if (args.dryRun) {
      console.log(JSON.stringify({
        status: 'dry_run',
        mode: 'async_oss_vector_materials',
        days: args.days,
        tables,
        fetched,
        candidates: candidates.length,
        skipped,
        pgvectorAvailable: schema.pgvectorAvailable,
        sourceConflictKey: schema.sourceConflictKey,
        fallbackReason: schema.fallbackReason,
      }, null, 2));
      return;
    }

    if (!candidates.length) {
      const stats = await vectorStats(pool, args.pgTable);
      console.log(JSON.stringify({
        status: 'empty',
        mode: 'async_oss_vector_materials',
        reason: 'No rows require async embedding for the selected window and model',
        days: args.days,
        tables,
        fetched,
        skipped,
        ...stats,
      }, null, 2));
      return;
    }

    const inputText = `${candidates.map((item) => item.text.replace(/\r?\n/g, ' ')).join('\n')}\n`;
    const objectKey = `${trimSlashes(args.ossPrefix)}/vector-materials-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.txt`;
    await putOssObject(args, objectKey, Buffer.from(inputText, 'utf-8'), 'text/plain; charset=utf-8');
    const signedInputUrl = signedOssGetUrl(args, objectKey, 24 * 60 * 60);
    const task = await createDashScopeTask(args, signedInputUrl);
    const resultUrl = await waitDashScopeTask(args, task.taskId);
    const results = await downloadEmbeddingResults(resultUrl);
    const indexed = await upsertAsyncResults(pool, args, schema, candidates, results);
    const resultErrors = results.filter((item) => item.error).length;
    const stats = await vectorStats(pool, args.pgTable);

    console.log(JSON.stringify({
      status: 'ok',
      mode: 'async_oss_vector_materials',
      embeddingModel: args.embeddingModel,
      embeddingDimensions: args.embeddingDimensions,
      days: args.days,
      tables,
      fetched,
      candidates: candidates.length,
      skipped,
      uploadedObject: objectKey,
      taskId: task.taskId,
      resultRows: results.length,
      resultErrors,
      indexed,
      pgvectorAvailable: schema.pgvectorAvailable,
      sourceConflictKey: schema.sourceConflictKey,
      fallbackReason: schema.fallbackReason,
      ...stats,
    }, null, 2));
  } finally {
    await pool.end();
  }
}

async function loadArgs(): Promise<Args> {
  const parsed = parseCliArgs(process.argv.slice(2));
  const mysqlContainer = String(parsed.mysqlContainer || process.env.MYSQL_DOCKER_CONTAINER || 'my_mysql');
  const inspected = await inspectMysqlEnv(mysqlContainer).catch(() => ({} as Record<string, string>));
  const dashscopeApiKey = String(
    parsed.dashscopeApiKey ||
    process.env.DASHSCOPE_API_KEY ||
    process.env.OPENAI_API_KEY ||
    await effectiveEmbeddingKey(),
  );
  return {
    days: positiveInt(parsed.days, Number(process.env.VECTOR_BACKFILL_DAYS || 1), 30),
    maxRows: positiveInt(parsed.maxRows || parsed.limit, Number(process.env.VECTOR_BACKFILL_MAX_ROWS || 10_000), 100_000),
    mysqlContainer,
    mysqlDatabase: String(parsed.mysqlDatabase || process.env.MYSQL_DATABASE || 'news'),
    mysqlUser: String(parsed.mysqlUser || process.env.MYSQL_USER || 'root'),
    mysqlPassword: String(parsed.mysqlPassword || process.env.MYSQL_PASSWORD || inspected.MYSQL_ROOT_PASSWORD || inspected.MYSQL_PASSWORD || ''),
    pgTable: String(parsed.pgTable || process.env.PGVECTOR_NEWS_TABLE || 'vector_materials'),
    embeddingModel: String(parsed.embeddingModel || process.env.PGVECTOR_EMBEDDING_MODEL || 'text-embedding-async-v2'),
    embeddingDimensions: positiveInt(parsed.embeddingDimensions || process.env.PGVECTOR_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_DIMENSIONS, 4096),
    dryRun: Boolean(parsed.dryRun || process.env.VECTOR_BACKFILL_DRY_RUN === '1'),
    pollIntervalSec: positiveInt(parsed.pollIntervalSec || process.env.DASHSCOPE_POLL_INTERVAL_SEC, 15, 300),
    pollTimeoutSec: positiveInt(parsed.pollTimeoutSec || process.env.DASHSCOPE_POLL_TIMEOUT_SEC, 7200, 86_400),
    ossAccessKeyId: String(parsed.ossAccessKeyId || process.env.ALI_OSS_ACCESS_KEY_ID || process.env.OSS_ACCESS_KEY_ID || ''),
    ossAccessKeySecret: String(parsed.ossAccessKeySecret || process.env.ALI_OSS_ACCESS_KEY_SECRET || process.env.OSS_ACCESS_KEY_SECRET || ''),
    ossBucket: String(parsed.ossBucket || process.env.ALI_OSS_BUCKET || process.env.OSS_BUCKET || ''),
    ossEndpoint: String(parsed.ossEndpoint || process.env.ALI_OSS_ENDPOINT || process.env.OSS_ENDPOINT || 'oss-cn-beijing.aliyuncs.com').replace(/^https?:\/\//, ''),
    ossPrefix: String(parsed.ossPrefix || process.env.ALI_OSS_PREFIX || process.env.OSS_PREFIX || 'dashscope-embedding-inputs'),
    dashscopeApiKey,
  };
}

function parseCliArgs(argv: string[]): Record<string, string | boolean> {
  const result: Record<string, string | boolean> = {};
  for (const arg of argv) {
    if (arg === '--' || !arg.startsWith('--')) continue;
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

async function effectiveEmbeddingKey(): Promise<string> {
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

async function ensureVectorMaterialsSchema(pool: PgPool, args: Args): Promise<SchemaState> {
  const available = await pool.query(`SELECT 1 FROM pg_available_extensions WHERE name = 'vector' LIMIT 1`);
  let pgvectorAvailable = Boolean(available.rows.length);
  let fallbackReason = '';
  if (pgvectorAvailable) {
    try {
      await pool.query('CREATE EXTENSION IF NOT EXISTS vector');
    } catch (error) {
      pgvectorAvailable = false;
      fallbackReason = safeError(error);
    }
  }

  await pool.query(
    `CREATE TABLE IF NOT EXISTS ${qi(args.pgTable)} (
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
    await pool.query(`ALTER TABLE ${qi(args.pgTable)} ADD COLUMN IF NOT EXISTS ${qi(name)} ${type}`);
  }

  if (pgvectorAvailable) {
    await pool.query(`ALTER TABLE ${qi(args.pgTable)} ADD COLUMN IF NOT EXISTS embedding_vector vector(${args.embeddingDimensions})`);
  }
  await pool.query(`CREATE INDEX IF NOT EXISTS ${qi(`${args.pgTable}_publish_time_idx`)} ON ${qi(args.pgTable)} (publish_time DESC NULLS LAST)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS ${qi(`${args.pgTable}_indexed_at_idx`)} ON ${qi(args.pgTable)} (indexed_at DESC NULLS LAST)`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${qi(`${args.pgTable}_mysql_source_uidx`)} ON ${qi(args.pgTable)} (mysql_database, mysql_table_name, mysql_id, embedding_model)`);

  let sourceConflictKey: SchemaState['sourceConflictKey'] = 'source';
  try {
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${qi(`${args.pgTable}_mysql_source_only_uidx`)} ON ${qi(args.pgTable)} (mysql_database, mysql_table_name, mysql_id)`);
  } catch (error) {
    sourceConflictKey = 'source_model';
    fallbackReason = fallbackReason || `source-only unique index was not created: ${safeError(error)}`;
  }

  if (pgvectorAvailable) {
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS ${qi(`${args.pgTable}_embedding_hnsw_idx`)} ON ${qi(args.pgTable)} USING hnsw (embedding_vector vector_cosine_ops)`);
    } catch (error) {
      fallbackReason = fallbackReason || `HNSW index was not created: ${safeError(error)}`;
    }
  } else {
    fallbackReason = fallbackReason || 'pgvector extension is unavailable; embeddings cannot be stored in embedding_vector';
  }
  return { pgvectorAvailable, sourceConflictKey, fallbackReason };
}

async function existingMysqlIds(pool: PgPool, args: Args, table: string): Promise<Set<number>> {
  try {
    const result = await pool.query(
      `SELECT mysql_id
         FROM ${qi(args.pgTable)}
        WHERE mysql_database = $1
          AND mysql_table_name = $2
          AND embedding_model = $3
          AND embedding_vector IS NOT NULL`,
      [args.mysqlDatabase, table, args.embeddingModel],
    );
    return new Set(result.rows.map((row) => Number(row.mysql_id)).filter((value) => Number.isFinite(value)));
  } catch {
    return new Set();
  }
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

async function putOssObject(args: Args, objectKey: string, body: Buffer, contentType: string): Promise<void> {
  const date = new Date().toUTCString();
  const resource = `/${args.ossBucket}/${objectKey}`;
  const signature = hmacSha1(args.ossAccessKeySecret, `PUT\n\n${contentType}\n${date}\n${resource}`);
  await httpsRequest({
    method: 'PUT',
    hostname: `${args.ossBucket}.${args.ossEndpoint}`,
    path: ossObjectPath(objectKey),
    headers: {
      Authorization: `OSS ${args.ossAccessKeyId}:${signature}`,
      Date: date,
      'Content-Type': contentType,
      'Content-Length': String(body.length),
    },
  }, body);
}

function signedOssGetUrl(args: Args, objectKey: string, ttlSeconds: number): string {
  const expires = Math.floor(Date.now() / 1000) + ttlSeconds;
  const resource = `/${args.ossBucket}/${objectKey}`;
  const signature = hmacSha1(args.ossAccessKeySecret, `GET\n\n\n${expires}\n${resource}`);
  const query = new URLSearchParams({
    OSSAccessKeyId: args.ossAccessKeyId,
    Expires: String(expires),
    Signature: signature,
  });
  return `https://${args.ossBucket}.${args.ossEndpoint}${ossObjectPath(objectKey)}?${query.toString()}`;
}

async function createDashScopeTask(args: Args, inputUrl: string): Promise<DashScopeTask> {
  const response = await fetch('https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${args.dashscopeApiKey}`,
      'Content-Type': 'application/json',
      'X-DashScope-Async': 'enable',
    },
    body: JSON.stringify({
      model: args.embeddingModel,
      input: { url: inputUrl },
      parameters: { text_type: 'document' },
    }),
  });
  const payload = await response.json().catch(() => ({})) as Record<string, unknown>;
  if (!response.ok) throw new Error(`DashScope async task creation failed: ${response.status} ${safeJson(payload)}`);
  const taskId = String((payload.output as Record<string, unknown> | undefined)?.task_id || payload.task_id || '');
  if (!taskId) throw new Error(`DashScope async task id was not returned: ${safeJson(payload)}`);
  return { taskId };
}

async function waitDashScopeTask(args: Args, taskId: string): Promise<string> {
  const deadline = Date.now() + args.pollTimeoutSec * 1000;
  let lastStatus = '';
  while (Date.now() < deadline) {
    const response = await fetch(`https://dashscope.aliyuncs.com/api/v1/tasks/${encodeURIComponent(taskId)}`, {
      headers: { Authorization: `Bearer ${args.dashscopeApiKey}` },
    });
    const payload = await response.json().catch(() => ({})) as Record<string, unknown>;
    if (!response.ok) throw new Error(`DashScope task polling failed: ${response.status} ${safeJson(payload)}`);
    const output = (payload.output || {}) as Record<string, unknown>;
    lastStatus = String(output.task_status || payload.task_status || '');
    if (lastStatus === 'SUCCEEDED') {
      const resultUrl = dashScopeResultUrl(output);
      if (!resultUrl) throw new Error(`DashScope task succeeded but no result URL was returned: ${safeJson(payload)}`);
      return resultUrl;
    }
    if (['FAILED', 'CANCELED', 'UNKNOWN'].includes(lastStatus)) {
      throw new Error(`DashScope task ${lastStatus}: ${safeJson(payload)}`);
    }
    await sleep(args.pollIntervalSec * 1000);
  }
  throw new Error(`DashScope task timed out after ${args.pollTimeoutSec}s; last status=${lastStatus || 'unknown'}`);
}

function dashScopeResultUrl(output: Record<string, unknown>): string {
  if (typeof output.url === 'string') return output.url;
  if (Array.isArray(output.results)) {
    for (const item of output.results) {
      if (item && typeof item === 'object' && typeof (item as Record<string, unknown>).url === 'string') {
        return String((item as Record<string, unknown>).url);
      }
    }
  }
  return '';
}

async function downloadEmbeddingResults(resultUrl: string): Promise<AsyncEmbeddingResult[]> {
  const response = await fetch(resultUrl);
  if (!response.ok) throw new Error(`DashScope result download failed: ${response.status}`);
  const buffer = Buffer.from(await response.arrayBuffer());
  const contentEncoding = response.headers.get('content-encoding') || '';
  const body = resultUrl.endsWith('.gz') || contentEncoding.includes('gzip') ? gunzipSync(buffer).toString('utf-8') : buffer.toString('utf-8');
  const raw: Array<{ textIndex: number; embedding: number[]; error?: string }> = [];
  for (const line of body.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const parsed = JSON.parse(trimmed) as Record<string, unknown>;
      const output = (parsed.output || {}) as Record<string, unknown>;
      const code = Number(output.code || parsed.code || 0);
      const error = code && code !== 200 ? String(output.message || parsed.message || `code=${code}`) : '';
      const embedding = Array.isArray(output.embedding)
        ? output.embedding.map((value) => Number(value)).filter((value) => Number.isFinite(value))
        : [];
      const textIndex = Number(output.text_index ?? parsed.text_index ?? raw.length);
      raw.push({ textIndex, embedding, error });
    } catch (error) {
      raw.push({ textIndex: raw.length, embedding: [], error: safeError(error) });
    }
  }
  if (!raw.length) return [];
  const minIndex = Math.min(...raw.map((item) => item.textIndex).filter((value) => Number.isFinite(value)));
  const shift = minIndex === 1 ? 1 : 0;
  return raw.map((item, lineIndex) => ({
    index: Number.isFinite(item.textIndex) ? item.textIndex - shift : lineIndex,
    embedding: item.embedding,
    error: item.error,
  }));
}

async function upsertAsyncResults(
  pool: PgPool,
  args: Args,
  schema: SchemaState,
  candidates: Candidate[],
  results: AsyncEmbeddingResult[],
): Promise<number> {
  let indexed = 0;
  for (const result of results) {
    const candidate = candidates[result.index];
    if (!candidate || result.error || result.embedding.length !== args.embeddingDimensions) continue;
    await upsertVectorMaterial(pool, args, schema, candidate.row, candidate.text, result.embedding);
    indexed += 1;
  }
  return indexed;
}

async function upsertVectorMaterial(
  pool: PgPool,
  args: Args,
  schema: SchemaState,
  row: MysqlRow,
  text: string,
  embedding: number[],
): Promise<void> {
  const embeddingValue = toVectorLiteral(embedding);
  const conflictTarget = schema.sourceConflictKey === 'source'
    ? '(mysql_database, mysql_table_name, mysql_id)'
    : '(mysql_database, mysql_table_name, mysql_id, embedding_model)';
  await pool.query(
    `INSERT INTO ${qi(args.pgTable)}
      (mysql_database, mysql_table_name, mysql_id, ch_title, entitle, summary, publish_time,
       designated_tag, data_type, data_source_url, website_name, tag, content_hash, embedding_model,
       embedding, embedding_vector, embedding_dimensions, indexed_at, vector_status, error_message)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::vector,$17,now(),'ready',NULL)
     ON CONFLICT ${conflictTarget}
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
       embedding_model = EXCLUDED.embedding_model,
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

async function vectorStats(pool: PgPool, table: string): Promise<{ totalRows: number; vectorRows: number; lastIndexedAt: string | null }> {
  const result = await pool.query(
    `SELECT count(*)::int AS total_rows,
            count(embedding_vector)::int AS vector_rows,
            max(indexed_at)::text AS last_indexed_at
       FROM ${qi(table)}`,
  );
  return {
    totalRows: Number(result.rows[0]?.total_rows || 0),
    vectorRows: Number(result.rows[0]?.vector_rows || 0),
    lastIndexedAt: result.rows[0]?.last_indexed_at ? String(result.rows[0].last_indexed_at) : null,
  };
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

function httpsRequest(options: https.RequestOptions, body?: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const request = https.request(options, (response) => {
      const chunks: Buffer[] = [];
      response.on('data', (chunk: Buffer) => chunks.push(chunk));
      response.on('end', () => {
        const data = Buffer.concat(chunks);
        const status = response.statusCode || 0;
        if (status < 200 || status >= 300) {
          reject(new Error(`HTTPS request failed: ${status} ${data.toString('utf-8').slice(0, 300)}`));
        } else {
          resolve(data);
        }
      });
    });
    request.on('error', reject);
    if (body) request.write(body);
    request.end();
  });
}

function hmacSha1(secret: string, value: string): string {
  return crypto.createHmac('sha1', secret).update(value).digest('base64');
}

function ossObjectPath(objectKey: string): string {
  return `/${objectKey.split('/').map(encodeURIComponent).join('/')}`;
}

function trimSlashes(value: string): string {
  return value.replace(/^\/+|\/+$/g, '') || 'dashscope-embedding-inputs';
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

function safeJson(value: unknown): string {
  return JSON.stringify(value).replace(/(api[_-]?key|token|secret)["']?\s*[:=]\s*["'][^"']+["']/gi, '$1:"***"').slice(0, 600);
}

function safeError(error: unknown): string {
  return (error instanceof Error ? error.message : String(error))
    .replace(/postgres(?:ql)?:\/\/[^@\s]+@/gi, 'postgres://***@')
    .replace(/(api[_-]?key|token|secret)[=:]\s*[^,\s]+/gi, '$1=***')
    .slice(0, 500);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(JSON.stringify({ status: 'failed', error: safeError(error) }, null, 2));
  process.exitCode = 1;
});
