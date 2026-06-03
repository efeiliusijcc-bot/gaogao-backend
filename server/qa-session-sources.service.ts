import { BadRequestException, Injectable } from '@nestjs/common';
import { OPENCLAW_QA_ARTIFACT_DIR } from './config.js';
import { RemoteFileService } from './remote-file.service.js';

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export interface QaSessionSourcesRecord {
  sessionId: string;
  updatedAt: string | null;
  sourceCount: number;
  sources: Record<string, JsonValue>[];
}

interface UpsertSourcesInput {
  sources?: unknown;
  merge?: boolean;
}

const MAX_SESSION_ID_LENGTH = 120;
const MAX_SOURCES = 100;
const MAX_STRING_LENGTH = 2000;
const MAX_OBJECT_KEYS = 80;
const MAX_ARRAY_ITEMS = 50;
const MAX_DEPTH = 4;

@Injectable()
export class QaSessionSourcesService {
  constructor(private readonly remoteFs: RemoteFileService) {}

  async getSources(sessionId: string): Promise<QaSessionSourcesRecord> {
    const safeSessionId = this.safeSessionId(sessionId);
    const filePath = this.sourcesFilePath(safeSessionId);
    const exists = await this.remoteFs.exists(filePath);
    if (!exists) return this.emptyRecord(safeSessionId);

    try {
      const parsed = JSON.parse(await this.remoteFs.readFile(filePath)) as Partial<QaSessionSourcesRecord>;
      const sources = Array.isArray(parsed.sources) ? this.normalizeSources(parsed.sources) : [];
      const updatedAt = typeof parsed.updatedAt === 'string' ? parsed.updatedAt : null;
      return {
        sessionId: safeSessionId,
        updatedAt,
        sourceCount: sources.length,
        sources,
      };
    } catch {
      return this.emptyRecord(safeSessionId);
    }
  }

  async upsertSources(sessionId: string, input: UpsertSourcesInput): Promise<QaSessionSourcesRecord> {
    const safeSessionId = this.safeSessionId(sessionId);
    const incoming = this.normalizeSources(input.sources);
    const merge = input.merge !== false;
    const current = merge ? await this.getSources(safeSessionId) : this.emptyRecord(safeSessionId);
    const sources = this.dedupeSources([...(merge ? current.sources : []), ...incoming]).slice(0, MAX_SOURCES);
    const record: QaSessionSourcesRecord = {
      sessionId: safeSessionId,
      updatedAt: new Date().toISOString(),
      sourceCount: sources.length,
      sources,
    };

    const dirPath = this.sessionDirPath(safeSessionId);
    await this.remoteFs.mkdir(dirPath);
    await this.remoteFs.writeFile(this.sourcesFilePath(safeSessionId), `${JSON.stringify(record, null, 2)}\n`);
    return record;
  }

  private safeSessionId(sessionId: string): string {
    const safe = String(sessionId || '')
      .trim()
      .replace(/[^a-zA-Z0-9_.:-]/g, '_')
      .slice(0, MAX_SESSION_ID_LENGTH);
    if (!safe || safe === '.' || safe === '..') throw new BadRequestException({ error: 'sessionId is required' });
    return safe;
  }

  private sessionDirPath(safeSessionId: string): string {
    return this.remoteFs.joinPath(OPENCLAW_QA_ARTIFACT_DIR, safeSessionId);
  }

  private sourcesFilePath(safeSessionId: string): string {
    return this.remoteFs.joinPath(this.sessionDirPath(safeSessionId), 'sources.json');
  }

  private emptyRecord(sessionId: string): QaSessionSourcesRecord {
    return {
      sessionId,
      updatedAt: null,
      sourceCount: 0,
      sources: [],
    };
  }

  private normalizeSources(value: unknown): Record<string, JsonValue>[] {
    if (!Array.isArray(value)) return [];
    return value
      .map((item) => this.sanitizeSource(item))
      .filter((item): item is Record<string, JsonValue> => item !== null)
      .slice(0, MAX_SOURCES);
  }

  private sanitizeSource(value: unknown): Record<string, JsonValue> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    const sanitized = this.sanitizeJson(value, 0);
    if (!sanitized || typeof sanitized !== 'object' || Array.isArray(sanitized)) return null;
    return sanitized;
  }

  private sanitizeJson(value: unknown, depth: number): JsonValue | undefined {
    if (value === null || typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.slice(0, MAX_STRING_LENGTH);
    if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
    if (depth >= MAX_DEPTH) return undefined;

    if (Array.isArray(value)) {
      const items = value
        .slice(0, MAX_ARRAY_ITEMS)
        .map((item) => this.sanitizeJson(item, depth + 1))
        .filter((item): item is JsonValue => item !== undefined);
      return items;
    }

    if (value && typeof value === 'object') {
      const entries = Object.entries(value as Record<string, unknown>).slice(0, MAX_OBJECT_KEYS);
      const result: Record<string, JsonValue> = {};
      for (const [key, item] of entries) {
        const safeKey = key.slice(0, 120);
        const sanitized = this.sanitizeJson(item, depth + 1);
        if (safeKey && sanitized !== undefined) result[safeKey] = sanitized;
      }
      return result;
    }

    return undefined;
  }

  private dedupeSources(sources: Record<string, JsonValue>[]): Record<string, JsonValue>[] {
    const seen = new Set<string>();
    const result: Record<string, JsonValue>[] = [];

    for (const source of sources) {
      const key = this.sourceDedupeKey(source);
      if (seen.has(key)) continue;
      seen.add(key);
      result.push(source);
    }

    return result;
  }

  private sourceDedupeKey(source: Record<string, JsonValue>): string {
    const url = this.firstString(source, ['url', 'source_url', 'data_source_url']);
    if (url) return `url:${url}`;

    const title = this.firstString(source, ['title', 'ch_title', 'headline', 'sourceTitle']);
    const publisher = this.firstString(source, ['publisher', 'website_name', 'source_name', 'site_name']);
    const fallback = JSON.stringify(source).slice(0, 300);
    return `meta:${title}|${publisher}|${fallback}`;
  }

  private firstString(source: Record<string, JsonValue>, keys: string[]): string {
    for (const key of keys) {
      const value = source[key];
      if (typeof value === 'string' && value.trim()) return value.trim().toLowerCase();
    }
    return '';
  }
}
