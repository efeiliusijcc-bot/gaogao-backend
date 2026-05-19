import { Injectable } from '@nestjs/common';
import { marked } from 'marked';
import { Subject } from 'rxjs';
import { v4 as uuid } from 'uuid';
import { OpenClawApprovalRequiredError, OpenClawService } from './openclaw.service.js';
import { RemoteFileService } from './remote-file.service.js';
import type { CreateJobRequest } from '../src/types/report.js';
import type { EventLogEntry, JobRecord, RunInput, ServerEvent } from './types.js';

type JobListTypeFilter = 'all' | 'write-hb-k' | 'write-hb-hb' | 'person-intelligence-report' | 'risk-assessment-reports';

interface JobListOptions {
  page?: string | number;
  pageSize?: string | number;
  type?: string;
  q?: string;
}

@Injectable()
export class ReportsService {
  private readonly jobs = new Map<string, JobRecord>();
  private readonly streams = new Map<string, Subject<ServerEvent>>();
  private dailySequence = new Map<string, number>();

  constructor(
    private readonly openClaw: OpenClawService,
    private readonly remoteFs: RemoteFileService,
  ) {
    void this.loadPersistedJobs();
  }

  createJob(req: CreateJobRequest): { jobId: string; status: string } {
    const jobId = uuid();
    const now = new Date().toISOString();
    const job: JobRecord = {
      jobId,
      skill: req.skill,
      payload: req.payload,
      status: 'queued',
      artifacts: {},
      createdAt: now,
      updatedAt: now,
      events: [],
      eventLog: [],
    };

    this.jobs.set(jobId, job);
    this.streams.set(jobId, new Subject<ServerEvent>());
    void this.writeJobState(job);
    setImmediate(() => void this.runJob(job));

    return { jobId, status: job.status };
  }

  listJobs(options: JobListOptions = {}) {
    const page = this.parsePositiveInt(options.page, 1);
    const pageSize = Math.min(this.parsePositiveInt(options.pageSize, 20), 100);
    const type = this.normalizeTypeFilter(options.type);
    const query = String(options.q ?? '').trim().toLowerCase();

    const filtered = Array.from(this.jobs.values())
      .filter((job) => type === 'all' || this.jobTypeKey(job) === type)
      .filter((job) => !query || this.jobSearchText(job).includes(query))
      .sort((a, b) => new Date(b.updatedAt || b.createdAt).getTime() - new Date(a.updatedAt || a.createdAt).getTime());

    const total = filtered.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage = Math.min(page, totalPages);
    const start = (safePage - 1) * pageSize;
    const items = filtered.slice(start, start + pageSize).map((job) => this.serializeJob(job));

    return {
      items,
      total,
      page: safePage,
      pageSize,
      totalPages,
      statusCounts: {
        succeeded: filtered.filter((job) => job.status === 'succeeded').length,
        running: filtered.filter((job) => job.status === 'running' || job.status === 'queued').length,
      },
    };
  }

  getJob(jobId: string): JobRecord | undefined {
    return this.jobs.get(jobId);
  }

  getStream(jobId: string): Subject<ServerEvent> | undefined {
    return this.streams.get(jobId);
  }

  getEventLog(jobId: string): { jobId: string; items: EventLogEntry[] } | undefined {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    return { jobId, items: job.eventLog ?? [] };
  }

  private serializeJob(job: JobRecord) {
    return {
      jobId: job.jobId,
      skill: job.skill,
      payload: job.payload,
      status: job.status,
      stage: job.stage,
      errorMessage: job.errorMessage,
      resultPath: job.resultPath,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
    };
  }

  private parsePositiveInt(value: string | number | undefined, fallback: number): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 1) return fallback;
    return Math.floor(parsed);
  }

  private normalizeTypeFilter(value: string | undefined): JobListTypeFilter {
    const allowed = new Set<JobListTypeFilter>([
      'all',
      'write-hb-k',
      'write-hb-hb',
      'person-intelligence-report',
      'risk-assessment-reports',
    ]);
    return allowed.has(value as JobListTypeFilter) ? (value as JobListTypeFilter) : 'all';
  }

  private jobTypeKey(job: JobRecord): JobListTypeFilter {
    if (job.skill === 'write-hb') {
      const reportType = String((job.payload as { report_type?: unknown }).report_type ?? '').toLowerCase();
      return reportType.includes('hb') ? 'write-hb-hb' : 'write-hb-k';
    }
    if (job.skill === 'person-intelligence-report') return 'person-intelligence-report';
    if (job.skill === 'risk-assessment-reports') return 'risk-assessment-reports';
    return 'all';
  }

  private jobSearchText(job: JobRecord): string {
    return [
      job.jobId,
      job.skill,
      job.status,
      job.stage,
      job.errorMessage,
      job.resultPath,
      this.payloadSearchText(job.payload),
    ].join(' ').toLowerCase();
  }

  private payloadSearchText(value: unknown): string {
    if (value === null || value === undefined) return '';
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
    if (Array.isArray(value)) return value.map((item) => this.payloadSearchText(item)).join(' ');
    if (typeof value === 'object') return Object.values(value).map((item) => this.payloadSearchText(item)).join(' ');
    return '';
  }

  async getResult(jobId: string) {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    if (job.status !== 'succeeded') return null;
    return { html: await this.renderMarkdownToHtml(job.markdown ?? ''), artifacts: job.artifacts };
  }

  async getResultFromDisk(jobId: string) {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    if (job.status !== 'succeeded') return null;

    const reportDir = this.remoteFs.remoteDir;
    const jobScopedPath = this.remoteFs.joinPath(reportDir, `${job.jobId}.md`);
    const hasJobScopedFile = await this.remoteFs.exists(jobScopedPath);

    let resultFilePath = job.resultPath ?? null;
    if (resultFilePath && !this.remoteFs.isInsideReportDir(resultFilePath)) {
      const remapped = this.remoteFs.remapToReportDir(resultFilePath);
      if (remapped && await this.remoteFs.exists(remapped)) {
        resultFilePath = remapped;
        job.resultPath = remapped;
      } else {
        resultFilePath = null;
      }
    }

    const direct = await this.readMarkdownFile(hasJobScopedFile ? jobScopedPath : resultFilePath);
    if (hasJobScopedFile && direct) {
      return { html: await this.renderMarkdownToHtml(direct.markdown), artifacts: job.artifacts, resultPath: direct.filePath };
    }

    const fallback = direct ?? (await this.findBestMarkdownFileForJob(job));
    const markdown = fallback?.markdown ?? job.markdown ?? '';

    if (fallback?.filePath && fallback.filePath !== job.resultPath) {
      job.resultPath = fallback.filePath;
      job.markdown = fallback.markdown;
      job.updatedAt = new Date().toISOString();
      await this.writeJobState(job);
    }

    return { html: await this.renderMarkdownToHtml(markdown), artifacts: job.artifacts, resultPath: fallback?.filePath ?? job.resultPath };
  }

  async getMarkdownFromDisk(jobId: string) {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    if (job.status !== 'succeeded' || !job.resultPath) return null;

    const markdown = await this.remoteFs.readFile(job.resultPath);
    return { markdown, artifacts: job.artifacts, resultPath: job.resultPath };
  }

  private async runJob(job: JobRecord) {
    job.status = 'running';
    job.updatedAt = new Date().toISOString();
    await this.writeJobState(job);
    const startedAtMs = Date.now();

    try {
      const requestUser = this.buildRequestUser(job);
      const runInput: RunInput = {
        skill: job.skill,
        payload: job.payload as unknown as Record<string, unknown>,
        requestUser,
        onEvent: (event) => this.pushEvent(job, event),
      };
      let result;
      let recoveredReport: Awaited<ReturnType<typeof this.resolveOpenClawReportFile>> = null;
      try {
        result = await this.openClaw.runReportViaGateway(runInput);
      } catch (gatewayError) {
        const message = gatewayError instanceof Error ? gatewayError.message : String(gatewayError);
        this.pushEvent(job, {
          type: 'stage',
          stage: 'gateway_fallback',
          message: `OpenClaw Gateway event stream unavailable; falling back to non-streaming generation. ${message}`,
        });
        recoveredReport = await this.resolveOpenClawReportFile('', startedAtMs);
        if (recoveredReport) {
          this.pushEvent(job, {
            type: 'stage',
            stage: 'report_file_recovered',
            message: `Recovered generated report file after empty Gateway response: ${recoveredReport.filePath}`,
          });
          result = { markdown: `REPORT_FILE: ${recoveredReport.filePath}`, artifacts: {} };
        } else {
          try {
            result = await this.openClaw.runReport(runInput);
          } catch (fallbackError) {
            recoveredReport = await this.resolveOpenClawReportFile('', startedAtMs);
            if (!recoveredReport) throw fallbackError;
            this.pushEvent(job, {
              type: 'stage',
              stage: 'report_file_recovered',
              message: `Recovered generated report file after empty fallback response: ${recoveredReport.filePath}`,
            });
            result = { markdown: `REPORT_FILE: ${recoveredReport.filePath}`, artifacts: {} };
          }
        }
      }

      const resolvedReport = recoveredReport ?? (await this.resolveOpenClawReportFile(result.markdown, startedAtMs));
      const finalMarkdown = resolvedReport?.markdown ?? result.markdown;
      if (!resolvedReport && /^\s*REPORT_FILE\s*:/im.test(finalMarkdown)) {
        throw new Error('OpenClaw returned a REPORT_FILE pointer, but no valid Markdown report file was found.');
      }
      this.assertUsableGeneratedMarkdown(finalMarkdown);
      job.status = 'succeeded';
      job.markdown = finalMarkdown;
      job.artifacts = result.artifacts;
      job.resultPath = resolvedReport?.filePath ?? (await this.writeReportFile(job, job.markdown));
      job.updatedAt = new Date().toISOString();
      await this.writeJobState(job);
      this.pushEvent(job, { type: 'stage', stage: 'done', message: 'Report generation completed and saved to disk.' });
      this.pushEvent(job, { type: 'done', jobId: job.jobId });
      this.streams.get(job.jobId)?.complete();
    } catch (error) {
      if (error instanceof OpenClawApprovalRequiredError) {
        job.status = 'waiting_approval';
        job.markdown = error.partialOutput;
        job.updatedAt = new Date().toISOString();
        await this.writeJobState(job);
        this.pushEvent(job, {
          type: 'stage',
          stage: 'approval_required',
          message: 'OpenClaw is waiting for tool approval. Run the approval command in the OpenClaw chat/session, then create the report again.',
        });
        this.pushEvent(job, {
          type: 'approval_required',
          commands: error.commands,
          message: 'OpenClaw requires approval before it can use external tools.',
          partialOutput: error.partialOutput,
        });
        this.pushEvent(job, { type: 'done', jobId: job.jobId });
        this.streams.get(job.jobId)?.complete();
        return;
      }

      const message = error instanceof Error ? error.message : String(error);
      job.status = 'failed';
      job.stage = 'failed';
      job.errorMessage = message;
      job.updatedAt = new Date().toISOString();
      await this.writeJobState(job);
      this.pushEvent(job, { type: 'error', message });
      this.pushEvent(job, { type: 'done', jobId: job.jobId });
      this.streams.get(job.jobId)?.complete();
    }
  }

  private pushEvent(job: JobRecord, event: ServerEvent) {
    job.events.push(event);
    const logEntry = this.toEventLogEntry(job, event);
    if (logEntry) {
      job.eventLog.push(logEntry);
      if (job.eventLog.length > 500) job.eventLog = job.eventLog.slice(-500);
    }
    if (event.type === 'stage') {
      job.stage = event.stage;
    }
    this.streams.get(job.jobId)?.next(event);
    void this.writeJobState(job);
  }

  private toEventLogEntry(job: JobRecord, event: ServerEvent): EventLogEntry | null {
    const now = new Date().toISOString();
    const baseId = `${job.jobId}:${job.eventLog.length + 1}:${event.type}`;

    if (event.type === 'stage') {
      return {
        id: `${baseId}:${event.stage}`,
        time: now,
        type: 'stage',
        label: '阶段进度',
        status: event.stage || 'running',
        phase: event.stage,
        actor: this.inferEventActor(event.stage),
        summary: this.sanitizeLogText(event.message || event.stage || 'OpenClaw 阶段更新', 220),
      };
    }

    if (event.type === 'tool_start' || event.type === 'tool_end' || event.type === 'tool_error') {
      const raw = event.raw && typeof event.raw === 'object' ? (event.raw as Record<string, unknown>) : {};
      const status =
        this.firstLogString(raw, ['status']) ||
        (event.type === 'tool_start' ? 'started' : event.type === 'tool_end' ? 'completed' : 'failed');
      const label = this.sanitizeLogText(this.firstLogString(raw, ['label']) || event.name || 'Tool', 80);
      const summary = this.sanitizeLogText(
        this.firstLogString(raw, ['summary']) ||
          (event.type === 'tool_error' ? event.message : `${label} ${status}`),
        220,
      );
      const command = this.sanitizeCommandForEventLog(this.firstLogString(raw, ['command']));
      const phase = this.sanitizeLogText(this.firstLogString(raw, ['phase']), 80);
      const actor = this.sanitizeLogText(this.firstLogString(raw, ['actor']), 80);
      const detail = this.sanitizeLogText(this.firstLogString(raw, ['detail']), 220);
      return {
        id: `${baseId}:${event.id ?? job.eventLog.length + 1}`,
        time: now,
        type: event.type,
        label,
        status,
        summary,
        ...(command ? { command } : {}),
        ...(phase ? { phase } : {}),
        ...(actor ? { actor } : {}),
        ...(detail ? { detail } : {}),
      };
    }

    if (event.type === 'error') {
      return {
        id: baseId,
        time: now,
        type: 'error',
        label: '任务错误',
        status: 'failed',
        phase: 'error',
        actor: 'system',
        summary: this.sanitizeLogText(event.message || '任务失败', 220),
      };
    }

    if (event.type === 'done') {
      return {
        id: `${baseId}:${event.jobId}`,
        time: now,
        type: 'done',
        label: '任务完成',
        status: 'completed',
        phase: 'done',
        actor: 'system',
        summary: '后端任务已结束。',
      };
    }

    return null;
  }

  private firstLogString(value: Record<string, unknown>, keys: string[]): string {
    for (const key of keys) {
      const candidate = value[key];
      if (typeof candidate === 'string' && candidate.trim()) return candidate;
    }
    return '';
  }

  private inferEventActor(phase: string): string {
    if (/research/i.test(phase)) return 'research-agent';
    if (/synthesis/i.test(phase)) return 'synthesis-agent';
    if (/openclaw|running|waiting_final_report/i.test(phase)) return 'main-agent';
    return 'system';
  }

  private sanitizeCommandForEventLog(value: string): string {
    if (!value) return '';
    const sanitized = this.sanitizeLogText(value, 180)
      .replace(/(?:\/home\/node\/\.openclaw\/workspace\/|\/usr\/docker\/openclaw\/workspace\/)/g, '.../')
      .replace(/([A-Z0-9_]*(?:API_KEY|TOKEN|SECRET|PASSWORD|KEY)[A-Z0-9_]*=)[^\s"'`]+/gi, '$1[redacted]');
    return sanitized;
  }

  private sanitizeLogText(value: string, maxLength: number): string {
    const redacted = String(value)
      .replace(/\b(api[_-]?key|token|secret|password|authorization)\b\s*[:=]\s*["']?[^"'\s,;}]+/gi, '$1=[redacted]')
      .replace(/\b(?:sk|tp)-[a-zA-Z0-9_-]{16,}\b/g, '[redacted-key]')
      .replace(/Bearer\s+[a-zA-Z0-9._-]+/gi, 'Bearer [redacted]')
      .replace(/\s+/g, ' ')
      .trim();
    if (redacted.length <= maxLength) return redacted;
    return `${redacted.slice(0, maxLength - 1)}…`;
  }

  private async writeReportFile(job: JobRecord, markdown: string): Promise<string> {
    try {
      const reportDir = this.remoteFs.remoteDir;
      await this.remoteFs.mkdir(reportDir);
      const filePath = this.remoteFs.joinPath(reportDir, `${job.jobId}.md`);
      await this.remoteFs.writeFile(filePath, markdown);
      return filePath;
    } catch (err) {
      console.error('writeReportFile failed:', err instanceof Error ? err.message : err);
      return '';
    }
  }

  private async writeJobState(job: JobRecord): Promise<void> {
    try {
      const reportDir = this.remoteFs.remoteDir;
      await this.remoteFs.mkdir(reportDir);
      const statePath = this.remoteFs.joinPath(reportDir, `${job.jobId}.json`);
      const { markdown: _markdown, events, ...serializable } = job;
      await this.remoteFs.writeFile(
        statePath,
        JSON.stringify({ ...serializable, eventCount: events.length }, null, 2),
      );
    } catch (err) {
      console.error('writeJobState failed:', err instanceof Error ? err.message : err);
    }
  }

  private async loadPersistedJobs(): Promise<void> {
    try {
      const reportDir = this.remoteFs.remoteDir;
      await this.remoteFs.mkdir(reportDir);
      const entries = await this.remoteFs.readdir(reportDir);
      await Promise.all(
        entries
          .filter((entry) => entry.isFile && entry.name.toLowerCase().endsWith('.json'))
          .map(async (entry) => {
            try {
              const filePath = this.remoteFs.joinPath(reportDir, entry.name);
              const parsed = JSON.parse(await this.remoteFs.readFile(filePath)) as Partial<JobRecord>;
              if (!parsed.jobId || this.jobs.has(parsed.jobId)) return;

              this.jobs.set(parsed.jobId, {
                jobId: parsed.jobId,
                skill: parsed.skill ?? 'risk-assessment-reports',
                payload: parsed.payload ?? {},
                status: parsed.status ?? 'failed',
                artifacts: parsed.artifacts ?? {},
                createdAt: parsed.createdAt ?? new Date().toISOString(),
                updatedAt: parsed.updatedAt ?? parsed.createdAt ?? new Date().toISOString(),
                stage: parsed.stage,
                resultPath: parsed.resultPath,
                errorMessage: parsed.errorMessage,
                events: [],
                eventLog: Array.isArray(parsed.eventLog) ? parsed.eventLog.filter((item) => item && typeof item === 'object') as EventLogEntry[] : [],
              } as JobRecord);
            } catch {
              // Ignore corrupted persisted job files.
            }
          }),
      );
    } catch {
      // Ignore startup restore failures; new jobs still work.
    }
  }

  private async resolveOpenClawReportFile(markdown: string, startedAtMs: number) {
    const fromText = await this.readMarkdownFile(this.extractReportPath(markdown));
    if (fromText) return fromText;

    const latest = await this.findLatestMarkdownFile(startedAtMs);
    if (latest) return latest;

    return null;
  }

  private extractReportPath(text: string): string | null {
    const normalized = text.replaceAll('\\\\', '/');
    const pattern = /(?:\/home\/node\/\.openclaw\/workspace\/report-agent\/reports\/|\/usr\/docker\/openclaw\/workspace\/report-agent\/reports\/)[^\r\n`"'<>|?*]+?\.md/gi;
    const matches = Array.from(normalized.matchAll(pattern)).map((match) => match[0].trim());
    return matches.find((candidate) => this.remoteFs.isInsideReportDir(candidate)) ?? null;
  }

  private async findLatestMarkdownFile(startedAtMs: number) {
    try {
      const reportDir = this.remoteFs.remoteDir;
      const entries = await this.remoteFs.readdir(reportDir);
      const files: { filePath: string; stat: { size: number; mtimeMs: number } }[] = [];
      for (const entry of entries) {
        if (!entry.isFile || !entry.name.toLowerCase().endsWith('.md')) continue;
        const filePath = this.remoteFs.joinPath(reportDir, entry.name);
        try {
          const stat = await this.remoteFs.stat(filePath);
          files.push({ filePath, stat });
        } catch { continue; }
      }

      const latest = files
        .filter(({ filePath, stat }) => this.remoteFs.isInsideReportDir(filePath) && stat.mtimeMs >= startedAtMs - 5000)
        .sort((a, b) => b.stat.mtimeMs - a.stat.mtimeMs || b.stat.size - a.stat.size)[0];
      return latest ? this.readMarkdownFile(latest.filePath) : null;
    } catch {
      return null;
    }
  }

  private async findBestMarkdownFileForJob(job: JobRecord) {
    try {
      const startedAtMs = new Date(job.createdAt).getTime();
      const endedAtMs = new Date(job.updatedAt || job.createdAt).getTime();
      const reportDir = this.remoteFs.remoteDir;
      const entries = await this.remoteFs.readdir(reportDir);
      const files: { filePath: string; stat: { size: number; mtimeMs: number } }[] = [];
      for (const entry of entries) {
        if (!entry.isFile || !entry.name.toLowerCase().endsWith('.md')) continue;
        const filePath = this.remoteFs.joinPath(reportDir, entry.name);
        try {
          const stat = await this.remoteFs.stat(filePath);
          files.push({ filePath, stat });
        } catch { continue; }
      }

      const candidates = files
        .filter(({ filePath, stat }) => {
          if (!this.remoteFs.isInsideReportDir(filePath)) return false;
          if (stat.mtimeMs < startedAtMs - 10_000) return false;
          if (Number.isFinite(endedAtMs) && stat.mtimeMs > endedAtMs + 60_000) return false;
          return true;
        })
        .sort((a, b) => b.stat.size - a.stat.size || b.stat.mtimeMs - a.stat.mtimeMs);

      for (const candidate of candidates) {
        const report = await this.readMarkdownFile(candidate.filePath);
        if (report) return report;
      }

      return null;
    } catch {
      return null;
    }
  }

  private async readMarkdownFile(filePath: string | null) {
    if (!filePath || !this.remoteFs.isInsideReportDir(filePath)) return null;
    try {
      const stat = await this.remoteFs.stat(filePath);
      if (!stat.isFile) return null;
      const markdown = await this.remoteFs.readFile(filePath);
      return this.isValidReportMarkdown(markdown, stat.size) ? { filePath, markdown } : null;
    } catch {
      return null;
    }
  }

  private assertUsableGeneratedMarkdown(markdown: string): void {
    const text = String(markdown || '').trim();
    if (!text) throw new Error('OpenClaw report-agent returned empty report content.');
    if (/[{｛]\s*(?:jobId|报告名|filename|fileName|actual file name|实际文件名)\s*[}｝]/i.test(text)) {
      throw new Error('OpenClaw report-agent returned placeholder output instead of a report file.');
    }
    if (/REPORT_FILE\s*:\s*.+\.json\b/i.test(text) || /\/final\/summary\.json/i.test(text)) {
      throw new Error('OpenClaw report-agent returned a JSON summary path instead of a Markdown report.');
    }
    if (/复制报告到|copy\s+report\s+to/i.test(text) && text.length < 2000) {
      throw new Error('OpenClaw report-agent returned workflow instructions instead of a final report.');
    }
    if (/^no response from openclaw\.?$/i.test(text)) {
      throw new Error('OpenClaw report-agent returned no response.');
    }
    if (/agent couldn't generate a response/i.test(text)) {
      throw new Error("OpenClaw report-agent couldn't generate a response.");
    }
    if (/quota exhausted|429\s+quota|500\s+internal|internal error/i.test(text) && text.length < 2000) {
      throw new Error(text.slice(0, 300));
    }
    if (text.length < 1000 && !/REPORT_FILE:\s*\/.+\.md/i.test(text)) {
      throw new Error('OpenClaw report-agent returned too little report content.');
    }
  }

  private isValidReportMarkdown(markdown: string, size: number): boolean {
    const text = markdown.trim();
    if (!text) return false;
    if (size < 2000) return false;
    if (/[{｛]\s*(?:jobId|报告名|filename|fileName|actual file name|实际文件名)\s*[}｝]/i.test(text)) return false;
    if (/REPORT_FILE\s*:\s*.+\.json\b/i.test(text) || /\/final\/summary\.json/i.test(text)) return false;
    if (/复制报告到|copy\s+report\s+to/i.test(text) && text.length < 2000) return false;
    if (/^no response from openclaw\.?$/i.test(text)) return false;
    if (/agent couldn't generate a response/i.test(text)) return false;
    if (/please try again/i.test(text) && text.length < 1000) return false;
    if (/quota exhausted|429\s+quota|500\s+internal|internal error/i.test(text) && text.length < 2000) return false;
    if (/报告已生成并保存/.test(text) && size < 5000) return false;
    return true;
  }

  private buildRequestUser(job: JobRecord): string {
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, '0');
    const d = String(now.getDate()).padStart(2, '0');
    const key = `${y}${m}${d}`;
    const next = (this.dailySequence.get(key) ?? 0) + 1;
    this.dailySequence.set(key, next);
    return `report-task-${key}-${String(next).padStart(3, '0')}-${job.jobId.slice(0, 8)}`;
  }

  private async renderMarkdownToHtml(markdown: string): Promise<string> {
    const parsed = marked(markdown || '');
    return typeof parsed === 'string' ? parsed : await parsed;
  }
}
