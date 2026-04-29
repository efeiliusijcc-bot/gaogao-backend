import { Injectable } from '@nestjs/common';
import { marked } from 'marked';
import { Subject } from 'rxjs';
import { v4 as uuid } from 'uuid';
import { OpenClawApprovalRequiredError, OpenClawService } from './openclaw.service.js';
import { RemoteFileService } from './remote-file.service.js';
import type { CreateJobRequest } from '../src/types/report.js';
import type { JobRecord, ServerEvent } from './types.js';

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
    };

    this.jobs.set(jobId, job);
    this.streams.set(jobId, new Subject<ServerEvent>());
    void this.writeJobState(job);
    setImmediate(() => void this.runJob(job));

    return { jobId, status: job.status };
  }

  listJobs() {
    return Array.from(this.jobs.values()).map((job) => ({
      jobId: job.jobId,
      skill: job.skill,
      payload: job.payload,
      status: job.status,
      stage: job.stage,
      errorMessage: job.errorMessage,
      resultPath: job.resultPath,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
    }));
  }

  getJob(jobId: string): JobRecord | undefined {
    return this.jobs.get(jobId);
  }

  getStream(jobId: string): Subject<ServerEvent> | undefined {
    return this.streams.get(jobId);
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
      const result = await this.openClaw.runReport({
        skill: job.skill,
        payload: job.payload as unknown as Record<string, unknown>,
        requestUser,
        onEvent: (event) => this.pushEvent(job, event),
      });

      const resolvedReport = await this.resolveOpenClawReportFile(result.markdown, startedAtMs);
      job.status = 'succeeded';
      job.markdown = resolvedReport?.markdown ?? result.markdown;
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
    if (event.type === 'stage') {
      job.stage = event.stage;
    }
    this.streams.get(job.jobId)?.next(event);
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

  private isValidReportMarkdown(markdown: string, size: number): boolean {
    const text = markdown.trim();
    if (!text) return false;
    if (size < 2000) return false;
    if (/agent couldn't generate a response/i.test(text)) return false;
    if (/please try again/i.test(text) && text.length < 1000) return false;
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
