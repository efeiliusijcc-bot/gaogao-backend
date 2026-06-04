import { Injectable } from '@nestjs/common';
import { marked } from 'marked';
import { Subject } from 'rxjs';
import { v4 as uuid } from 'uuid';
import { OpenClawApprovalRequiredError, OpenClawService } from './openclaw.service.js';
import { RemoteFileService } from './remote-file.service.js';
import { VectorSourceService, type VectorSearchResult, type VectorSourceItem } from './vector-source.service.js';
import type { CreateJobRequest } from '../src/types/report.js';
import type {
  EventLogEntry,
  JobRecord,
  ReportProgressEvidence,
  ReportProgressStage,
  ReportProgressStageKey,
  ReportProgressStageStatus,
  ReportProgressState,
  RunInput,
  ServerEvent,
} from './types.js';

type JobListTypeFilter = 'all' | 'write-hb-k' | 'write-hb-hb' | 'person-intelligence-report' | 'risk-assessment-reports';

interface JobListOptions {
  page?: string | number;
  pageSize?: string | number;
  type?: string;
  q?: string;
}

interface DatabaseSourceItem {
  title: string;
  url: string;
  summary: string;
  websiteName: string;
  publishTime: string;
}

interface DatabaseQueryPlanSummary {
  tablesDiscovered: number;
  tablesChecked: number;
  strictHits: number;
  expandedHits: number;
  returnedSources: number;
  broadeningApplied: boolean;
  contentRowsRead: number;
}

interface VectorQueryPlanSummary {
  enabled: boolean;
  available: boolean;
  storageMode: string;
  embeddingModel: string;
  activeTable: string;
  indexedRows: number;
  vectorHits: number;
  keywordBoostedHits: number;
  returnedSources: number;
  broadeningApplied: boolean;
  lastIndexedAt: string | null;
  fallbackReason: string;
}

interface DatabaseSourcesResponse {
  status: 'hit' | 'empty' | 'fallback' | 'unavailable';
  sources: DatabaseSourceItem[];
  fallbackReason: string;
  totalHits: number;
  updatedAt: string | null;
  queryPlan: DatabaseQueryPlanSummary;
  retrievalMode?: 'keyword' | 'vector' | 'hybrid';
  vectorPlan?: VectorQueryPlanSummary;
}

type ReportSourceListType = 'all' | 'database_recall' | 'tool_search' | 'report_refs' | 'structured_sources' | 'candidate_hits' | 'extract_failed';
type ReportSourceOrigin = 'database_recall' | 'tool_search';
type ReportEvidenceKind = 'report_reference' | 'structured_source' | 'research_source' | 'evidence_card';
type ReportSourceEngine = 'exa' | 'firecrawl' | 'tavily' | 'tavily_extract' | 'pg_vector' | 'database';

const PROGRESS_STAGE_DEFS: Array<Omit<ReportProgressStage, 'status' | 'evidence'>> = [
  { key: 'prepare', title: '任务准备', desc: '整理编报要求并建立任务空间' },
  { key: 'source', title: '信源筛选', desc: '检索并筛选与主题相关的可信信源' },
  { key: 'plan', title: '调研规划', desc: '拆解调研方向并安排采集任务' },
  { key: 'research', title: '资料采集', desc: '采集公开资料并提取关键事实' },
  { key: 'consolidate', title: '素材整合', desc: '汇总信源、证据和分析要点' },
  { key: 'report', title: '报告生成', desc: '生成报告正文并完成校验' },
];

interface ReportSourcesOptions {
  type?: string;
  page?: string | number;
  pageSize?: string | number;
}

interface ReportSourceListItem {
  id: string;
  sourceGroup: Exclude<ReportSourceListType, 'all'>;
  sourceOrigin?: ReportSourceOrigin;
  evidenceKind?: ReportEvidenceKind;
  engine?: ReportSourceEngine;
  citationNo?: number;
  title: string;
  url?: string;
  sourceName?: string;
  publishTime?: string;
  summary?: string;
  excerpt?: string;
  sourceType?: string;
  relevanceScore?: number;
  status?: string;
  method?: string;
  failedReason?: string;
  rawReferenceText?: string;
  matchStatus?: 'matched' | 'raw_only' | 'failed';
  candidateStage?: string;
  hitType?: string;
}

interface ReportSourcesResponse {
  items: ReportSourceListItem[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
  hasMore: boolean;
  meta?: Record<string, unknown>;
}

interface ReportSourceSummary {
  databaseRecallCount: number;
  toolSearchCount: number;
  reportReferenceCount: number;
  structuredSourceCount: number;
}

@Injectable()
export class ReportsService {
  private readonly jobs = new Map<string, JobRecord>();
  private readonly streams = new Map<string, Subject<ServerEvent>>();
  private dailySequence = new Map<string, number>();

  constructor(
    private readonly openClaw: OpenClawService,
    private readonly remoteFs: RemoteFileService,
    private readonly vectorSources: VectorSourceService,
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
    job.progressState = this.buildInitialProgressState(job);

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

  async getJobWithRecoveredReport(jobId: string): Promise<JobRecord | undefined> {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    await this.recoverJobFromExistingReport(job, 'detail_lookup');
    return job;
  }

  getStream(jobId: string): Subject<ServerEvent> | undefined {
    return this.streams.get(jobId);
  }

  getEventLog(jobId: string): { jobId: string; items: EventLogEntry[] } | undefined {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    return { jobId, items: job.eventLog ?? [] };
  }

  async getProgressState(jobId: string): Promise<ReportProgressState | undefined> {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;
    await this.refreshProgressState(job);
    return job.progressState;
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
      progressState: job.progressState,
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

  private buildInitialProgressState(job: JobRecord): ReportProgressState {
    const now = new Date().toISOString();
    return {
      jobId: job.jobId,
      currentStage: job.status === 'queued' || job.status === 'running' ? 'prepare' : null,
      updatedAt: now,
      stages: PROGRESS_STAGE_DEFS.map((stage, index) => ({
        ...stage,
        status: index === 0 && (job.status === 'queued' || job.status === 'running') ? 'running' : 'not_started',
        evidence: index === 0
          ? [{ source: 'job_status', message: `任务状态：${job.status}`, time: now }]
          : [],
      })),
    };
  }

  private async refreshProgressState(job: JobRecord, emit = false): Promise<ReportProgressState> {
    const state = await this.computeProgressState(job);
    job.progressState = state;
    if (emit) this.emitProgressState(job, state);
    else void this.writeJobState(job);
    return state;
  }

  private emitProgressState(job: JobRecord, progressState: ReportProgressState): void {
    job.events = job.events.filter((event) => event.type !== 'progress_state');
    const event: ServerEvent = { type: 'progress_state', progressState };
    job.events.push(event);
    this.streams.get(job.jobId)?.next(event);
    void this.writeJobState(job);
  }

  private async computeProgressState(job: JobRecord): Promise<ReportProgressState> {
    const evidenceByStage = new Map<ReportProgressStageKey, ReportProgressEvidence[]>();
    const statusByStage = new Map<ReportProgressStageKey, ReportProgressStageStatus>();
    const addEvidence = (
      key: ReportProgressStageKey,
      status: ReportProgressStageStatus,
      evidence: ReportProgressEvidence,
    ) => {
      evidenceByStage.set(key, [...(evidenceByStage.get(key) || []), evidence]);
      const current = statusByStage.get(key);
      if (current === 'failed') return;
      if (status === 'failed' || current !== 'done') statusByStage.set(key, status);
    };

    const now = new Date().toISOString();
    if (job.status === 'queued' || job.status === 'running') {
      addEvidence('prepare', 'running', { source: 'job_status', message: `任务状态：${job.status}`, time: job.updatedAt || now });
    }

    for (const entry of job.eventLog || []) {
      const mapped = this.progressStageFromEventLog(entry);
      if (!mapped) continue;
      addEvidence(mapped.key, mapped.status, {
        source: entry.type === 'tool_start' || entry.type === 'tool_end' || entry.type === 'tool_error' ? 'tool_event' : 'event',
        message: entry.summary || entry.label || entry.phase || mapped.key,
        time: entry.time || now,
      });
    }

    const artifactEvidence = await this.collectTrustedProgressArtifactEvidence(job);
    for (const item of artifactEvidence) addEvidence(item.key, item.status, item.evidence);

    if (job.status === 'succeeded') {
      for (const stage of PROGRESS_STAGE_DEFS) {
        addEvidence(stage.key, 'done', {
          source: stage.key === 'report' ? 'report_file' : 'job_status',
          message: stage.key === 'report' ? '最终报告已确认生成。' : '任务已成功完成。',
          time: job.updatedAt || now,
        });
      }
    }

    if (job.status === 'failed' || job.status === 'cancelled' || job.status === 'waiting_approval') {
      const failedKey = this.lastStartedProgressStage(statusByStage) || 'prepare';
      addEvidence(failedKey, 'failed', {
        source: 'job_status',
        message: job.errorMessage || `任务状态：${job.status}`,
        time: job.updatedAt || now,
      });
    }

    const stages = this.normalizeProgressStageOrder(PROGRESS_STAGE_DEFS.map((stage) => ({
      ...stage,
      status: statusByStage.get(stage.key) || 'not_started',
      evidence: evidenceByStage.get(stage.key) || [],
    })));
    const currentStage = this.currentProgressStage(stages);
    return {
      jobId: job.jobId,
      currentStage,
      updatedAt: now,
      stages,
    };
  }

  private progressStageFromEventLog(entry: EventLogEntry): { key: ReportProgressStageKey; status: ReportProgressStageStatus } | null {
    const haystack = `${entry.phase || ''} ${entry.label || ''} ${entry.summary || ''} ${entry.command || ''} ${entry.detail || ''}`.toLowerCase();
    const status: ReportProgressStageStatus =
      entry.type === 'tool_error' || entry.status === 'failed'
        ? 'failed'
        : entry.type === 'tool_end' || entry.status === 'completed' || /已完成|完成/.test(entry.summary || '')
          ? 'done'
          : 'running';

    if (/waiting_final_report|gateway_fallback|openclaw:|^start$|^running$|received/.test(entry.phase || '')) return null;
    if (/context_preparing|context\.json|preparing openclaw/.test(haystack)) return { key: 'prepare', status };
    if (/pg向量|pg-sources|pg_sources|vector_sources|database_sources|database_query_plan|数据库信源|信源检索/.test(haystack)) return { key: 'source', status };
    if (/research_planning|harness_cli\.py\s+plan|plan\.json|调研计划/.test(haystack)) return { key: 'plan', status };
    if (/research_dispatch|research_waiting|research_collecting|harness_cli\.py\s+run|research_|sessions_spawn|sessions_yield|资料|调研子任务/.test(haystack)) return { key: 'research', status };
    if (/research_consolidating|consolidated\.json|素材整合|证据包/.test(haystack)) return { key: 'consolidate', status };
    if (/synthesis|report_saving|report_verifying|final\/report\.md|报告文件|撰写|校验报告/.test(haystack)) {
      return { key: 'report', status: status === 'failed' ? 'failed' : 'running' };
    }
    return null;
  }

  private async collectProgressArtifactEvidence(job: JobRecord): Promise<Array<{ key: ReportProgressStageKey; evidence: ReportProgressEvidence }>> {
    const reportDir = this.remoteFs.remoteDir;
    const cachedJobDir = typeof job.artifacts?.openClawJobDir === 'string' ? job.artifacts.openClawJobDir : '';
    const resolvedJobDir = cachedJobDir || await this.resolveOpenClawJobDir(job);
    if (resolvedJobDir && resolvedJobDir !== cachedJobDir) {
      job.artifacts = { ...job.artifacts, openClawJobDir: resolvedJobDir };
    }
    const jobDir = resolvedJobDir || this.remoteFs.joinPath(reportDir, job.jobId);
    const now = new Date().toISOString();
    const result: Array<{ key: ReportProgressStageKey; evidence: ReportProgressEvidence }> = [];
    const addIfExists = async (key: ReportProgressStageKey, filePath: string, message: string) => {
      try {
        if (await this.remoteFs.exists(filePath)) {
          result.push({ key, evidence: { source: key === 'report' ? 'report_file' : 'artifact', message, time: now } });
        }
      } catch {
        // Missing artifacts are expected while a job is running.
      }
    };

    await addIfExists('prepare', this.remoteFs.joinPath(jobDir, 'context.json'), '任务上下文文件已生成。');
    await addIfExists('source', this.remoteFs.joinPath(jobDir, 'database', 'database_sources.json'), '数据库信源文件已生成。');
    await addIfExists('source', this.remoteFs.joinPath(jobDir, 'database', 'vector_sources.json'), '向量信源文件已生成。');
    await addIfExists('source', this.remoteFs.joinPath(jobDir, 'database', 'database_query_plan.json'), '信源查询计划已生成。');
    await addIfExists('plan', this.remoteFs.joinPath(jobDir, 'plan.json'), '调研计划文件已生成。');
    try {
      const groupEntries = await this.remoteFs.readdir(this.remoteFs.joinPath(jobDir, 'groups'));
      if (groupEntries.some((entry) => entry.isFile && /^group_[a-z0-9_-]+\.json$/i.test(entry.name))) {
        result.push({ key: 'plan', evidence: { source: 'artifact', message: '调研分组文件已生成。', time: now } });
      }
    } catch {
      // Group files are created later in the workflow.
    }
    try {
      const researchEntries = await this.remoteFs.readdir(this.remoteFs.joinPath(jobDir, 'research'));
      if (researchEntries.some((entry) => entry.isFile && /^research_[a-z0-9_-]+\.json$/i.test(entry.name))) {
        result.push({ key: 'research', evidence: { source: 'artifact', message: '调研结果文件已生成。', time: now } });
      }
    } catch {
      // Research files are created later in the workflow.
    }
    await addIfExists('consolidate', this.remoteFs.joinPath(jobDir, 'research', 'consolidated.json'), '综合素材文件已生成。');
    await addIfExists('report', this.remoteFs.joinPath(jobDir, 'final', 'report.md'), '最终报告文件已生成。');
    if (job.resultPath) await addIfExists('report', job.resultPath, '最终报告文件已登记。');
    return result;
  }

  private async collectTrustedProgressArtifactEvidence(job: JobRecord): Promise<Array<{ key: ReportProgressStageKey; status: ReportProgressStageStatus; evidence: ReportProgressEvidence }>> {
    const cachedJobDir = typeof job.artifacts?.openClawJobDir === 'string' ? job.artifacts.openClawJobDir : '';
    if (cachedJobDir && !this.openClawJobDirMatchesJob(cachedJobDir, job.jobId)) {
      const { openClawJobDir, ...restArtifacts } = job.artifacts || {};
      job.artifacts = restArtifacts;
    }

    const jobDir = await this.resolveOpenClawJobDir(job) || this.remoteFs.joinPath(this.remoteFs.remoteDir, job.jobId);
    const now = new Date().toISOString();
    const result: Array<{ key: ReportProgressStageKey; status: ReportProgressStageStatus; evidence: ReportProgressEvidence }> = [];
    const addIfExists = async (
      key: ReportProgressStageKey,
      filePath: string,
      message: string,
      status: ReportProgressStageStatus = 'done',
    ) => {
      try {
        if (await this.remoteFs.exists(filePath)) {
          result.push({ key, status, evidence: { source: key === 'report' ? 'report_file' : 'artifact', message, time: now } });
        }
      } catch {
        // Missing artifacts are expected while a job is running.
      }
    };

    await addIfExists('prepare', this.remoteFs.joinPath(jobDir, 'context.json'), '任务上下文文件已生成。');
    await addIfExists('source', this.remoteFs.joinPath(jobDir, 'database', 'database_sources.json'), '数据库信源文件已生成。');
    await addIfExists('source', this.remoteFs.joinPath(jobDir, 'database', 'vector_sources.json'), '向量信源文件已生成。');
    await addIfExists('source', this.remoteFs.joinPath(jobDir, 'database', 'database_query_plan.json'), '信源查询计划已生成。');
    await addIfExists('plan', this.remoteFs.joinPath(jobDir, 'plan.json'), '调研计划文件已生成。');
    try {
      const groupEntries = await this.remoteFs.readdir(this.remoteFs.joinPath(jobDir, 'groups'));
      if (groupEntries.some((entry) => entry.isFile && /^group_[a-z0-9_-]+\.json$/i.test(entry.name))) {
        result.push({ key: 'plan', status: 'done', evidence: { source: 'artifact', message: '调研分组文件已生成。', time: now } });
      }
    } catch {
      // Group files are created later in the workflow.
    }
    try {
      const researchEntries = await this.remoteFs.readdir(this.remoteFs.joinPath(jobDir, 'research'));
      if (researchEntries.some((entry) => entry.isFile && /^research_[a-z0-9_-]+\.json$/i.test(entry.name))) {
        result.push({ key: 'research', status: 'done', evidence: { source: 'artifact', message: '调研结果文件已生成。', time: now } });
      }
    } catch {
      // Research files are created later in the workflow.
    }
    await addIfExists('consolidate', this.remoteFs.joinPath(jobDir, 'research', 'consolidated.json'), '综合素材文件已生成。');
    await addIfExists(
      'report',
      this.remoteFs.joinPath(jobDir, 'final', 'report.md'),
      '最终报告文件已生成。',
      job.status === 'succeeded' || job.resultPath ? 'done' : 'running',
    );
    if (job.resultPath) await addIfExists('report', job.resultPath, '最终报告文件已登记。');
    return result;
  }

  private lastStartedProgressStage(statusByStage: Map<ReportProgressStageKey, ReportProgressStageStatus>): ReportProgressStageKey | null {
    let result: ReportProgressStageKey | null = null;
    for (const stage of PROGRESS_STAGE_DEFS) {
      if (statusByStage.has(stage.key)) result = stage.key;
    }
    return result;
  }

  private normalizeProgressStageOrder(stages: ReportProgressStage[]): ReportProgressStage[] {
    const failedIndex = stages.findIndex((stage) => stage.status === 'failed');
    const observableLimit = failedIndex >= 0
      ? failedIndex
      : stages.reduce((last, stage, index) => stage.status !== 'not_started' ? index : last, -1);
    if (observableLimit <= 0) return stages;

    const now = new Date().toISOString();
    return stages.map((stage, index) => {
      if (index >= observableLimit || stage.status === 'failed' || stage.status === 'done') return stage;
      return {
        ...stage,
        status: 'done',
        evidence: [
          ...stage.evidence,
          {
            source: 'artifact',
            message: `已观察到后续阶段“${stages[observableLimit].title}”的真实执行证据。`,
            time: now,
          },
        ],
      };
    });
  }

  private currentProgressStage(stages: ReportProgressStage[]): ReportProgressStageKey | null {
    const failed = stages.find((stage) => stage.status === 'failed');
    if (failed) return failed.key;
    const running = stages.find((stage) => stage.status === 'running');
    if (running) return running.key;
    for (let index = stages.length - 1; index >= 0; index -= 1) {
      if (stages[index].status === 'done') return stages[index].key;
    }
    return null;
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
    await this.recoverJobFromExistingReport(job, 'result_lookup');
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
    await this.recoverJobFromExistingReport(job, 'download_lookup');
    if (job.status !== 'succeeded' || !job.resultPath) return null;

    const markdown = await this.remoteFs.readFile(job.resultPath);
    return { markdown, artifacts: job.artifacts, resultPath: job.resultPath };
  }

  async getDatabaseSources(jobId: string): Promise<DatabaseSourcesResponse | undefined> {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;

    const dir = await this.resolveOpenClawJobDir(job);
    if (!dir) {
      const vectorResult = this.vectorResultFromJob(job);
      const vectorSources = this.normalizeVectorSources(vectorResult?.sources || []).slice(0, 50);
      if (vectorSources.length) {
        return {
          status: 'hit',
          sources: vectorSources,
          fallbackReason: '',
          totalHits: Math.max(vectorResult?.totalHits || 0, vectorSources.length),
          updatedAt: vectorResult?.updatedAt || null,
          queryPlan: this.emptyDatabaseQueryPlanSummary(),
          retrievalMode: 'vector',
          vectorPlan: this.buildVectorQueryPlanSummary(vectorResult),
        };
      }
      return {
        status: 'unavailable',
        sources: [],
        fallbackReason: '',
        totalHits: 0,
        updatedAt: null,
        queryPlan: this.emptyDatabaseQueryPlanSummary(),
        retrievalMode: 'keyword',
        vectorPlan: this.buildVectorQueryPlanSummary(vectorResult),
      };
    }

    const planPath = this.remoteFs.joinPath(dir, 'database', 'database_query_plan.json');
    const sourcesPath = this.remoteFs.joinPath(dir, 'database', 'database_sources.json');
    const plan = await this.readJsonFile(planPath);
    const planObject = plan && !Array.isArray(plan) ? plan : null;
    const sourcesRaw = await this.readJsonFile(sourcesPath);
    const sourcesList = Array.isArray(sourcesRaw) ? sourcesRaw : [];
    const vectorResult = this.vectorResultFromJob(job);
    const vectorSources = this.normalizeVectorSources(vectorResult?.sources || []);
    const sources = this.mergeDatabaseSources(vectorSources, this.normalizeDatabaseSources(sourcesList)).slice(0, 50);
    const queryPlan = this.buildDatabaseQueryPlanSummary(planObject, sources.length);
    const vectorPlan = this.buildVectorQueryPlanSummary(vectorResult);
    const fallbackReason = this.sanitizeLogText(
      this.firstString(planObject, ['database_source_fallback_reason', 'fallbackReason', 'fallback_reason']) ||
        vectorPlan.fallbackReason,
      300,
    );

    let updatedAt: string | null = null;
    try {
      const sourceStat = await this.remoteFs.stat(sourcesPath);
      updatedAt = Number.isFinite(sourceStat.mtimeMs) ? new Date(sourceStat.mtimeMs).toISOString() : null;
    } catch {
      try {
        const planStat = await this.remoteFs.stat(planPath);
        updatedAt = Number.isFinite(planStat.mtimeMs) ? new Date(planStat.mtimeMs).toISOString() : null;
      } catch {
        updatedAt = null;
      }
    }

    const planTotalHits = this.firstNumber(planObject, ['total_hits', 'totalHits', 'relevant_hits']) || 0;
    const vectorTotalHits = vectorResult?.totalHits || 0;
    const totalHits = Math.max(planTotalHits + vectorTotalHits, sources.length);
    const status: DatabaseSourcesResponse['status'] = sources.length
      ? 'hit'
      : fallbackReason
        ? 'fallback'
        : plan
          ? 'empty'
          : 'unavailable';

    const retrievalMode = vectorSources.length && sourcesList.length
      ? 'hybrid'
      : vectorSources.length
        ? 'vector'
        : 'keyword';

    return { status, sources, fallbackReason, totalHits, updatedAt, queryPlan, retrievalMode, vectorPlan };
  }

  async getSources(jobId: string, options: ReportSourcesOptions = {}): Promise<ReportSourcesResponse | undefined> {
    const job = this.jobs.get(jobId);
    if (!job) return undefined;

    const type = this.normalizeReportSourceType(options.type);
    const page = this.parsePositiveInt(options.page, 1);
    const pageSize = Math.min(this.parsePositiveInt(options.pageSize, 10), 100);

    const [reportRefs, structuredSources, toolSearchSources, candidateResult, extractFailed] = await Promise.all([
      this.reportReferenceSources(job),
      this.structuredReportSources(job),
      this.toolSearchSources(job),
      type === 'candidate_hits' ? this.candidateHitSources(job) : Promise.resolve({ items: [], total: 0, detailSaved: false }),
      type === 'extract_failed' ? this.extractFailedSources(job) : Promise.resolve([]),
    ]);

    const databaseRecall = this.databaseRecallChannelSources(structuredSources, reportRefs);
    const toolSearch = this.toolSearchChannelSources(toolSearchSources, reportRefs, databaseRecall);
    const summary: ReportSourceSummary = {
      databaseRecallCount: databaseRecall.length,
      toolSearchCount: toolSearch.length,
      reportReferenceCount: reportRefs.length,
      structuredSourceCount: structuredSources.length,
    };

    const groups: Record<Exclude<ReportSourceListType, 'all'>, ReportSourceListItem[]> = {
      database_recall: databaseRecall,
      tool_search: toolSearch,
      report_refs: reportRefs,
      structured_sources: structuredSources,
      candidate_hits: candidateResult.items,
      extract_failed: extractFailed,
    };
    const allItems = type === 'all' ? [...databaseRecall, ...toolSearch] : groups[type] || [];
    const total = type === 'candidate_hits'
      ? (candidateResult.total || allItems.length)
      : allItems.length;
    const start = (page - 1) * pageSize;
    const items = allItems.slice(start, start + pageSize);

    return {
      items,
      total,
      page,
      pageSize,
      totalPages: Math.max(1, Math.ceil(total / pageSize)),
      hasMore: start + items.length < total && allItems.length > start + items.length,
      meta: {
        summary,
        ...(type === 'candidate_hits'
          ? {
            detailSaved: candidateResult.detailSaved,
            message: candidateResult.detailSaved
              ? ''
              : `候选池共 ${total} 条，当前历史任务未保存候选明细。`,
          }
          : {}),
      },
    };
  }

  private async enrichPayloadWithVectorSources(job: JobRecord): Promise<Record<string, unknown>> {
    const payload = { ...(job.payload as unknown as Record<string, unknown>) };
    if (job.skill !== 'write-hb') return payload;

    const knownContext = typeof payload.known_context === 'string' ? payload.known_context : '';
    const parsed = this.parseJsonObject(knownContext) || {};
    const databaseOptions = parsed.databaseSourceOptions && typeof parsed.databaseSourceOptions === 'object' && !Array.isArray(parsed.databaseSourceOptions)
      ? parsed.databaseSourceOptions as Record<string, unknown>
      : {};
    const databaseEnabled = databaseOptions.enabled === true || String(databaseOptions.enabled).toLowerCase() === 'true';
    if (!databaseEnabled) return payload;

    const maxRows = this.boundInt(databaseOptions.maxMetadataRows, 50, 1, 100);
    const lookbackDays = this.boundInt(databaseOptions.lookbackDays, 30, 0, 365);
    const result = await this.vectorSources.search({
      topic: String(payload.topic || parsed.topic || ''),
      knownContext: parsed,
      maxRows,
      lookbackDays,
    });

    job.artifacts = {
      ...job.artifacts,
      vectorDatabaseSources: result.sources,
      vectorDatabaseQueryPlan: result.queryPlan,
      vectorDatabaseSourceStatus: result.status,
    };
    await this.writeJobState(job);

    const enrichedContext = {
      ...parsed,
      vectorDatabaseSourceOptions: {
        enabled: true,
        provider: 'postgres_pgvector',
        mode: 'semantic_summary',
        lookbackDays,
        maxMetadataRows: maxRows,
      },
      vectorDatabaseSources: result.sources,
      vectorDatabaseQueryPlan: result.queryPlan,
    };
    payload.known_context = JSON.stringify(enrichedContext, null, 2);
    return payload;
  }

  private parseJsonObject(text: string): Record<string, unknown> | null {
    if (!text.trim()) return null;
    try {
      const parsed = JSON.parse(text) as unknown;
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null;
    } catch {
      return null;
    }
  }

  private boundInt(raw: unknown, fallback: number, min: number, max: number): number {
    const parsed = typeof raw === 'number' ? raw : Number(String(raw ?? ''));
    if (!Number.isFinite(parsed)) return fallback;
    return Math.max(min, Math.min(max, Math.floor(parsed)));
  }

  private async runJob(job: JobRecord) {
    job.status = 'running';
    job.updatedAt = new Date().toISOString();
    await this.writeJobState(job);
    const startedAtMs = Date.now();

    try {
      const requestUser = this.buildRequestUser(job);
      const enrichedPayload = await this.enrichPayloadWithVectorSources(job);
      const runInput: RunInput = {
        skill: job.skill,
        payload: enrichedPayload,
        requestUser,
        onEvent: (event) => this.pushEvent(job, event),
        jobId: job.jobId,
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

      let resolvedReport = recoveredReport ?? (await this.resolveOpenClawReportFile(result.markdown, startedAtMs, job.jobId));
      const finalMarkdown = resolvedReport?.markdown ?? result.markdown;
      if (!resolvedReport && /^\s*REPORT_FILE\s*:/im.test(finalMarkdown)) {
        throw new Error('OpenClaw returned a REPORT_FILE pointer, but no valid Markdown report file was found.');
      }
      try {
        this.assertUsableGeneratedMarkdown(finalMarkdown);
      } catch (validationError) {
        const lateReport = await this.resolveOpenClawReportFile('', startedAtMs, job.jobId, 150_000);
        if (!lateReport) throw validationError;
        this.pushEvent(job, {
          type: 'stage',
          stage: 'report_file_recovered',
          message: `Recovered generated report file after validation fallback: ${lateReport.filePath}`,
        });
        resolvedReport = lateReport;
      }
      const usableMarkdown = resolvedReport?.markdown ?? finalMarkdown;
      job.status = 'succeeded';
      job.markdown = usableMarkdown;
      job.artifacts = { ...job.artifacts, ...result.artifacts };
      job.resultPath = resolvedReport?.filePath ?? (await this.writeReportFile(job, job.markdown));
      await this.writeReportReferencesArtifact(job, usableMarkdown);
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
      const recovered = await this.recoverJobFromExistingReport(job, 'failure_handler');
      if (recovered) {
        this.pushEvent(job, { type: 'done', jobId: job.jobId });
        this.streams.get(job.jobId)?.complete();
        return;
      }

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
    if (event.type === 'progress_state') {
      this.emitProgressState(job, event.progressState);
      return;
    }
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
    void this.refreshProgressState(job, true);
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

  private firstString(data: Record<string, unknown> | null, keys: string[]): string {
    if (!data) return '';
    for (const key of keys) {
      const value = data[key];
      if (typeof value === 'string' && value.trim()) return value.trim();
    }
    return '';
  }

  private firstNumber(data: Record<string, unknown> | null, keys: string[]): number | undefined {
    if (!data) return undefined;
    for (const key of keys) {
      const value = data[key];
      if (typeof value === 'number' && Number.isFinite(value)) return value;
      if (typeof value === 'string') {
        const parsed = Number(value);
        if (Number.isFinite(parsed)) return parsed;
      }
    }
    return undefined;
  }

  private firstBoolean(data: Record<string, unknown> | null, keys: string[]): boolean | undefined {
    if (!data) return undefined;
    for (const key of keys) {
      const value = data[key];
      if (typeof value === 'boolean') return value;
      if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (normalized === 'true') return true;
        if (normalized === 'false') return false;
      }
    }
    return undefined;
  }

  private arrayLength(data: Record<string, unknown> | null, keys: string[]): number | undefined {
    if (!data) return undefined;
    for (const key of keys) {
      const value = data[key];
      if (Array.isArray(value)) return value.length;
    }
    return undefined;
  }

  private nonNegativeInt(value: number | undefined): number {
    if (value === undefined || !Number.isFinite(value)) return 0;
    return Math.max(0, Math.trunc(value));
  }

  private emptyDatabaseQueryPlanSummary(): DatabaseQueryPlanSummary {
    return {
      tablesDiscovered: 0,
      tablesChecked: 0,
      strictHits: 0,
      expandedHits: 0,
      returnedSources: 0,
      broadeningApplied: false,
      contentRowsRead: 0,
    };
  }

  private buildDatabaseQueryPlanSummary(plan: Record<string, unknown> | null, sourceCount: number): DatabaseQueryPlanSummary {
    const tablesDiscovered =
      this.firstNumber(plan, ['tables_discovered_count', 'tablesDiscoveredCount', 'discovered_tables_count']) ??
      this.arrayLength(plan, ['tables_discovered', 'tablesDiscovered', 'discovered_tables']);
    const tablesChecked =
      this.firstNumber(plan, ['tables_checked_count', 'tablesCheckedCount', 'checked_tables_count']) ??
      this.arrayLength(plan, ['tables_checked', 'tablesChecked', 'checked_tables']);
    const strictHits = this.firstNumber(plan, ['strict_hits', 'strictHits']);
    const expandedHits = this.firstNumber(plan, ['expanded_hits', 'expandedHits']);
    const returnedSources = this.firstNumber(plan, ['returned_sources', 'returnedSources']);
    const broadeningApplied = this.firstBoolean(plan, ['broadening_applied', 'broadeningApplied']) ?? this.nonNegativeInt(expandedHits) > 0;
    const contentRowsRead = this.firstNumber(plan, ['content_rows_read', 'contentRowsRead']);

    return {
      tablesDiscovered: this.nonNegativeInt(tablesDiscovered),
      tablesChecked: this.nonNegativeInt(tablesChecked),
      strictHits: this.nonNegativeInt(strictHits),
      expandedHits: this.nonNegativeInt(expandedHits),
      returnedSources: this.nonNegativeInt(returnedSources ?? sourceCount),
      broadeningApplied,
      contentRowsRead: this.nonNegativeInt(contentRowsRead),
    };
  }

  private buildVectorQueryPlanSummary(result: VectorSearchResult | null): VectorQueryPlanSummary {
    const plan = result?.queryPlan;
    return {
      enabled: Boolean(plan?.enabled),
      available: Boolean(plan?.available),
      storageMode: this.sanitizeLogText(String(plan?.storageMode || ''), 80),
      embeddingModel: this.sanitizeLogText(String(plan?.embeddingModel || ''), 80),
      activeTable: this.sanitizeLogText(String(plan?.activeTable || plan?.sourceTable || ''), 120),
      indexedRows: this.nonNegativeInt(Number(plan?.indexedRows || 0)),
      vectorHits: this.nonNegativeInt(Number(plan?.vectorHits || 0)),
      keywordBoostedHits: this.nonNegativeInt(Number(plan?.keywordBoostedHits || 0)),
      returnedSources: this.nonNegativeInt(Number(plan?.returnedSources || result?.sources.length || 0)),
      broadeningApplied: Boolean(plan?.broadeningApplied),
      lastIndexedAt: plan?.lastIndexedAt || null,
      fallbackReason: this.sanitizeLogText(String(plan?.fallbackReason || ''), 300),
    };
  }

  private vectorResultFromJob(job: JobRecord): VectorSearchResult | null {
    const sources = Array.isArray(job.artifacts?.vectorDatabaseSources)
      ? job.artifacts.vectorDatabaseSources as VectorSourceItem[]
      : [];
    const rawPlan = job.artifacts?.vectorDatabaseQueryPlan && typeof job.artifacts.vectorDatabaseQueryPlan === 'object'
      ? job.artifacts.vectorDatabaseQueryPlan as VectorSearchResult['queryPlan']
      : null;
    const status = String(job.artifacts?.vectorDatabaseSourceStatus || (sources.length ? 'hit' : rawPlan?.fallbackReason ? 'fallback' : 'unavailable'));
    if (!sources.length && !rawPlan) return null;
    return {
      status: ['hit', 'empty', 'fallback', 'unavailable'].includes(status) ? status as VectorSearchResult['status'] : 'unavailable',
      sources,
      totalHits: Math.max(Number(rawPlan?.vectorHits || 0), sources.length),
      queryPlan: rawPlan || {
        enabled: false,
        available: false,
        storageMode: 'unavailable',
        embeddingModel: '',
        indexTable: '',
        activeTable: '',
        sourceTable: '',
        embeddingColumnType: '',
        pgvectorAvailable: false,
        indexedRows: 0,
        vectorHits: 0,
        keywordBoostedHits: 0,
        returnedSources: sources.length,
        broadeningApplied: false,
        lastIndexedAt: null,
        fallbackReason: '',
      },
      updatedAt: rawPlan?.lastIndexedAt || null,
    };
  }

  private normalizeVectorSources(items: VectorSourceItem[]): DatabaseSourceItem[] {
    return items
      .map((item) => ({
        title: this.sanitizeLogText(item.title || '', 200),
        url: this.sanitizeLogText(item.url || '', 500),
        summary: this.sanitizeLogText(item.summary || '', 1000),
        websiteName: this.sanitizeLogText(item.websiteName || '', 120),
        publishTime: this.sanitizeLogText(item.publishTime || '', 60),
      }))
      .filter((item) => item.title || item.url);
  }

  private mergeDatabaseSources(primary: DatabaseSourceItem[], secondary: DatabaseSourceItem[]): DatabaseSourceItem[] {
    const seen = new Set<string>();
    const merged: DatabaseSourceItem[] = [];
    for (const item of [...primary, ...secondary]) {
      const key = item.url || `${item.title}|${item.summary}`;
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push(item);
    }
    return merged;
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
                progressState: parsed.progressState,
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

  private async resolveOpenClawReportFile(markdown: string, startedAtMs: number, jobId?: string, waitMs = 0) {
    const deadline = Date.now() + Math.max(0, waitMs);
    do {
      const found = await this.resolveOpenClawReportFileOnce(markdown, startedAtMs, jobId);
      if (found) return found;
      if (Date.now() >= deadline) break;
      await this.sleep(5_000);
    } while (Date.now() < deadline);

    return null;
  }

  private async resolveOpenClawReportFileOnce(markdown: string, startedAtMs: number, jobId?: string) {
    const fromText = await this.readMarkdownFile(this.extractReportPath(markdown));
    if (fromText) return fromText;

    if (jobId) {
      const fromJobDir = await this.findMarkdownFileInJobDir(jobId);
      if (fromJobDir) return fromJobDir;
    }

    const latest = await this.findLatestMarkdownFile(startedAtMs);
    if (latest) return latest;

    return null;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
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

  private async findMarkdownFileInJobDir(jobId: string) {
    const reportDir = this.remoteFs.remoteDir;
    const jobDir = this.remoteFs.joinPath(reportDir, jobId);
    const priorityPaths = [
      this.remoteFs.joinPath(jobDir, 'final', 'report.md'),
      this.remoteFs.joinPath(jobDir, 'report.md'),
    ];

    for (const filePath of priorityPaths) {
      const report = await this.readMarkdownFile(filePath);
      if (report) return report;
    }

    try {
      const finalDir = this.remoteFs.joinPath(jobDir, 'final');
      const entries = await this.remoteFs.readdir(finalDir);
      const files: { filePath: string; stat: { size: number; mtimeMs: number } }[] = [];
      for (const entry of entries) {
        if (!entry.isFile || !entry.name.toLowerCase().endsWith('.md')) continue;
        const filePath = this.remoteFs.joinPath(finalDir, entry.name);
        try {
          files.push({ filePath, stat: await this.remoteFs.stat(filePath) });
        } catch { continue; }
      }
      files.sort((a, b) => b.stat.size - a.stat.size || b.stat.mtimeMs - a.stat.mtimeMs);
      for (const candidate of files) {
        const report = await this.readMarkdownFile(candidate.filePath);
        if (report) return report;
      }
    } catch {
      return null;
    }

    return null;
  }

  private async recoverJobFromExistingReport(job: JobRecord, reason: string): Promise<boolean> {
    if (job.status === 'succeeded' && job.resultPath && job.markdown) return false;

    const report = await this.findMarkdownFileInJobDir(job.jobId) ?? await this.findBestMarkdownFileForJob(job);
    if (!report) return false;

    job.status = 'succeeded';
    job.stage = 'done';
    job.markdown = report.markdown;
    job.resultPath = report.filePath;
    job.errorMessage = undefined;
    await this.writeReportReferencesArtifact(job, report.markdown);
    job.updatedAt = new Date().toISOString();

    this.pushEvent(job, {
      type: 'stage',
      stage: 'report_file_recovered',
      message: `Recovered generated report file during ${reason}: ${report.filePath}`,
    });
    this.pushEvent(job, { type: 'stage', stage: 'done', message: 'Report generation completed and saved to disk.' });
    await this.writeJobState(job);
    return true;
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

  private async readJsonFile(filePath: string): Promise<Record<string, unknown> | unknown[] | null> {
    try {
      const raw = await this.remoteFs.readFile(filePath);
      const parsed = JSON.parse(raw) as unknown;
      if (!parsed || typeof parsed !== 'object') return null;
      return parsed as Record<string, unknown> | unknown[];
    } catch {
      return null;
    }
  }

  private normalizeDatabaseSources(items: unknown[]): DatabaseSourceItem[] {
    const result: DatabaseSourceItem[] = [];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      const source = item as Record<string, unknown>;
      const title = this.sanitizeLogText(
        this.firstString(source, ['ch_title', 'title', 'entitle']),
        200,
      );
      const url = this.sanitizeLogText(
        this.firstString(source, ['data_source_url', 'url']),
        500,
      );
      if (!title && !url) continue;
      result.push({
        title,
        url,
        summary: this.sanitizeLogText(this.firstString(source, ['summary']), 1000),
        websiteName: this.sanitizeLogText(this.firstString(source, ['website_name', 'websiteName']), 120),
        publishTime: this.sanitizeLogText(this.firstString(source, ['publish_time', 'publishTime']), 60),
      });
    }
    return result;
  }

  private normalizeReportSourceType(type: unknown): ReportSourceListType {
    const normalized = String(type || '').trim();
    if (
      normalized === 'database_recall' ||
      normalized === 'tool_search' ||
      normalized === 'report_refs' ||
      normalized === 'structured_sources' ||
      normalized === 'candidate_hits' ||
      normalized === 'extract_failed' ||
      normalized === 'all'
    ) {
      return normalized;
    }
    return 'all';
  }

  private databaseRecallChannelSources(
    structuredSources: ReportSourceListItem[],
    reportRefs: ReportSourceListItem[],
  ): ReportSourceListItem[] {
    const databaseItems = structuredSources.map((source) => ({
      ...source,
      sourceGroup: 'database_recall' as const,
      sourceOrigin: 'database_recall' as const,
      evidenceKind: source.evidenceKind || 'structured_source' as const,
      engine: source.engine || this.inferDatabaseEngine(source),
      sourceType: source.sourceType || '数据库记录',
    }));
    const databaseKeys = new Set(databaseItems.map((item) => this.sourceDedupeKey(item)).filter(Boolean));
    const matchingRefs = reportRefs
      .filter((ref) => databaseKeys.has(this.sourceDedupeKey(ref)))
      .map((ref) => ({
        ...ref,
        sourceGroup: 'database_recall' as const,
        sourceOrigin: 'database_recall' as const,
        evidenceKind: 'report_reference' as const,
        engine: this.inferDatabaseEngine(ref),
      }));
    return this.mergeReportSourceItems([...databaseItems, ...matchingRefs], 'database_recall');
  }

  private toolSearchChannelSources(
    researchSources: ReportSourceListItem[],
    reportRefs: ReportSourceListItem[],
    databaseRecall: ReportSourceListItem[],
  ): ReportSourceListItem[] {
    const databaseKeys = new Set(databaseRecall.map((item) => this.sourceDedupeKey(item)).filter(Boolean));
    const researchItems = researchSources.map((source) => ({
      ...source,
      sourceGroup: 'tool_search' as const,
      sourceOrigin: 'tool_search' as const,
      evidenceKind: source.evidenceKind || 'research_source' as const,
      engine: source.engine || this.inferToolSearchEngine(source),
    }));
    const researchKeys = new Set(researchItems.map((item) => this.sourceDedupeKey(item)).filter(Boolean));
    const publicRefs = reportRefs
      .filter((ref) => {
        const key = this.sourceDedupeKey(ref);
        return !key || !databaseKeys.has(key) || researchKeys.has(key);
      })
      .map((ref) => ({
        ...ref,
        sourceGroup: 'tool_search' as const,
        sourceOrigin: 'tool_search' as const,
        evidenceKind: 'report_reference' as const,
        engine: this.inferToolSearchEngine(ref),
      }));
    return this.mergeReportSourceItems([...researchItems, ...publicRefs], 'tool_search');
  }

  private mergeReportSourceItems(
    items: ReportSourceListItem[],
    sourceGroup: 'database_recall' | 'tool_search',
  ): ReportSourceListItem[] {
    const merged = new Map<string, ReportSourceListItem>();
    for (const item of items) {
      const key = this.sourceDedupeKey(item) || `${sourceGroup}:${item.id}`;
      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, { ...item, sourceGroup });
        continue;
      }
      merged.set(key, {
        ...existing,
        citationNo: existing.citationNo ?? item.citationNo,
        title: this.longerText(existing.title, item.title),
        url: existing.url || item.url,
        sourceName: existing.sourceName || item.sourceName,
        publishTime: existing.publishTime || item.publishTime,
        summary: this.longerText(existing.summary, item.summary),
        excerpt: this.longerText(existing.excerpt, item.excerpt),
        sourceType: existing.sourceType || item.sourceType,
        relevanceScore: Math.max(existing.relevanceScore || 0, item.relevanceScore || 0) || undefined,
        status: existing.status || item.status,
        method: existing.method || item.method,
        rawReferenceText: existing.rawReferenceText || item.rawReferenceText,
        matchStatus: existing.matchStatus || item.matchStatus,
        evidenceKind: existing.evidenceKind === 'report_reference' ? item.evidenceKind || existing.evidenceKind : existing.evidenceKind,
        engine: existing.engine || item.engine,
      });
    }
    return Array.from(merged.values()).sort((a, b) => (b.relevanceScore || 0) - (a.relevanceScore || 0));
  }

  private sourceDedupeKey(item: Partial<ReportSourceListItem>): string {
    const url = this.normalizeSourceUrl(item.url);
    if (url) return `url:${url}`;
    const title = String(item.title || '').trim().toLowerCase();
    const sourceName = String(item.sourceName || '').trim().toLowerCase();
    return title ? `title:${title}|${sourceName}` : '';
  }

  private normalizeSourceUrl(value: unknown): string {
    const raw = String(value || '').trim();
    if (!raw) return '';
    try {
      const parsed = new URL(raw);
      parsed.hash = '';
      parsed.searchParams.sort();
      return parsed.toString().replace(/\/$/, '').toLowerCase();
    } catch {
      return raw.replace(/[?#].*$/, '').replace(/\/$/, '').toLowerCase();
    }
  }

  private longerText(a?: string, b?: string): string {
    const left = String(a || '');
    const right = String(b || '');
    return right.length > left.length ? right : left;
  }

  private async reportReferenceSources(job: JobRecord): Promise<ReportSourceListItem[]> {
    const persisted = await this.readReportReferencesArtifact(job);
    if (persisted?.length) return persisted;

    const markdown = await this.reportMarkdown(job);
    if (!markdown) return [];
    const rebuilt = await this.buildReportReferenceItems(job, markdown);
    await this.writeReportReferencesArtifact(job, markdown, rebuilt);
    return rebuilt;
  }

  private async buildReportReferenceItems(job: JobRecord, markdown: string): Promise<ReportSourceListItem[]> {
    const references = this.parseReferenceEntriesRobust(markdown);
    const citationNumbers = this.parseCitationNumbers(markdown);
    const structured = await this.structuredReportSources(job);
    const allNumbers = citationNumbers.length
      ? citationNumbers
      : Array.from(references.keys()).sort((a, b) => a - b);

    return allNumbers.map((number, index) => {
      const reference = references.get(number);
      const fallback = structured[number - 1];
      const rawReferenceText = reference?.rawReferenceText || reference?.summary || reference?.title || '';
      const matched = Boolean(fallback?.title || fallback?.url || fallback?.summary);
      return {
        id: `report-ref-${number}`,
        sourceGroup: 'report_refs',
        sourceOrigin: undefined,
        evidenceKind: 'report_reference',
        citationNo: number,
        title: reference?.title || fallback?.title || `\u53c2\u8003\u7f16\u53f7 [${number}]`,
        url: reference?.url || fallback?.url || '',
        sourceName: reference?.sourceName || fallback?.sourceName || '',
        publishTime: reference?.publishTime || fallback?.publishTime || '',
        summary: reference?.summary || fallback?.summary || rawReferenceText,
        excerpt: `\u6b63\u6587\u5f15\u7528\u7f16\u53f7 [${number}]`,
        sourceType: '\u62a5\u544a\u5f15\u7528',
        relevanceScore: Math.max(100 - index, 1),
        status: 'referenced',
        method: reference ? '\u62a5\u544a\u53c2\u8003\u8d44\u6599\u7d22\u5f15' : matched ? '\u7ed3\u6784\u5316\u4fe1\u6e90\u5339\u914d' : '\u6b63\u6587\u5f15\u7528\u7f16\u53f7',
        rawReferenceText,
        matchStatus: matched ? 'matched' : 'raw_only',
      };
    });
  }

  private reportReferencesArtifactPath(job: JobRecord): string {
    return this.remoteFs.joinPath(this.remoteFs.remoteDir, job.jobId, 'references', 'report_references.json');
  }

  private async reportReferencesArtifactCandidatePaths(job: JobRecord): Promise<string[]> {
    const paths = new Set<string>();
    const knownPath = this.firstString(job.artifacts || {}, ['reportReferencesPath', 'report_references_path']);
    if (knownPath) paths.add(knownPath);
    const dir = await this.resolveOpenClawJobDir(job);
    if (dir) paths.add(this.remoteFs.joinPath(dir, 'references', 'report_references.json'));
    paths.add(this.reportReferencesArtifactPath(job));
    return Array.from(paths);
  }

  private async readReportReferencesArtifact(job: JobRecord): Promise<ReportSourceListItem[] | null> {
    const paths = await this.reportReferencesArtifactCandidatePaths(job);
    for (const filePath of paths) {
      const raw = await this.readJsonFile(filePath);
      if (!raw) continue;
      const items = Array.isArray(raw)
        ? raw
        : this.arrayFromObject(raw, ['references', 'items', 'sources', 'data']);
      const normalized = items
        .map((item, index) => this.normalizeReportReferenceArtifactItem(item, index))
        .filter((item): item is ReportSourceListItem => Boolean(item));
      if (normalized.length) return normalized;
    }
    return null;
  }

  private normalizeReportReferenceArtifactItem(item: unknown, index: number): ReportSourceListItem | null {
    if (!item || typeof item !== 'object') return null;
    const source = item as Record<string, unknown>;
    const citationNo = this.firstNumber(source, ['citationNo', 'citation_no', 'number', 'refNo', 'ref_no']) ?? index + 1;
    const normalized = this.normalizeSourceRecord(source, index, 'report_refs');
    const title = this.sanitizeLogText(
      this.firstString(source, ['title', 'ch_title', 'headline', 'sourceTitle']) ||
        this.firstString(source, ['rawReferenceText', 'raw_reference_text', 'referenceText', 'reference_text']) ||
        `\u53c2\u8003\u7f16\u53f7 [${citationNo}]`,
      220,
    );
    const rawReferenceText = this.sanitizeLogText(
      this.firstString(source, ['rawReferenceText', 'raw_reference_text', 'referenceText', 'reference_text']),
      1200,
    );
    const status = this.firstString(source, ['matchStatus', 'match_status']);
    return {
      ...normalized,
      id: this.sanitizeLogText(normalized.id || `report-ref-${citationNo}`, 260),
      sourceGroup: 'report_refs',
      evidenceKind: 'report_reference',
      citationNo,
      title,
      sourceType: normalized.sourceType || '\u62a5\u544a\u5f15\u7528',
      relevanceScore: normalized.relevanceScore ?? Math.max(100 - index, 1),
      status: normalized.status || 'referenced',
      method: normalized.method || '\u62a5\u544a\u53c2\u8003\u8d44\u6599\u7d22\u5f15',
      rawReferenceText,
      matchStatus: status === 'matched' || status === 'failed' || status === 'raw_only'
        ? status
        : rawReferenceText
          ? 'raw_only'
          : 'matched',
    };
  }

  private async writeReportReferencesArtifact(
    job: JobRecord,
    markdown: string,
    prebuiltItems?: ReportSourceListItem[],
  ): Promise<void> {
    try {
      const items = prebuiltItems ?? await this.buildReportReferenceItems(job, markdown);
      const references = items.slice(0, 300).map((item) => ({
        citationNo: item.citationNo,
        title: item.title || '',
        sourceName: item.sourceName || '',
        url: item.url || '',
        publishedAt: item.publishTime || '',
        summary: item.summary || '',
        excerpt: item.excerpt || '',
        rawReferenceText: item.rawReferenceText || '',
        sourceType: item.sourceType || '',
        sourceOrigin: item.sourceOrigin || '',
        evidenceKind: item.evidenceKind || '',
        engine: item.engine || '',
        relevanceScore: item.relevanceScore,
        status: item.status || '',
        method: item.method || '',
        matchStatus: item.matchStatus || 'raw_only',
      }));
      const filePath = this.reportReferencesArtifactPath(job);
      const dirPath = this.remoteFs.joinPath(this.remoteFs.remoteDir, job.jobId, 'references');
      await this.remoteFs.mkdir(dirPath);
      await this.remoteFs.writeFile(
        filePath,
        JSON.stringify({
          jobId: job.jobId,
          updatedAt: new Date().toISOString(),
          sourceCount: references.length,
          references,
        }, null, 2),
      );
      job.artifacts = {
        ...job.artifacts,
        reportReferencesPath: filePath,
        reportReferencesCount: references.length,
      };
    } catch (err) {
      console.error('writeReportReferencesArtifact failed:', err instanceof Error ? err.message : err);
    }
  }

  private parseReferenceEntriesRobust(markdown: string): Map<number, Partial<ReportSourceListItem>> {
    const refs = new Map<number, Partial<ReportSourceListItem>>();
    const refsStart = this.findReferenceSectionStart(markdown);
    if (refsStart < 0) return refs;
    const refText = markdown.slice(refsStart);
    const regex = /(?:^|\n)\s*(?:\[(\d{1,3})\]|(\d{1,3})[\u3001.\uff0e])\s*([\s\S]*?)(?=\n\s*(?:\[\d{1,3}\]|\d{1,3}[\u3001.\uff0e])\s*|$)/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(refText)) !== null) {
      const number = Number(match[1] || match[2]);
      const entry = String(match[3] || '').replace(/\s+/g, ' ').trim();
      if (!number || !entry) continue;
      const url = entry.match(/https?:\/\/\S+/)?.[0]?.replace(/[),.;\uff0c\u3002\uff1b\uff09]+$/g, '') || '';
      const title = url ? entry.replace(url, '').trim() : entry;
      refs.set(number, {
        title: this.sanitizeLogText(title || entry, 220),
        url: this.sanitizeLogText(url, 500),
        summary: this.sanitizeLogText(entry, 800),
        rawReferenceText: this.sanitizeLogText(entry, 1200),
      });
    }
    return refs;
  }

  private findReferenceSectionStart(markdown: string): number {
    return markdown.search(
      /(?:^|\n)\s*(?:#{1,6}\s*)?(?:\*\*)?\s*(?:[\u4e00-\u9fa5]+[\u3001.\uff0e]\s*)?(?:\u53c2\u8003\u6587\u732e|\u53c2\u8003\u8d44\u6599|references)(?:\*\*)?\s*[:\uff1a]?\s*(?:\n|$)/iu,
    );
  }

  private async structuredReportSources(job: JobRecord): Promise<ReportSourceListItem[]> {
    const data = await this.getDatabaseSources(job.jobId);
    return (data?.sources || []).map((source, index) => ({
      id: `structured-${source.url || source.title || index}`,
      sourceGroup: 'structured_sources',
      sourceOrigin: 'database_recall',
      evidenceKind: 'structured_source',
      engine: data?.retrievalMode === 'vector' || data?.retrievalMode === 'hybrid' ? 'pg_vector' : 'database',
      title: source.title || source.url || '未命名信源',
      url: source.url || '',
      sourceName: source.websiteName || '',
      publishTime: source.publishTime || '',
      summary: source.summary || '',
      excerpt: '',
      sourceType: data?.retrievalMode === 'vector' ? '向量召回' : data?.retrievalMode === 'hybrid' ? '混合召回' : '数据库记录',
      relevanceScore: Math.max(95 - index, 1),
      status: 'structured',
      method: data?.retrievalMode === 'vector' ? '向量透明展示' : data?.retrievalMode === 'hybrid' ? '数据库/向量透明展示' : '数据库透明展示',
    }));
  }

  private async candidateHitSources(job: JobRecord): Promise<{ items: ReportSourceListItem[]; total: number; detailSaved: boolean }> {
    const data = await this.getDatabaseSources(job.jobId);
    const total = this.candidateHitTotal(data);
    const rawItems = await this.readCandidateSourceItems(job);
    const items = rawItems.map((item, index) => this.normalizeCandidateSourceItem(item, index));
    return {
      items,
      total: total || items.length,
      detailSaved: items.length > 0,
    };
  }

  private async extractFailedSources(job: JobRecord): Promise<ReportSourceListItem[]> {
    const dir = await this.resolveOpenClawJobDir(job);
    if (!dir) return [];
    const sourcesPath = this.remoteFs.joinPath(dir, 'database', 'database_sources.json');
    const raw = await this.readJsonFile(sourcesPath);
    const items = Array.isArray(raw) ? raw : this.arrayFromObject(raw, ['items', 'sources', 'results', 'data']);
    return items
      .filter((item) => {
        const source = item && typeof item === 'object' ? item as Record<string, unknown> : {};
        const text = `${this.firstString(source, ['status', 'extract_status', 'source_status'])} ${this.firstString(source, ['error', 'message', 'failure_reason', 'failedReason'])}`;
        return /fail|error|失败|错误|不可用/i.test(text);
      })
      .map((item, index) => {
        const source = item as Record<string, unknown>;
        const normalized = this.normalizeSourceRecord(source, index, 'extract_failed');
        return {
          ...normalized,
          status: 'failed',
          failedReason: this.firstString(source, ['failure_reason', 'failedReason', 'error', 'message']),
          sourceType: normalized.sourceType || '抽取失败',
        };
      });
  }

  private async toolSearchSources(job: JobRecord): Promise<ReportSourceListItem[]> {
    const dir = this.remoteFs.joinPath(this.remoteFs.remoteDir, job.jobId);
    if (!(await this.remoteFs.exists(dir))) return [];
    const researchDir = this.remoteFs.joinPath(dir, 'research');
    if (!(await this.remoteFs.exists(researchDir))) return [];

    const rawItems: Array<{ item: unknown; evidenceKind: ReportEvidenceKind }> = [];
    for (const filename of ['consolidated.json']) {
      const parsed = await this.readJsonFile(this.remoteFs.joinPath(researchDir, filename));
      rawItems.push(...this.extractToolSearchRawItems(parsed));
    }

    try {
      const entries = await this.remoteFs.readdir(researchDir);
      for (const entry of entries) {
        if (!entry.isFile || !/^research_[a-z0-9_-]+\.json$/i.test(entry.name)) continue;
        const parsed = await this.readJsonFile(this.remoteFs.joinPath(researchDir, entry.name));
        rawItems.push(...this.extractToolSearchRawItems(parsed));
        if (rawItems.length >= 300) break;
      }
    } catch {
      // Missing research directory is a valid state for older or failed jobs.
    }

    const normalized = rawItems
      .slice(0, 300)
      .map(({ item, evidenceKind }, index) => this.normalizeToolSearchSourceItem(item, index, evidenceKind))
      .filter((item): item is ReportSourceListItem => Boolean(item));
    return this.mergeReportSourceItems(normalized, 'tool_search').slice(0, 300);
  }

  private extractToolSearchRawItems(value: unknown, depth = 0): Array<{ item: unknown; evidenceKind: ReportEvidenceKind }> {
    if (!value || depth > 6) return [];
    if (Array.isArray(value)) return value.flatMap((item) => this.extractToolSearchRawItems(item, depth + 1));
    if (typeof value !== 'object') return [];

    const record = value as Record<string, unknown>;
    const result: Array<{ item: unknown; evidenceKind: ReportEvidenceKind }> = [];
    for (const [key, candidate] of Object.entries(record)) {
      const evidenceKind = this.evidenceKindForToolSearchKey(key);
      if (evidenceKind && Array.isArray(candidate)) {
        for (const item of candidate) {
          if (this.isToolSearchRawItem(item)) result.push({ item, evidenceKind });
        }
      } else if (candidate && typeof candidate === 'object') {
        result.push(...this.extractToolSearchRawItems(candidate, depth + 1));
      }
      if (result.length >= 300) break;
    }
    return result;
  }

  private evidenceKindForToolSearchKey(key: string): ReportEvidenceKind | null {
    const normalized = key.toLowerCase();
    if (normalized === 'sources' || normalized === 'source_list') return 'research_source';
    if (normalized === 'documents') return 'research_source';
    if (normalized === 'evidence_cards' || normalized === 'evidencecards') return 'evidence_card';
    if (normalized === 'key_findings' || normalized === 'keyfindings' || normalized === 'verification_needed') return 'evidence_card';
    return null;
  }

  private isToolSearchRawItem(item: unknown): boolean {
    if (!item || typeof item !== 'object') return false;
    const source = item as Record<string, unknown>;
    const haystack = [
      this.firstString(source, ['engine', 'search_engine', 'provider']),
      this.firstString(source, ['method', 'retrievalMode', 'collection_method', 'mode']),
      this.firstString(source, ['source_type', 'type', 'sourceType']),
      this.firstString(source, ['url', 'source_url', 'data_source_url', 'sourceUrl']),
    ].join(' ').toLowerCase();
    return /\b(exa|firecrawl|tavily|tavily_extract)\b/.test(haystack);
  }

  private normalizeToolSearchSourceItem(
    item: unknown,
    index: number,
    evidenceKind: ReportEvidenceKind,
  ): ReportSourceListItem | null {
    if (!item || typeof item !== 'object') return null;
    const source = item as Record<string, unknown>;
    const normalized = this.normalizeSourceRecord(source, index, 'tool_search');
    const engine = this.inferToolSearchEngine(normalized, source);
    if (!engine) return null;
    return {
      ...normalized,
      sourceGroup: 'tool_search',
      sourceOrigin: 'tool_search',
      evidenceKind,
      engine,
      sourceType: this.toolSearchSourceTypeLabel(engine),
      status: normalized.status || this.firstString(source, ['success']) || 'collected',
      method: normalized.method || this.toolSearchMethodLabel(engine),
    };
  }

  private inferDatabaseEngine(item: Partial<ReportSourceListItem>, raw?: Record<string, unknown>): ReportSourceEngine {
    const text = `${item.engine || ''} ${item.sourceType || ''} ${item.method || ''} ${raw ? JSON.stringify(raw).slice(0, 500) : ''}`.toLowerCase();
    return /pg|vector|向量/.test(text) ? 'pg_vector' : 'database';
  }

  private inferToolSearchEngine(item: Partial<ReportSourceListItem>, raw?: Record<string, unknown>): ReportSourceEngine | undefined {
    const text = `${item.engine || ''} ${item.sourceType || ''} ${item.method || ''} ${item.url || ''} ${raw ? JSON.stringify(raw).slice(0, 500) : ''}`.toLowerCase();
    if (/tavily_extract/.test(text)) return 'tavily_extract';
    if (/\bfirecrawl\b/.test(text)) return 'firecrawl';
    if (/\btavily\b/.test(text)) return 'tavily';
    if (/\bexa\b/.test(text)) return 'exa';
    return undefined;
  }

  private toolSearchSourceTypeLabel(engine?: ReportSourceEngine): string {
    if (engine === 'exa') return 'Exa搜索';
    if (engine === 'firecrawl') return 'Firecrawl抽取';
    if (engine === 'tavily_extract') return 'Tavily抽取';
    if (engine === 'tavily') return 'Tavily搜索';
    return '工具调用搜索';
  }

  private toolSearchMethodLabel(engine?: ReportSourceEngine): string {
    if (engine === 'exa') return 'Exa 语义搜索';
    if (engine === 'firecrawl') return 'Firecrawl 内容抽取';
    if (engine === 'tavily_extract') return 'Tavily Extract 内容抽取';
    if (engine === 'tavily') return 'Tavily 实时搜索';
    return '工具调用搜索';
  }

  private candidateHitTotal(data: DatabaseSourcesResponse | undefined): number {
    const queryPlanTotal = (data?.queryPlan.strictHits || 0) + (data?.queryPlan.expandedHits || 0);
    const vectorTotal = data?.vectorPlan?.vectorHits || 0;
    return Math.max(data?.totalHits || 0, queryPlanTotal + vectorTotal);
  }

  private async readCandidateSourceItems(job: JobRecord): Promise<unknown[]> {
    const artifactItems = this.arrayFromObject(job.artifacts, [
      'candidateSources',
      'candidate_hits',
      'candidateHits',
      'retrievalHits',
      'vectorDatabaseCandidateSources',
    ]);
    const fileItems: unknown[] = [];
    const dir = await this.resolveOpenClawJobDir(job);
    if (dir) {
      const databaseDir = this.remoteFs.joinPath(dir, 'database');
      for (const filename of ['database_candidate_sources.json', 'candidate_sources.json', 'retrieval_hits.json']) {
        const parsed = await this.readJsonFile(this.remoteFs.joinPath(databaseDir, filename));
        if (Array.isArray(parsed)) fileItems.push(...parsed);
        else fileItems.push(...this.arrayFromObject(parsed, ['items', 'sources', 'results', 'data', 'hits', 'candidates']));
      }
      const plan = await this.readJsonFile(this.remoteFs.joinPath(databaseDir, 'database_query_plan.json'));
      fileItems.push(...this.arrayFromObject(plan, [
        'candidateSources',
        'candidate_sources',
        'candidateHits',
        'candidate_hits',
        'retrievalHits',
        'retrieval_hits',
        'hits',
        'candidates',
      ]));
    }
    return this.dedupeRawSources([...fileItems, ...artifactItems]);
  }

  private normalizeCandidateSourceItem(item: unknown, index: number): ReportSourceListItem {
    const source = item && typeof item === 'object' ? item as Record<string, unknown> : {};
    const normalized = this.normalizeSourceRecord(source, index, 'candidate_hits');
    return {
      ...normalized,
      sourceType: normalized.sourceType || '候选命中',
      relevanceScore: this.firstNumber(source, ['relevance_score', 'relevanceScore', 'score', 'similarity', 'rank_score']) ?? normalized.relevanceScore,
      status: this.firstString(source, ['status', 'source_status']) || 'candidate',
      method: this.firstString(source, ['method', 'retrievalMode', 'collection_method']) || '检索阶段候选池',
      candidateStage: this.firstString(source, ['candidateStage', 'candidate_stage', 'stage']),
      hitType: this.firstString(source, ['hitType', 'hit_type', 'type']),
    };
  }

  private normalizeSourceRecord(source: Record<string, unknown>, index: number, sourceGroup: Exclude<ReportSourceListType, 'all'>): ReportSourceListItem {
    const title = this.firstString(source, ['title', 'ch_title', 'headline', 'sourceTitle', 'name']);
    const url = this.firstString(source, ['url', 'source_url', 'data_source_url', 'sourceUrl']);
    const sourceName = this.firstString(source, ['publisher', 'website_name', 'source_name', 'site_name', 'sourceName', 'websiteName']);
    const publishTime = this.firstString(source, ['published_at', 'publish_time', 'pub_time', 'source_time', 'publishTime', 'publishedAt', 'time']);
    const summary = this.firstString(source, ['summary', 'abstract', 'description', 'snippet', 'finding', 'claim', 'content_preview']);
    const excerpt = this.firstString(source, ['excerpt', 'content_excerpt', 'chunk_text', 'content_chunk', 'body', 'content', 'markdown', 'content_preview']);
    const sourceType = this.firstString(source, ['source_type', 'type', 'tag', 'designated_tag', 'sourceType']);
    const score = this.firstNumber(source, ['relevance_score', 'relevanceScore', 'score', 'similarity', 'rank_score', 'credibility_score']);
    const id = this.firstString(source, ['id', 'sourceId', 'source_id', 'mysql_id']) || `${sourceGroup}-${url || title || index}`;
    const engine = this.firstString(source, ['engine', 'search_engine', 'provider']);
    return {
      id: this.sanitizeLogText(id, 260),
      sourceGroup,
      title: this.sanitizeLogText(title || url || '未命名信源', 220),
      url: this.sanitizeLogText(url, 500),
      sourceName: this.sanitizeLogText(sourceName, 140),
      publishTime: this.sanitizeLogText(publishTime, 80),
      summary: this.sanitizeLogText(summary, 1200),
      excerpt: this.sanitizeLogText(excerpt, 1200),
      sourceType: this.sanitizeLogText(sourceType, 80),
      relevanceScore: score,
      status: this.sanitizeLogText(this.firstString(source, ['status', 'extract_status', 'source_status']), 80),
      method: this.sanitizeLogText(this.firstString(source, ['method', 'retrievalMode', 'collection_method']), 120),
      engine: this.sanitizeLogText(engine, 40) as ReportSourceEngine,
    };
  }

  private async reportMarkdown(job: JobRecord): Promise<string> {
    if (job.markdown) return job.markdown;
    const recovered = await this.readMarkdownFile(job.resultPath || null);
    return recovered?.markdown || '';
  }

  private parseCitationNumbers(markdown: string): number[] {
    const refsStart = this.findReferenceSectionStart(markdown);
    const body = refsStart >= 0 ? markdown.slice(0, refsStart) : markdown;
    const seen = new Set<number>();
    const numbers: number[] = [];
    const regex = /\[(\d{1,3})\]/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(body)) !== null) {
      const number = Number(match[1]);
      if (!number || seen.has(number)) continue;
      seen.add(number);
      numbers.push(number);
    }
    return numbers.sort((a, b) => a - b);
  }

  private parseReferenceEntries(markdown: string): Map<number, Partial<ReportSourceListItem>> {
    const refs = new Map<number, Partial<ReportSourceListItem>>();
    const refsStart = this.findReferenceSectionStart(markdown);
    if (refsStart < 0) return refs;
    const refText = markdown.slice(refsStart);
    const regex = /\[(\d{1,3})\]\s*([\s\S]*?)(?=\n\s*\[\d{1,3}\]\s*|$)/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(refText)) !== null) {
      const number = Number(match[1]);
      const entry = String(match[2] || '').replace(/\s+/g, ' ').trim();
      if (!number || !entry) continue;
      const url = entry.match(/https?:\/\/\S+/)?.[0]?.replace(/[),.;，。；）]+$/g, '') || '';
      const title = url ? entry.replace(url, '').trim() : entry;
      refs.set(number, {
        title: this.sanitizeLogText(title || entry, 220),
        url: this.sanitizeLogText(url, 500),
        summary: this.sanitizeLogText(entry, 800),
      });
    }
    return refs;
  }

  private arrayFromObject(value: unknown, keys: string[]): unknown[] {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return [];
    const record = value as Record<string, unknown>;
    for (const key of keys) {
      const candidate = record[key];
      if (Array.isArray(candidate)) return candidate;
    }
    return [];
  }

  private dedupeRawSources(items: unknown[]): unknown[] {
    const seen = new Set<string>();
    const result: unknown[] = [];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      const source = item as Record<string, unknown>;
      const key = this.firstString(source, ['url', 'source_url', 'data_source_url']) ||
        `${this.firstString(source, ['title', 'ch_title', 'headline'])}|${this.firstString(source, ['summary', 'abstract', 'description'])}`;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      result.push(item);
    }
    return result;
  }

  private async resolveOpenClawJobDir(job: JobRecord): Promise<string | null> {
    const exactDir = this.remoteFs.joinPath(this.remoteFs.remoteDir, job.jobId);
    if (job.status === 'queued' || job.status === 'running') return exactDir;

    const fromKnownPath = await this.resolveOpenClawJobDirFromKnownPaths(job);
    if (fromKnownPath) return fromKnownPath;

    const reportDir = this.remoteFs.remoteDir;
    const entries = await this.remoteFs.readdir(reportDir);
    const createdAtMs = new Date(job.createdAt).getTime();
    const updatedAtMs = new Date(job.updatedAt || job.createdAt).getTime();
    const candidates: Array<{ dir: string; score: number }> = [];

    for (const entry of entries) {
      if (!entry.isDirectory || !/^[0-9a-f]{8}-[0-9a-f-]{27}$/i.test(entry.name)) continue;
      const dir = this.remoteFs.joinPath(reportDir, entry.name);
      const planPath = this.remoteFs.joinPath(dir, 'database', 'database_query_plan.json');
      const sourcesPath = this.remoteFs.joinPath(dir, 'database', 'database_sources.json');
      const hasPlan = await this.remoteFs.exists(planPath);
      const hasSources = await this.remoteFs.exists(sourcesPath);
      if (!hasPlan && !hasSources) continue;

      const reportPath = this.remoteFs.joinPath(dir, 'final', 'report.md');
      let mtimeMs = 0;
      try {
        mtimeMs = (await this.remoteFs.stat(reportPath)).mtimeMs;
      } catch {
        try {
          mtimeMs = (await this.remoteFs.stat(hasSources ? sourcesPath : planPath)).mtimeMs;
        } catch {
          mtimeMs = 0;
        }
      }
      if (!mtimeMs) continue;

      const inWindow = mtimeMs >= createdAtMs - 15 * 60_000 && mtimeMs <= updatedAtMs + 15 * 60_000;
      if (!inWindow || !(await this.openClawJobDirHasJobEvidence(dir, job))) continue;
      const proximity = Math.abs(mtimeMs - updatedAtMs);
      const score = proximity;
      candidates.push({ dir, score });
    }

    if (!candidates.length) return null;
    candidates.sort((a, b) => a.score - b.score);
    return candidates[0]?.dir ?? null;
  }

  private async resolveOpenClawJobDirFromKnownPaths(job: JobRecord): Promise<string | null> {
    const candidates = new Set<string>();
    const addFromText = (value: unknown) => {
      if (typeof value !== 'string' || !value.trim()) return;
      for (const dir of this.extractOpenClawJobDirs(value)) candidates.add(dir);
    };

    addFromText(job.resultPath);
    addFromText(job.markdown);
    for (const entry of job.eventLog || []) {
      addFromText(entry.summary);
      addFromText(entry.command);
      addFromText(entry.detail);
    }
    for (const event of job.events || []) addFromText(JSON.stringify(event));

    for (const dir of candidates) {
      if (await this.hasDatabaseSourceFiles(dir)) return dir;
    }
    return null;
  }

  private openClawJobDirMatchesJob(dir: string, jobId: string): boolean {
    return dir.replace(/\\/g, '/').split('/').filter(Boolean).pop() === jobId;
  }

  private async openClawJobDirHasJobEvidence(dir: string, job: JobRecord): Promise<boolean> {
    if (this.openClawJobDirMatchesJob(dir, job.jobId)) return true;
    try {
      const contextText = await this.remoteFs.readFile(this.remoteFs.joinPath(dir, 'context.json'));
      return contextText.includes(job.jobId);
    } catch {
      return false;
    }
  }

  private extractOpenClawJobDirs(text: string): string[] {
    const dirs = new Set<string>();
    const normalizedText = text.replace(/\\/g, '/');
    const pathMatches = normalizedText.match(/(?:[A-Za-z]:\/|\/)[^\s"'<>，。；、)）\]]+/g) || [];
    for (const rawPath of pathMatches) {
      const candidate = rawPath.replace(/[.,;:]+$/g, '');
      const dir = this.openClawJobDirFromPath(candidate);
      if (dir) dirs.add(dir);
    }
    return Array.from(dirs);
  }

  private openClawJobDirFromPath(rawPath: string): string | null {
    const filePath = rawPath.replace(/\\/g, '/');
    const reportDir = this.remoteFs.remoteDir.replace(/\\/g, '/').replace(/\/+$/g, '');
    const uuidSegment = '[0-9a-f]{8}-[0-9a-f-]{27}';
    const nestedMatch = filePath.match(new RegExp(`^(.*/${uuidSegment})(?:/|$)`, 'i'));
    if (nestedMatch?.[1]) return nestedMatch[1];

    if (filePath.startsWith(`${reportDir}/`)) {
      const relative = filePath.slice(reportDir.length + 1);
      const firstSegment = relative.split('/')[0] || '';
      if (new RegExp(`^${uuidSegment}$`, 'i').test(firstSegment)) return `${reportDir}/${firstSegment}`;
    }
    return null;
  }

  private async hasDatabaseSourceFiles(dir: string): Promise<boolean> {
    const planPath = this.remoteFs.joinPath(dir, 'database', 'database_query_plan.json');
    const sourcesPath = this.remoteFs.joinPath(dir, 'database', 'database_sources.json');
    return (await this.remoteFs.exists(planPath)) || (await this.remoteFs.exists(sourcesPath));
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
    if (this.hasForbiddenWriteHbPrefaceHeadings(text)) {
      throw new Error('OpenClaw report-agent returned invalid K/HB format: standalone 导语/摘要 headings are not allowed.');
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
    if (this.hasForbiddenWriteHbPrefaceHeadings(text)) return false;
    return true;
  }

  private hasForbiddenWriteHbPrefaceHeadings(markdown: string): boolean {
    const text = String(markdown || '');
    const looksLikeWriteHb =
      /\*\*编号：\*\*\s*[KH]-\d{8}-\d{3}/.test(text) ||
      /##\s*\*\*一、基本情况\*\*/.test(text) ||
      /##\s*\*\*二、涉我风险/.test(text) ||
      /##\s*\*\*三、对策建议/.test(text) ||
      /##\s*\*\*一、事件概述\*\*/.test(text);
    if (!looksLikeWriteHb) return false;

    const beforeFirstSection = text.split(/##\s*\*\*一、(?:基本情况|事件概述)\*\*/)[0] || text;
    return /(?:^|\n)\s{0,3}#{1,6}\s*(?:导语|摘要|导语\s*[/／、-]\s*摘要|摘要\s*导语)\s*(?:\n|$)/.test(beforeFirstSection) ||
      /(?:^|\n)\s*(?:导语|摘要|导语\s*[/／、-]\s*摘要|摘要\s*导语)\s*(?:\n|$)/.test(beforeFirstSection);
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
    const parsed = marked(this.normalizeMarkdownStrongMarkers(markdown || ''));
    return typeof parsed === 'string' ? parsed : await parsed;
  }

  private normalizeMarkdownStrongMarkers(markdown: string): string {
    const lines = markdown.split(/\r?\n/);
    let inFence = false;

    return lines
      .map((line) => {
        if (/^\s*```/.test(line)) {
          inFence = !inFence;
          return line;
        }
        if (inFence || !line.includes('**')) return line;

        const inlineCode: string[] = [];
        const masked = line.replace(/(`+)([^`]*?)\1/g, (match) => {
          inlineCode.push(match);
          return `\u0000CODE${inlineCode.length - 1}\u0000`;
        });

        const normalized = masked.replace(/\*\*([^*\n]+?)\*\*/g, '<strong>$1</strong>');
        return normalized.replace(/\u0000CODE(\d+)\u0000/g, (_match, index) => inlineCode[Number(index)] || '');
      })
      .join('\n');
  }
}
