import type { ReportPayload, SkillName } from '../src/types/report.js';

export type ReportJobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'waiting_approval';
export type ReportProgressStageKey = 'prepare' | 'source' | 'plan' | 'research' | 'consolidate' | 'report';
export type ReportProgressStageStatus = 'not_started' | 'running' | 'done' | 'failed';

export interface ReportProgressEvidence {
  source: 'event' | 'tool_event' | 'artifact' | 'report_file' | 'job_status';
  message: string;
  time: string;
}

export interface ReportProgressStage {
  key: ReportProgressStageKey;
  title: string;
  desc: string;
  status: ReportProgressStageStatus;
  evidence: ReportProgressEvidence[];
}

export interface ReportProgressState {
  jobId: string;
  currentStage: ReportProgressStageKey | null;
  updatedAt: string;
  stages: ReportProgressStage[];
}

export interface JobRecord {
  jobId: string;
  skill: SkillName;
  payload: ReportPayload;
  status: ReportJobStatus;
  stage?: string;
  markdown?: string;
  resultPath?: string;
  errorMessage?: string;
  artifacts: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  completedAt?: string;
  events: ServerEvent[];
  eventLog: EventLogEntry[];
  progressState?: ReportProgressState;
}

export interface EventLogEntry {
  id: string;
  time: string;
  type: 'stage' | 'tool_start' | 'tool_delta' | 'tool_end' | 'tool_error' | 'done' | 'error';
  label: string;
  status: string;
  summary: string;
  command?: string;
  phase?: string;
  actor?: string;
  detail?: string;
  toolName?: string;
  toolDisplayName?: string;
  toolId?: string;
  toolEngine?: string;
}

export type ServerEvent =
  | { type: 'stage'; stage: string; message: string }
  | { type: 'progress_state'; progressState: ReportProgressState }
  | { type: 'status'; status: string; message?: string }
  | { type: 'token'; content: string }
  | { type: 'text_delta'; content: string }
  | { type: 'tool_start'; id?: string; name?: string; raw: unknown }
  | { type: 'tool_delta'; id?: string; name?: string; raw: unknown }
  | { type: 'tool_end'; id?: string; name?: string; raw: unknown }
  | { type: 'tool_error'; id?: string; name?: string; message: string; raw?: unknown }
  | { type: 'sources'; sources: Record<string, unknown>[] }
  | { type: 'approval_required'; commands: string[]; message: string; partialOutput?: string }
  | { type: 'artifact'; name: string; available: boolean }
  | { type: 'done'; jobId: string }
  | { type: 'error'; message: string };

export interface RunInput {
  skill: SkillName;
  payload: Record<string, unknown>;
  requestUser?: string;
  onEvent: (event: ServerEvent) => void;
  jobId: string;
}

export interface RunResult {
  markdown: string;
  artifacts: Record<string, unknown>;
}

export interface OpenClawHealth {
  ok: boolean;
  status: 'ready' | 'degraded' | 'down';
  checks: {
    openclawHttpApi: boolean;
    localProbe: boolean;
  };
  timeoutMs: number;
  details: string[];
}

export interface ReportPlanRequest {
  topic: string;
  reportType: string;
  context?: string;
  parameters?: Record<string, string>;
}

export interface ReportPlanOption {
  id: string;
  label: string;
  detail: string;
  selected?: boolean;
}

export type ReportPlanStepType =
  | 'search_queries'
  | 'source_scope'
  | 'basic_info_module'
  | 'analysis_module'
  | 'output_module'
  | 'report_section';

export interface ReportPlanStep {
  id: string;
  type: ReportPlanStepType;
  sectionKey?: string;
  sectionTitle?: string;
  title: string;
  description: string;
  allowMultiple: boolean;
  options: ReportPlanOption[];
}

export interface ReportPlanResponse {
  title: string;
  summary: string;
  searchQueries: string[];
  steps: ReportPlanStep[];
}
