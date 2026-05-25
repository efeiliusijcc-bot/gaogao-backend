export type SkillName = 'risk-assessment-reports' | 'person-intelligence-report' | 'write-hb';

export type RiskScenario = 'leader_outbound' | 'foreign_leader_visit' | 'domestic_holiday';
export type PersonReportType = 'new_leader' | 'visiting_dignitary';
export type WriteHbReportType = 'K报' | 'HB报';
export type OutputDepth = 'brief' | 'standard' | 'detailed';
export type JobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'waiting_approval';

export interface RiskAssessmentPayload {
  scenario: RiskScenario;
  target_country?: string;
  target_city?: string;
  visit_time?: string;
  holiday_name?: string;
  holiday_time?: string;
  time_window?: string;
  focus_areas?: string[];
  known_context?: string;
  language?: string;
}

export interface PersonReportPayload {
  target_name: string;
  country_or_region: string;
  current_position: string;
  report_type: PersonReportType;
  visit_context?: string;
  appointment_context?: string;
  focus_areas?: string[];
  time_range?: string;
  output_depth?: OutputDepth;
  language?: string;
}

export interface WriteHbPayload {
  topic: string;
  report_type: WriteHbReportType;
  outline?: string;
  focus_areas?: string[];
  known_context?: string;
  language?: string;
}

export type ReportPayload = RiskAssessmentPayload | PersonReportPayload | WriteHbPayload;

export interface CreateJobRequest {
  skill: SkillName;
  payload: ReportPayload;
}

export interface ReportJob {
  jobId: string;
  skill: SkillName;
  payload: ReportPayload;
  status: JobStatus;
  stage?: string;
  markdown?: string;
  html?: string;
  resultPath?: string;
  errorMessage?: string;
  artifacts?: {
    source_table?: unknown[];
    risk_matrix?: unknown[];
    information_gaps?: string[];
  };
  createdAt: string;
  updatedAt: string;
}

export interface ReportJobListResponse {
  items: ReportJob[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
  statusCounts?: {
    succeeded: number;
    running: number;
  };
}

export interface OpenClawHealth {
  ok: boolean;
  status: 'ready' | 'degraded' | 'down';
  checks: {
    tavilyApiKey?: boolean;
    openclawBinary?: boolean;
    powershell?: boolean;
    openclawHttpApi?: boolean;
    localProbe: boolean;
  };
  timeoutMs: number;
  details: string[];
}

export interface DatabaseSourceItem {
  title: string;
  url: string;
  summary: string;
  websiteName: string;
  publishTime: string;
}

export interface DatabaseSourcesResponse {
  status: 'hit' | 'empty' | 'fallback' | 'unavailable';
  sources: DatabaseSourceItem[];
  fallbackReason: string;
  totalHits: number;
  updatedAt: string | null;
}

export type SSEEvent =
  | { type: 'stage'; stage: string; message: string }
  | { type: 'status'; status: string; message?: string }
  | { type: 'token'; content: string }
  | { type: 'text_delta'; content: string }
  | { type: 'tool_start'; id?: string; name?: string; raw: unknown }
  | { type: 'tool_delta'; id?: string; name?: string; raw: unknown }
  | { type: 'tool_end'; id?: string; name?: string; raw: unknown }
  | { type: 'tool_error'; id?: string; name?: string; message: string; raw?: unknown }
  | { type: 'approval_required'; commands: string[]; message: string; partialOutput?: string }
  | { type: 'artifact'; name: string; available: boolean }
  | { type: 'done'; jobId: string }
  | { type: 'error'; message: string };
