import type { ReportPayload, SkillName } from '../src/types/report.js';

export type ReportJobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'waiting_approval';

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
  events: ServerEvent[];
  eventLog: EventLogEntry[];
}

export interface EventLogEntry {
  id: string;
  time: string;
  type: 'stage' | 'tool_start' | 'tool_end' | 'tool_error' | 'done' | 'error';
  label: string;
  status: string;
  summary: string;
  command?: string;
}

export type ServerEvent =
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

export interface RunInput {
  skill: SkillName;
  payload: Record<string, unknown>;
  requestUser?: string;
  onEvent: (event: ServerEvent) => void;
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
