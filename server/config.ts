import fs from 'fs';
import path from 'path';
import os from 'os';

const OPENCLAW_CONFIG_PATH =
  process.env.OPENCLAW_CONFIG_PATH || path.join(os.homedir(), '.openclaw', 'openclaw.json');

function readGatewayToken(): string | undefined {
  try {
    const raw = fs.readFileSync(OPENCLAW_CONFIG_PATH, 'utf-8');
    return raw.match(/"token"\s*:\s*"([^"]+)"/)?.[1];
  } catch {
    return undefined;
  }
}

function readDeviceAuthToken(): string | undefined {
  try {
    const filePath = path.join(os.homedir(), '.openclaw', 'identity', 'device-auth.json');
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf-8')) as {
      tokens?: { operator?: { token?: string } };
    };
    return parsed.tokens?.operator?.token;
  } catch {
    return undefined;
  }
}

export const OPENCLAW_BASE_URL = process.env.OPENCLAW_BASE_URL || 'http://localhost:18789/v1';
export const OPENCLAW_HEALTH_URL =
  process.env.OPENCLAW_HEALTH_URL || OPENCLAW_BASE_URL.replace(/\/v1\/?$/, '/health');
export const OPENCLAW_API_KEY = process.env.OPENCLAW_API_KEY || readGatewayToken() || 'openclaw-local';
export const OPENCLAW_DEVICE_TOKEN = readDeviceAuthToken();
export const OPENCLAW_MODEL = process.env.OPENCLAW_MODEL || 'openclaw/report-agent';
export const OPENCLAW_QA_AGENT_ID = process.env.OPENCLAW_QA_AGENT_ID || 'qa-agent';
export const OPENCLAW_QA_MODEL = process.env.OPENCLAW_QA_MODEL || 'openclaw/qa-agent';
export const OPENCLAW_QA_TIMEOUT_MS = Number(process.env.OPENCLAW_QA_TIMEOUT_MS || 900000);
export const REPORT_TIMEOUT_MS = Number(process.env.OPENCLAW_TIMEOUT_MS || 900000);
export const HEALTH_TIMEOUT_MS = Number(process.env.OPENCLAW_HEALTH_TIMEOUT_MS || 30000);
export const OPENCLAW_WS_URL =
  process.env.OPENCLAW_WS_URL || OPENCLAW_BASE_URL.replace(/^http/, 'ws').replace(/\/v1\/?$/, '');
export const OPENCLAW_STATE_DIR = process.env.OPENCLAW_STATE_DIR || path.join(os.homedir(), '.openclaw');
export const REPORT_OUTPUT_DIR =
  process.env.REPORT_OUTPUT_DIR || path.join(OPENCLAW_STATE_DIR, 'workspace', 'report-agent', 'reports');
export const OPENCLAW_REMOTE_HOST = process.env.OPENCLAW_REMOTE_HOST || '';
export const OPENCLAW_REMOTE_USER = process.env.OPENCLAW_REMOTE_USER || 'root';
export const OPENCLAW_REMOTE_SSH_KEY =
  process.env.OPENCLAW_REMOTE_SSH_KEY || path.join(os.homedir(), '.ssh', 'id_ed25519');
export const OPENCLAW_REMOTE_REPORT_DIR =
  process.env.OPENCLAW_REMOTE_REPORT_DIR || '/usr/docker/openclaw/workspace/report-agent/reports';
export const OPENCLAW_CONTAINER_REPORT_DIR =
  '/home/node/.openclaw/workspace/report-agent/reports';
export const TAVILY_API_KEY = process.env.TAVILY_API_KEY || '';
