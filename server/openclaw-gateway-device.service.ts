import { Injectable } from '@nestjs/common';
import crypto from 'crypto';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { WebSocket } from 'ws';
import { OPENCLAW_STATE_DIR, OPENCLAW_WS_URL } from './config.js';

const ED25519_SPKI_PREFIX = Buffer.from('302a300506032b6570032100', 'hex');
const CLIENT_ID = 'gateway-client';
const CLIENT_MODE = 'backend';
const ROLE = 'operator';
const DEFAULT_SCOPES = ['operator.admin', 'operator.read', 'operator.write', 'operator.approvals'];

interface DeviceIdentity {
  deviceId: string;
  publicKeyPem: string;
  privateKeyPem: string;
}

interface StoredDeviceAuth {
  token: string;
  scopes?: string[];
}

interface GatewayResponse {
  type: 'res';
  id: string;
  ok: boolean;
  payload?: unknown;
  error?: {
    code?: string;
    message?: string;
    details?: unknown;
  };
}

interface GatewayRequestOptions {
  timeoutMs?: number;
}

@Injectable()
export class OpenClawGatewayDeviceService {
  async health() {
    try {
      const result = await this.withClient(async (client) => {
        const hello = client.hello;
        const health = await client.request('health', {});
        return { hello, health };
      });

      return {
        ok: true,
        status: 'paired',
        wsUrl: OPENCLAW_WS_URL,
        clientId: CLIENT_ID,
        deviceId: this.safeDeviceId(result.hello),
        auth: this.summarizeHelloAuth(result.hello),
        health: result.health,
        details: [],
      };
    } catch (error) {
      return {
        ok: false,
        status: 'failed',
        wsUrl: OPENCLAW_WS_URL,
        clientId: CLIENT_ID,
        details: [error instanceof Error ? error.message : String(error)],
      };
    }
  }

  async request(method: string, params: Record<string, unknown> = {}, options: GatewayRequestOptions = {}) {
    return this.withClient((client) => client.request(method, params, options));
  }

  async runAgent(params: {
    agentId: string;
    message: string;
    timeoutMs: number;
    sessionKey?: string;
    label?: string;
    onEvent?: (event: { type: string; payload: unknown }) => void;
  }) {
    const idempotencyKey = crypto.randomUUID();
    const sessionKey = params.sessionKey || `agent:${params.agentId}:openai:${idempotencyKey}`;
    return this.withClient((client) => {
      if (params.onEvent) client.onGatewayEvent = params.onEvent;
      return client.request(
        'agent',
        {
          agentId: params.agentId,
          message: params.message,
          idempotencyKey,
          sessionKey,
          deliver: false,
          timeout: Math.ceil(params.timeoutMs / 1000),
          bestEffortDeliver: true,
          ...(params.label ? { label: params.label } : {}),
        },
        { timeoutMs: params.timeoutMs + 30000 },
      );
    });
  }

  private async withClient<T>(fn: (client: DeviceGatewayClient) => Promise<T>): Promise<T> {
    const client = new DeviceGatewayClient({
      url: OPENCLAW_WS_URL,
      identity: this.loadIdentity(),
      storedAuth: this.loadStoredAuth(),
    });

    try {
      await client.connect();
      return await fn(client);
    } finally {
      client.close();
    }
  }

  private loadIdentity(): DeviceIdentity {
    const filePath = path.join(OPENCLAW_STATE_DIR, 'identity', 'device.json');
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf-8')) as Partial<DeviceIdentity>;
    if (!parsed.deviceId || !parsed.publicKeyPem || !parsed.privateKeyPem) {
      throw new Error(`Invalid OpenClaw device identity at ${filePath}`);
    }
    return {
      deviceId: parsed.deviceId,
      publicKeyPem: parsed.publicKeyPem,
      privateKeyPem: parsed.privateKeyPem,
    };
  }

  private loadStoredAuth(): StoredDeviceAuth {
    const filePath = path.join(OPENCLAW_STATE_DIR, 'identity', 'device-auth.json');
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf-8')) as {
      tokens?: Record<string, StoredDeviceAuth>;
    };
    const operator = parsed.tokens?.operator;
    if (!operator?.token) {
      throw new Error(`Missing OpenClaw operator device token at ${filePath}`);
    }
    return operator;
  }

  private safeDeviceId(hello: unknown) {
    const auth = this.readAuthObject(hello);
    return typeof auth?.deviceId === 'string' ? auth.deviceId : undefined;
  }

  private summarizeHelloAuth(hello: unknown) {
    const auth = this.readAuthObject(hello);
    if (!auth) return undefined;
    return {
      role: typeof auth.role === 'string' ? auth.role : undefined,
      scopes: Array.isArray(auth.scopes) ? auth.scopes : undefined,
      hasDeviceToken: typeof auth.deviceToken === 'string' && auth.deviceToken.length > 0,
    };
  }

  private readAuthObject(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object') return null;
    const auth = (value as { auth?: unknown }).auth;
    return auth && typeof auth === 'object' ? (auth as Record<string, unknown>) : null;
  }
}

class DeviceGatewayClient {
  private ws: WebSocket | null = null;
  private pending = new Map<string, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
  hello: unknown;
  onGatewayEvent?: (event: { type: string; payload: unknown }) => void;

  constructor(
    private readonly options: {
      url: string;
      identity: DeviceIdentity;
      storedAuth: StoredDeviceAuth;
    },
  ) {}

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.options.url);
      this.ws = ws;

      const timeout = setTimeout(() => {
        reject(new Error(`OpenClaw device handshake timed out for ${this.options.url}`));
        ws.close(1008, 'device handshake timeout');
      }, 15000);

      ws.on('message', (raw) => {
        try {
          const frame = JSON.parse(raw.toString()) as { type?: string; event?: string; payload?: unknown };
          if (frame.type === 'event' && frame.event === 'connect.challenge') {
            const nonce = this.readNonce(frame.payload);
            void this.sendConnect(nonce)
              .then((hello) => {
                this.hello = hello;
                clearTimeout(timeout);
                resolve();
              })
              .catch((error) => {
                clearTimeout(timeout);
                reject(error);
              });
            return;
          }

          if (frame.type === 'res') {
            this.handleResponse(frame as GatewayResponse);
            return;
          }

          if (frame.type === 'event') {
            this.onGatewayEvent?.({ type: String(frame.event || 'event'), payload: frame.payload });
          }
        } catch (error) {
          clearTimeout(timeout);
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      });

      ws.on('error', (error) => {
        clearTimeout(timeout);
        reject(error instanceof Error ? error : new Error(String(error)));
      });

      ws.on('close', (code, reason) => {
        const text = reason.toString() || 'no reason';
        this.rejectPending(new Error(`OpenClaw gateway closed (${code}): ${text}`));
      });
    });
  }

  request(method: string, params: Record<string, unknown> = {}, options: GatewayRequestOptions = {}) {
    const id = crypto.randomUUID();
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error('OpenClaw gateway is not connected');
    }

    const promise = new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      setTimeout(() => {
        if (this.pending.delete(id)) {
          reject(new Error(`OpenClaw gateway request timed out: ${method}`));
        }
      }, options.timeoutMs ?? 30000).unref?.();
    });

    ws.send(JSON.stringify({ type: 'req', id, method, params }));
    return promise;
  }

  close() {
    this.ws?.close();
    this.ws = null;
  }

  private async sendConnect(nonce: string) {
    const signedAtMs = Date.now();
    const scopes = this.options.storedAuth.scopes?.length ? this.options.storedAuth.scopes : DEFAULT_SCOPES;
    const token = this.options.storedAuth.token;
    const platform = process.platform || os.platform();
    const deviceFamily = 'backend';
    const payload = [
      'v3',
      this.options.identity.deviceId,
      CLIENT_ID,
      CLIENT_MODE,
      ROLE,
      scopes.join(','),
      String(signedAtMs),
      token,
      nonce,
      platform,
      deviceFamily,
    ].join('|');

    return this.request('connect', {
      minProtocol: 3,
      maxProtocol: 3,
      client: {
        id: CLIENT_ID,
        displayName: 'Gaogao Backend',
        version: 'gaogao-local',
        platform,
        deviceFamily,
        mode: CLIENT_MODE,
      },
      caps: [],
      auth: {
        token,
        deviceToken: token,
      },
      role: ROLE,
      scopes,
      device: {
        id: this.options.identity.deviceId,
        publicKey: publicKeyRawBase64UrlFromPem(this.options.identity.publicKeyPem),
        signature: signDevicePayload(this.options.identity.privateKeyPem, payload),
        signedAt: signedAtMs,
        nonce,
      },
    });
  }

  private readNonce(payload: unknown) {
    const nonce = payload && typeof payload === 'object' ? (payload as { nonce?: unknown }).nonce : undefined;
    if (typeof nonce !== 'string' || nonce.trim().length === 0) {
      throw new Error('OpenClaw connect challenge did not include a nonce');
    }
    return nonce.trim();
  }

  private handleResponse(frame: GatewayResponse) {
    const pending = this.pending.get(frame.id);
    if (!pending) return;

    const status =
      frame.payload && typeof frame.payload === 'object'
        ? (frame.payload as { status?: unknown }).status
        : undefined;
    if (status === 'accepted') return;

    this.pending.delete(frame.id);
    if (frame.ok) {
      pending.resolve(frame.payload);
    } else {
      pending.reject(
        new Error(
          `OpenClaw gateway request failed: ${frame.error?.message || frame.error?.code || 'unknown error'}`,
        ),
      );
    }
  }

  private rejectPending(error: Error) {
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
  }
}

function signDevicePayload(privateKeyPem: string, payload: string) {
  const key = crypto.createPrivateKey(privateKeyPem);
  return base64UrlEncode(crypto.sign(null, Buffer.from(payload, 'utf8'), key));
}

function publicKeyRawBase64UrlFromPem(publicKeyPem: string) {
  const spki = crypto.createPublicKey(publicKeyPem).export({
    type: 'spki',
    format: 'der',
  });
  const raw =
    spki.length === ED25519_SPKI_PREFIX.length + 32 && spki.subarray(0, ED25519_SPKI_PREFIX.length).equals(ED25519_SPKI_PREFIX)
      ? spki.subarray(ED25519_SPKI_PREFIX.length)
      : spki;
  return base64UrlEncode(raw);
}

function base64UrlEncode(buf: Buffer) {
  return buf.toString('base64').replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/g, '');
}
