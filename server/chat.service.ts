import { Injectable } from '@nestjs/common';
import { Subject } from 'rxjs';
import { v4 as uuid } from 'uuid';
import { OpenClawService } from './openclaw.service.js';
import type { ServerEvent } from './types.js';

interface ChatRequest {
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>;
  stream?: boolean;
  sessionId?: string;
}

@Injectable()
export class ChatService {
  private readonly streams = new Map<string, Subject<ServerEvent>>();
  private readonly history = new Map<string, ServerEvent[]>();

  constructor(private readonly openClaw: OpenClawService) {}

  async complete(body: ChatRequest) {
    if (body.stream) {
      const streamId = uuid();
      this.streams.set(streamId, new Subject<ServerEvent>());
      this.history.set(streamId, []);
      setImmediate(() => void this.runStream(streamId, body.messages, body.sessionId));
      return { streamId, eventsUrl: `/api/chat/streams/${streamId}` };
    }

    const events: ServerEvent[] = [];
    const text = await this.openClaw.streamQa(body.messages, (event) => events.push(event), body.sessionId);
    return {
      choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
      events,
    };
  }

  stream(streamId: string) {
    return {
      events: this.history.get(streamId),
      subject: this.streams.get(streamId),
    };
  }

  private async runStream(streamId: string, messages: ChatRequest['messages'], sessionId?: string) {
    try {
      await this.openClaw.streamQa(messages, (event) => this.push(streamId, event), sessionId);
      this.push(streamId, { type: 'done', jobId: streamId });
      this.streams.get(streamId)?.complete();
    } catch (error) {
      this.push(streamId, { type: 'error', message: error instanceof Error ? error.message : String(error) });
      this.streams.get(streamId)?.complete();
    }
  }

  private push(streamId: string, event: ServerEvent) {
    this.history.get(streamId)?.push(event);
    this.streams.get(streamId)?.next(event);
  }
}
