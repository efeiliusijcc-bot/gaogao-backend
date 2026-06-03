import { Body, Controller, Get, HttpException, HttpStatus, Param, Post, Put, Sse } from '@nestjs/common';
import { Observable } from 'rxjs';
import { ChatService } from './chat.service.js';
import { QaSessionSourcesService } from './qa-session-sources.service.js';
import type { ServerEvent } from './types.js';

@Controller('/api/chat')
export class ChatController {
  constructor(
    private readonly chat: ChatService,
    private readonly qaSources: QaSessionSourcesService,
  ) {}

  @Post('completions')
  completions(@Body() body: { messages?: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>; stream?: boolean; sessionId?: string }) {
    if (!Array.isArray(body.messages) || body.messages.length === 0) {
      throw new HttpException({ error: 'messages is required' }, HttpStatus.BAD_REQUEST);
    }
    return this.chat.complete({ messages: body.messages, stream: body.stream, sessionId: body.sessionId });
  }

  @Sse('streams/:streamId')
  stream(@Param('streamId') streamId: string): Observable<MessageEvent> {
    const { events, subject } = this.chat.stream(streamId);

    return new Observable((subscriber) => {
      if (!events || !subject) {
        subscriber.next({ data: { type: 'error', message: 'Stream not found' } as ServerEvent } as MessageEvent);
        subscriber.complete();
        return undefined;
      }

      for (const event of events) {
        subscriber.next({ data: event } as MessageEvent);
      }

      const subscription = subject.subscribe({
        next: (event) => subscriber.next({ data: event } as MessageEvent),
        complete: () => subscriber.complete(),
      });
      return () => subscription.unsubscribe();
    });
  }

  @Get('sessions/:sessionId/sources')
  sources(@Param('sessionId') sessionId: string) {
    return this.qaSources.getSources(sessionId);
  }

  @Put('sessions/:sessionId/sources')
  upsertSources(@Param('sessionId') sessionId: string, @Body() body: { sources?: unknown; merge?: boolean }) {
    return this.qaSources.upsertSources(sessionId, body || {});
  }
}
