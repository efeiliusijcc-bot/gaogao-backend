import { Injectable } from '@nestjs/common';
import OpenAI from 'openai';
import {
  HEALTH_TIMEOUT_MS,
  OPENCLAW_API_KEY,
  OPENCLAW_BASE_URL,
  OPENCLAW_CONTAINER_REPORT_DIR,
  OPENCLAW_HEALTH_URL,
  OPENCLAW_MODEL,
  REPORT_TIMEOUT_MS,
} from './config.js';
import { OpenClawGatewayDeviceService } from './openclaw-gateway-device.service.js';
import type { OpenClawHealth, RunInput, RunResult, ServerEvent } from './types.js';

export class OpenClawApprovalRequiredError extends Error {
  constructor(
    readonly commands: string[],
    readonly partialOutput: string,
  ) {
    super('OpenClaw requires tool approval before it can continue.');
    this.name = 'OpenClawApprovalRequiredError';
  }
}

@Injectable()
export class OpenClawService {
  constructor(private readonly gatewayDevice: OpenClawGatewayDeviceService) {}

  private readonly client = new OpenAI({
    apiKey: OPENCLAW_API_KEY,
    baseURL: OPENCLAW_BASE_URL,
    timeout: REPORT_TIMEOUT_MS,
  });

  async health(timeoutMs = HEALTH_TIMEOUT_MS): Promise<OpenClawHealth> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(OPENCLAW_HEALTH_URL, {
        method: 'GET',
        signal: controller.signal,
      });

      clearTimeout(timeout);
      if (!response.ok) {
        return {
          ok: false,
          status: 'degraded',
          checks: { openclawHttpApi: true, localProbe: false },
          timeoutMs,
          details: [`OpenClaw HTTP probe failed with status ${response.status}.`],
        };
      }

      return {
        ok: true,
        status: 'ready',
        checks: { openclawHttpApi: true, localProbe: true },
        timeoutMs,
        details: [],
      };
    } catch (error) {
      clearTimeout(timeout);
      return {
        ok: false,
        status: 'down',
        checks: { openclawHttpApi: false, localProbe: false },
        timeoutMs,
        details: [error instanceof Error ? error.message : String(error)],
      };
    }
  }

  async runReport(input: RunInput): Promise<RunResult> {
    const prompt = this.buildReportPrompt(input);
    input.onEvent({ type: 'stage', stage: 'start', message: 'Preparing OpenClaw non-streaming request...' });
    input.onEvent({
      type: 'stage',
      stage: 'running',
      message: `Running OpenClaw report-agent with stream=false (timeout ${Math.ceil(REPORT_TIMEOUT_MS / 1000)}s)...`,
    });

    const markdown = await this.completeReportPrompt(prompt, input.requestUser);
    if (!markdown) throw new Error('OpenClaw report-agent returned no text.');

    const wholeReportError = this.extractTextError(markdown);
    if (wholeReportError) {
      input.onEvent({
        type: 'stage',
        stage: 'segmenting',
        message: `Whole-report generation failed (${wholeReportError}); retrying as segmented non-streaming generation.`,
      });
      return this.runReportSegmented(input, prompt);
    }

    input.onEvent({ type: 'stage', stage: 'received', message: 'OpenClaw returned a complete non-streaming response.' });
    this.assertNoApprovalCommands(markdown);
    return { markdown, artifacts: {} };
  }

  private async runReportSegmented(input: RunInput, basePrompt: string): Promise<RunResult> {
    const segments = [
      {
        title: '标题、摘要与关键信息',
        prompt: `${basePrompt}\n\n只生成以下部分：标题、摘要、关键信息表。不要输出其他章节。`,
      },
      {
        title: '公开履历与政治背景',
        prompt: `${basePrompt}\n\n只生成以下部分：公开履历、政治背景、关键时间线。不要输出其他章节。`,
      },
      {
        title: '政策立场与风险研判',
        prompt: `${basePrompt}\n\n只生成以下部分：政策立场、涉华/涉外态度、风险研判。不要输出其他章节。`,
      },
      {
        title: '结论、建议、来源与信息缺口',
        prompt: `${basePrompt}\n\n只生成以下部分：结论、工作建议、来源清单、可信度评估、信息缺口。不要输出其他章节。`,
      },
    ];

    const parts: string[] = [];
    for (const [index, segment] of segments.entries()) {
      input.onEvent({
        type: 'stage',
        stage: `segment:${index + 1}`,
        message: `Generating segment ${index + 1}/${segments.length}: ${segment.title}`,
      });
      const text = await this.completeReportPrompt(segment.prompt, input.requestUser);
      const error = this.extractTextError(text);
      if (error) throw new Error(`Segment ${index + 1} failed: ${error}`);
      parts.push(`## ${segment.title}\n\n${text.trim()}`);
    }

    const markdown = parts.join('\n\n---\n\n').trim();
    this.assertNoApprovalCommands(markdown);
    return { markdown, artifacts: {} };
  }

  private async completeReportPrompt(prompt: string, requestUser?: string): Promise<string> {
    const completion = await this.client.chat.completions.create({
      model: OPENCLAW_MODEL,
      stream: false,
      ...(requestUser ? { user: requestUser } : {}),
      messages: [
        {
          role: 'system',
          content: [
            'You are report-agent. Generate rigorous Chinese Markdown reports using public sources only.',
            'All generated Chinese report text must be valid UTF-8 and must not contain Unicode replacement characters such as U+FFFD, consecutive replacement characters, or \\ufffd. Rewrite any damaged sentence before saving.',
            'If the task uses write-hb, operate silently: do not send assistant-visible progress, planning, research notes, summaries, or draft text while using tools.',
            'For write-hb, any assistant message that calls tools must contain no visible text. The final assistant message must contain exactly one REPORT_FILE line.',
          ].join('\n'),
        },
        { role: 'user', content: prompt },
      ],
    });
    return this.extractCompletionText(completion);
  }

  async runReportViaGateway(input: RunInput): Promise<RunResult> {
    const prompt = this.buildReportPrompt(input);
    input.onEvent({ type: 'stage', stage: 'start', message: 'Preparing OpenClaw Gateway device request...' });
    input.onEvent({
      type: 'stage',
      stage: 'running',
      message: `Running OpenClaw report-agent through paired Gateway device (timeout ${Math.ceil(REPORT_TIMEOUT_MS / 1000)}s)...`,
    });

    const agentPayload = await this.gatewayDevice.runAgent({
      agentId: 'report-agent',
      message: prompt,
      timeoutMs: REPORT_TIMEOUT_MS,
      label: this.buildReportLabel(input),
      onEvent: (event) => this.forwardGatewayEvent(event, input.onEvent),
    });

    const markdown = this.extractAgentMarkdown(agentPayload);
    if (!markdown) {
      throw new Error(`OpenClaw report-agent returned no text. Raw payload: ${JSON.stringify(agentPayload).slice(0, 2000)}`);
    }

    const agentError = this.extractAgentError(agentPayload, markdown);
    if (agentError) throw new Error(agentError);

    this.assertNoApprovalCommands(markdown);
    return { markdown, artifacts: {} };
  }

  async streamChat(
    messages: OpenAI.Chat.Completions.ChatCompletionMessageParam[],
    onEvent: (event: ServerEvent) => void,
  ): Promise<string> {
    const stream = await this.client.chat.completions.create({
      model: OPENCLAW_MODEL,
      messages,
      stream: true,
    });

    let text = '';
    const seenTools = new Set<string>();

    for await (const chunk of stream) {
      for (const choice of chunk.choices || []) {
        const delta = choice.delta;
        const content = typeof delta.content === 'string' ? delta.content : '';

        if (content) {
          text += content;
          onEvent({ type: 'text_delta', content });
          onEvent({ type: 'token', content });
        }

        const toolCalls = delta.tool_calls || [];
        for (const toolCall of toolCalls) {
          const id = toolCall.id || `tool-${toolCall.index}`;
          const name = toolCall.function?.name;
          if (!seenTools.has(id)) {
            seenTools.add(id);
            onEvent({ type: 'tool_start', id, name, raw: toolCall });
          }
          onEvent({ type: 'tool_delta', id, name, raw: toolCall });
        }

        if (choice.finish_reason) {
          for (const id of seenTools) {
            onEvent({ type: 'tool_end', id, raw: { finishReason: choice.finish_reason } });
          }
        }
      }
    }

    return text.trim();
  }

  private buildReportPrompt(input: RunInput): string {
    const payloadWithOutput = {
      ...input.payload,
      output_dir: OPENCLAW_CONTAINER_REPORT_DIR,
      output_file_instruction: `如果需要写入文件，请把最终 Markdown 报告保存到 ${OPENCLAW_CONTAINER_REPORT_DIR}。`,
    };

    const yamlPayload = Object.entries(payloadWithOutput)
      .map(([key, value]) => {
        if (Array.isArray(value)) {
          return `${key}:\n${value.map((item) => `  - ${String(item)}`).join('\n')}`;
        }
        return `${key}: ${String(value)}`;
      })
      .join('\n');

    const skillLabel = this.getSkillLabel(input);
    const extraRequirements = this.getSkillRequirements(input);

    return [
      `请使用 OpenClaw Skill: ${input.skill} 生成${skillLabel}。`,
      '',
      '输入参数如下：',
      '```yaml',
      yamlPayload,
      '```',
      '',
      '要求：',
      '1. 严格按照对应 Skill 的工作流执行。',
      '2. 仅使用公开来源，严禁编造事实。',
      '3. 输出完整 Markdown 报告。',
      '4. 报告末尾列出来源、可信度和信息缺口。',
      `5. 如需写入文件，只能写入目录：${OPENCLAW_CONTAINER_REPORT_DIR}。`,
      ...extraRequirements,
    ].join('\n');
  }

  private getSkillLabel(input: RunInput): string {
    if (input.skill === 'risk-assessment-reports') return '风险评估报告';
    if (input.skill === 'person-intelligence-report') return '人物情报报告';
    if (input.skill === 'write-hb') {
      const reportType = typeof input.payload.report_type === 'string' ? input.payload.report_type : 'K报/HB报';
      return `${reportType}现场调研报告`;
    }
    return '报告';
  }

  private getSkillRequirements(input: RunInput): string[] {
    if (input.skill !== 'write-hb') return [];

    const reportType = typeof input.payload.report_type === 'string' ? input.payload.report_type : 'K报或HB报';
    return [
      `6. write-hb 的 report_type 为 ${reportType}，必须按该报种对应大纲撰写，不要混用 K报 与 HB报 结构。`,
      '7. Tavily 搜索必须通过 exec 调用容器内脚本：node /home/node/.openclaw/workspace/skills/tavily-search/scripts/search.mjs。',
      '8. 正文提取必须通过 exec 调用容器内脚本：node /home/node/.openclaw/workspace/skills/tavily-search/scripts/extract.mjs。',
      `9. 必须把完整成稿 Markdown 写入 ${OPENCLAW_CONTAINER_REPORT_DIR} 下的 .md 文件；不要只在对话中输出正文。`,
      `10. 静默执行：调研、检索、提取、规划、草稿、进度说明都不要发送到对话；不要输出“任务已启动”“正在检索”“获取了足够素材”等中间文本。`,
      `11. 最终对话只输出一行：REPORT_FILE: ${OPENCLAW_CONTAINER_REPORT_DIR}/实际文件名.md。除这一行外不要输出摘要、正文、来源表或其他说明。`,
      '12. 最终保存的 Markdown 正文、标题、来源、文件名均不得包含 Unicode 替换字符 U+FFFD、连续替换字符、\\ufffd 或明显乱码；如素材中有乱码，必须改写为语义完整的中文句子后再保存。',
    ];
  }

  private buildReportLabel(input: RunInput): string {
    const name =
      typeof input.payload.target_name === 'string'
        ? input.payload.target_name
        : typeof input.payload.targetName === 'string'
          ? input.payload.targetName
          : typeof input.payload.subject === 'string'
            ? input.payload.subject
            : typeof input.payload.topic === 'string'
              ? input.payload.topic
              : undefined;
    return name ? `${input.skill}: ${name}` : input.skill;
  }

  private extractCompletionText(completion: OpenAI.Chat.Completions.ChatCompletion): string {
    return completion.choices
      .map((choice) => {
        const content = choice.message?.content;
        if (typeof content === 'string') return content;
        return '';
      })
      .join('\n\n')
      .trim();
  }

  private forwardGatewayEvent(
    event: { type: string; payload: unknown },
    onEvent: (event: ServerEvent) => void,
  ) {
    const payload = event.payload && typeof event.payload === 'object' ? (event.payload as Record<string, unknown>) : {};
    if (event.type !== 'agent.stream') return;

    const stream = typeof payload.stream === 'string' ? payload.stream : '';
    const data = payload.data && typeof payload.data === 'object' ? (payload.data as Record<string, unknown>) : {};

    if (stream === 'tool') {
      const phase = typeof data.phase === 'string' ? data.phase : '';
      const id = typeof data.toolCallId === 'string' ? data.toolCallId : undefined;
      const name = typeof data.name === 'string' ? data.name : undefined;
      if (phase === 'start') onEvent({ type: 'tool_start', id, name, raw: data });
      else if (phase === 'result') onEvent({ type: 'tool_end', id, name, raw: data });
      else onEvent({ type: 'tool_delta', id, name, raw: data });
    }

    if (stream === 'lifecycle') {
      const phase = typeof data.phase === 'string' ? data.phase : '';
      if (phase) onEvent({ type: 'stage', stage: `openclaw:${phase}`, message: `OpenClaw ${phase}` });
    }
  }

  private extractAgentMarkdown(payload: unknown): string {
    const root = payload && typeof payload === 'object' ? (payload as Record<string, unknown>) : {};
    const result = root.result && typeof root.result === 'object' ? (root.result as Record<string, unknown>) : root;
    const meta = result.meta && typeof result.meta === 'object' ? (result.meta as Record<string, unknown>) : undefined;
    const fromMeta =
      typeof meta?.finalAssistantVisibleText === 'string'
        ? meta.finalAssistantVisibleText
        : typeof meta?.finalAssistantRawText === 'string'
          ? meta.finalAssistantRawText
          : '';
    const payloads = Array.isArray(result.payloads) ? result.payloads : [];
    const fromPayloads = payloads
      .map((item) => (item && typeof item === 'object' ? (item as Record<string, unknown>).text : undefined))
      .filter((text): text is string => typeof text === 'string' && text.trim().length > 0)
      .join('\n\n');
    return (fromPayloads || fromMeta).trim();
  }

  private extractAgentError(payload: unknown, markdown: string): string | null {
    const root = payload && typeof payload === 'object' ? (payload as Record<string, unknown>) : {};
    const result = root.result && typeof root.result === 'object' ? (root.result as Record<string, unknown>) : root;
    const meta = result.meta && typeof result.meta === 'object' ? (result.meta as Record<string, unknown>) : undefined;
    const stopReason = typeof meta?.stopReason === 'string' ? meta.stopReason : '';
    const embeddedRunError = typeof meta?.embeddedRunError === 'string' ? meta.embeddedRunError : '';
    const trimmed = markdown.trim();

    if (stopReason === 'error' || embeddedRunError) {
      return `OpenClaw report-agent failed: ${embeddedRunError || trimmed.slice(0, 300)}`;
    }

    const textError = this.extractTextError(trimmed);
    if (textError) return `OpenClaw report-agent failed: ${textError}`;

    return null;
  }

  private extractTextError(text: string): string | null {
    const trimmed = text.trim();
    if (!trimmed.startsWith('{')) return null;

    try {
      const parsed = JSON.parse(trimmed.split('\n')[0]) as { error?: unknown };
      return typeof parsed.error === 'string' && parsed.error ? parsed.error : null;
    } catch {
      return null;
    }
  }

  private assertNoApprovalCommands(text: string): void {
    const approvalCommands = this.extractApprovalCommands(text);
    if (approvalCommands.length > 0) {
      throw new OpenClawApprovalRequiredError(approvalCommands, text);
    }
  }

  private extractApprovalCommands(text: string): string[] {
    const commands = new Set<string>();
    const pattern = /\/approve\s+[a-zA-Z0-9_-]+\s+(?:allow-once|allow-always|deny)/g;
    for (const match of text.matchAll(pattern)) {
      commands.add(match[0]);
    }
    return Array.from(commands);
  }
}
