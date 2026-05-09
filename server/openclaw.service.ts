import { Injectable } from '@nestjs/common';
import fs from 'fs';
import OpenAI from 'openai';
import path from 'path';
import {
  HEALTH_TIMEOUT_MS,
  OPENCLAW_API_KEY,
  OPENCLAW_BASE_URL,
  OPENCLAW_CONTAINER_REPORT_DIR,
  OPENCLAW_HEALTH_URL,
  OPENCLAW_MODEL,
  OPENCLAW_STATE_DIR,
  REPORT_TIMEOUT_MS,
  TAVILY_API_KEY,
} from './config.js';
import { OpenClawGatewayDeviceService } from './openclaw-gateway-device.service.js';
import type { OpenClawHealth, ReportPlanRequest, ReportPlanResponse, RunInput, RunResult, ServerEvent } from './types.js';
import type { ReportPlanStepType } from './types.js';

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

  async planReport(input: ReportPlanRequest): Promise<ReportPlanResponse> {
    const fallback = this.buildFallbackPlan(input);
    const searchFindings = await this.searchPlanningSources(fallback.searchQueries.slice(0, 3));
    const prompt = [
      '请为一个中文深度编报任务生成“规划搜索与子任务选择”方案。',
      '只输出严格 JSON，不要输出 Markdown，不要解释。',
      'JSON 字段必须是：title, summary, searchQueries, steps。',
      'steps 每项字段：id, type, title, description, allowMultiple, options。',
      'options 每项字段：id, label, detail, selected。',
      '要求：',
      '1. searchQueries 给出 4-6 个可用于公开信息检索的中文查询词。',
      '2. steps 必须覆盖 source_scope、basic_info_module、analysis_module、output_module 四类；type 只能使用 search_queries、source_scope、basic_info_module、analysis_module、output_module。',
      '3. 每步 3-5 个选项，允许多选，默认选中最重要的 2-3 个。',
      '4. 选项要贴合报类和主题，不要泛泛而谈。',
      '5. 不要包含 URL、密钥、环境变量或长正文。',
      '6. source_scope 用于让用户选择信源范围；basic_info_module 用于选择基本信息正文模块；analysis_module 用于选择研判模块；output_module 用于选择最终输出口径。',
      '',
      `报类：${input.reportType}`,
      `主题：${input.topic}`,
      `补充上下文：${input.context || '无'}`,
      `结构化参数：${JSON.stringify(input.parameters || {})}`,
      `初步公开检索摘要：${searchFindings || '检索暂不可用，请按主题和上下文规划。'}`,
    ].join('\n');

    try {
      const completion = await this.client.chat.completions.create({
        model: OPENCLAW_MODEL,
        stream: false,
        messages: [
          {
            role: 'system',
            content: 'You are a precise report planning assistant. Return compact valid JSON only.',
          },
          { role: 'user', content: prompt },
        ],
      });
      const plan = this.normalizePlanResponse(this.extractCompletionText(completion), fallback);
      return this.isPlanRelevant(input.topic, plan) ? plan : fallback;
    } catch {
      return fallback;
    }
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
    const sessionKey = this.buildGatewaySessionKey(input);
    const seenSessionEvents = new Set<string>();
    const flushSessionEvents = () => this.forwardSessionToolEvents(sessionKey, input.onEvent, seenSessionEvents);
    input.onEvent({ type: 'stage', stage: 'start', message: 'Preparing OpenClaw Gateway device request...' });
    input.onEvent({
      type: 'stage',
      stage: 'running',
      message: `Running OpenClaw report-agent through paired Gateway device (timeout ${Math.ceil(REPORT_TIMEOUT_MS / 1000)}s)...`,
    });

    const pollTimer = setInterval(flushSessionEvents, 2000);
    pollTimer.unref?.();

    let agentPayload: unknown;
    try {
      agentPayload = await this.gatewayDevice.runAgent({
        agentId: 'report-agent',
        message: prompt,
        timeoutMs: REPORT_TIMEOUT_MS,
        sessionKey,
        label: this.buildReportLabel(input),
        onEvent: (event) => this.forwardGatewayEvent(event, input.onEvent),
      });
    } finally {
      clearInterval(pollTimer);
      flushSessionEvents();
    }

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
      .map(([key, value]) => this.formatPromptPayloadValue(key, value))
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

  private formatPromptPayloadValue(key: string, value: unknown): string {
    if (Array.isArray(value)) {
      return `${key}:\n${value.map((item) => `  - ${String(item)}`).join('\n')}`;
    }

    const text = typeof value === 'string' ? value : JSON.stringify(value);
    if (text.includes('\n') || text.trim().startsWith('{') || text.trim().startsWith('[')) {
      return `${key}: |\n${text.split('\n').map((line) => `  ${line}`).join('\n')}`;
    }

    return `${key}: ${text}`;
  }

  private getSkillRequirements(input: RunInput): string[] {
    if (input.skill !== 'write-hb') return [];

    const reportType = typeof input.payload.report_type === 'string' ? input.payload.report_type : 'K报或HB报';
    return [
      `6. write-hb 的 report_type 为 ${reportType}，必须按该报种对应大纲撰写，不要混用 K报 与 HB报 结构。`,
      '7. known_context 如果是 JSON，必须先解析其 selectedSearchQueries、userProvidedSources、selectedModules、parameterValues、supplement；如果解析失败，再按普通文本上下文处理。',
      '8. Research Phase：优先使用 web-research-firecrawl 进行前置研究。若用户提供 userProvidedSources 或 selectedSources，先围绕这些信源/机构/URL 抽取和核验；再围绕 selectedSearchQueries 做公开检索。推荐通过 exec 调用：python3 /home/node/.openclaw/workspace/report-agent/skills/web-research-firecrawl/scripts/research_cli.py brief --query "检索词" --max-sources 8 --instruction "围绕用户选中的编报模块提取证据、关键信息、待核验项"。',
      '9. Research Phase 输出必须形成内部素材：sources、evidence_cards、key_findings、verification_needed 和信息缺口；不要把完整网页正文、长 stdout/stderr 或研究草稿发送到对话。',
      '10. Firecrawl/Exa/Tavily triad 不可用时，允许回退到 write-hb 原 Tavily 调研流程：node /home/node/.openclaw/workspace/skills/tavily-search/scripts/search.mjs 和 extract.mjs，并在内部记录“Firecrawl 不可用，已回退”。',
      '11. Write-HB Phase：只在 Research Phase 完成后，基于前置研究结果和用户 selectedModules，按 write-hb 的 K报/HB报大纲撰写完整成稿；不得把未选模块强行作为正文重点。',
      `12. 必须把完整成稿 Markdown 写入 ${OPENCLAW_CONTAINER_REPORT_DIR} 下的 .md 文件；不要只在对话中输出正文。`,
      `13. 静默执行：调研、检索、提取、规划、草稿、进度说明都不要发送到对话；不要输出“任务已启动”“正在检索”“获取了足够素材”等中间文本。`,
      `14. 最终对话只输出一行：REPORT_FILE: ${OPENCLAW_CONTAINER_REPORT_DIR}/实际文件名.md。除这一行外不要输出摘要、正文、来源表或其他说明。`,
      '15. 最终保存的 Markdown 正文、标题、来源、文件名均不得包含 Unicode 替换字符 U+FFFD、连续替换字符、\\ufffd 或明显乱码；如素材中有乱码，必须改写为语义完整的中文句子后再保存。',
      '16. 正文段落不得出现 http:// 或 https:// 原始网址；正文引用只写来源机构、发布时间和参考资料编号，完整 URL 只放在文末参考资料部分。',
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

  private buildGatewaySessionKey(input: RunInput): string {
    return `agent:report-agent:openai-user:${input.requestUser || cryptoSafeLabel(this.buildReportLabel(input))}`;
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

  private normalizePlanResponse(text: string, fallback: ReportPlanResponse): ReportPlanResponse {
    try {
      const jsonText = text.trim().replace(/^```(?:json)?/i, '').replace(/```$/i, '').trim();
      const parsed = JSON.parse(jsonText) as Partial<ReportPlanResponse>;
      const steps = Array.isArray(parsed.steps)
        ? parsed.steps
            .map((step, stepIndex) => ({
              id: this.safePlanId(step?.id, `step-${stepIndex + 1}`),
              type: this.safePlanStepType((step as { type?: unknown } | undefined)?.type, fallback.steps[stepIndex]?.type),
              title: this.sanitizeText(String(step?.title || fallback.steps[stepIndex]?.title || `步骤 ${stepIndex + 1}`), 40),
              description: this.sanitizeText(String(step?.description || ''), 120),
              allowMultiple: step?.allowMultiple !== false,
              options: Array.isArray(step?.options)
                ? step.options.slice(0, 6).map((option, optionIndex) => ({
                    id: this.safePlanId(option?.id, `option-${optionIndex + 1}`),
                    label: this.sanitizeText(String(option?.label || `选项 ${optionIndex + 1}`), 40),
                    detail: this.sanitizeText(String(option?.detail || ''), 120),
                    selected: typeof option?.selected === 'boolean' ? option.selected : optionIndex < 3,
                  }))
                : [],
            }))
            .filter((step) => step.options.length > 0)
        : [];

      const normalizedSteps = steps.length ? this.ensurePlanStepTypes(steps, fallback.steps) : fallback.steps;

      return {
        title: this.sanitizeText(String(parsed.title || fallback.title), 60),
        summary: this.sanitizeText(String(parsed.summary || fallback.summary), 180),
        searchQueries: Array.isArray(parsed.searchQueries)
          ? parsed.searchQueries.map((item) => this.sanitizeText(String(item), 80)).filter(Boolean).slice(0, 8)
          : fallback.searchQueries,
        steps: normalizedSteps,
      };
    } catch {
      return fallback;
    }
  }

  private ensurePlanStepTypes(steps: ReportPlanResponse['steps'], fallbackSteps: ReportPlanResponse['steps']): ReportPlanResponse['steps'] {
    const required: ReportPlanStepType[] = ['source_scope', 'basic_info_module', 'analysis_module', 'output_module'];
    const result = [...steps];
    for (const type of required) {
      if (!result.some((step) => step.type === type)) {
        const fallback = fallbackSteps.find((step) => step.type === type);
        if (fallback) result.push(fallback);
      }
    }
    return result.slice(0, 6);
  }

  private safePlanId(value: unknown, fallback: string): string {
    const text = typeof value === 'string' ? value : fallback;
    return text.toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || fallback;
  }

  private safePlanStepType(value: unknown, fallback?: ReportPlanStepType): ReportPlanStepType {
    const allowed = new Set<ReportPlanStepType>([
      'search_queries',
      'source_scope',
      'basic_info_module',
      'analysis_module',
      'output_module',
    ]);
    return typeof value === 'string' && allowed.has(value as ReportPlanStepType)
      ? (value as ReportPlanStepType)
      : fallback || 'analysis_module';
  }

  private buildFallbackPlan(input: ReportPlanRequest): ReportPlanResponse {
    const topic = this.sanitizeText(input.topic || '未命名编报', 60);
    const reportType = this.sanitizeText(input.reportType || 'report', 40);
    const keywords = this.extractPlanningKeywords(topic);
    const primaryKeyword = this.buildPrimaryPlanningKeyword(topic, keywords);
    const reportLabel = reportType === 'write-hb-k'
      ? 'K报'
      : reportType === 'write-hb-hb'
        ? 'HB报'
        : reportType === 'person-intelligence-report'
          ? '人物报'
          : reportType === 'risk-assessment-reports'
            ? '风险报'
            : '编报';
    return {
      title: `${topic}：编报规划`,
      summary: `已围绕“${topic}”生成${reportLabel}检索词和研判子任务，请选择需要纳入正式编报的方向。`,
      searchQueries: [
        `${topic} 最新动态`,
        `${topic} 政策背景 影响`,
        `${primaryKeyword} 公开报道 研判`,
        `${topic} 风险 对策`,
        `${topic} 各方立场`,
      ],
      steps: [
        {
          id: 'source-scope',
          type: 'source_scope',
          title: '信源范围',
          description: `选择围绕“${topic}”需要优先检索、抽取和交叉核验的信源类型。`,
          allowMultiple: true,
          options: [
            { id: 'official-sources', label: '官方信源', detail: `优先核验“${topic}”相关政府部门、国际组织、监管机构、法院或议会文件。`, selected: true },
            { id: 'major-media', label: '主流媒体', detail: `补充“${topic}”的一线报道、公开采访、事件进展和各方回应。`, selected: true },
            { id: 'think-tank-research', label: '智库研究', detail: `检索“${topic}”相关智库、研究机构、行业报告和专家分析。`, selected: true },
            { id: 'data-material', label: '数据材料', detail: `补充支撑“${topic}”判断的公开数据、统计口径、案例和图表来源。`, selected: false },
          ],
        },
        {
          id: 'basic-info-module',
          type: 'basic_info_module',
          title: '基本信息模块',
          description: `选择“${topic}”在基本情况部分需要展开的事实模块。`,
          allowMultiple: true,
          options: [
            { id: 'topic-timeline', label: `${primaryKeyword}时间线`, detail: `梳理“${topic}”的起因、关键节点、近期变化和后续触发点。`, selected: true },
            { id: 'policy-source', label: `${primaryKeyword}政策源头`, detail: `检索与“${topic}”直接相关的官方文件、政策表态、监管口径和执行依据。`, selected: true },
            { id: 'actor-network', label: `${primaryKeyword}相关方`, detail: `识别“${topic}”涉及的国家、机构、企业、行业、人物及其利益关系。`, selected: true },
            { id: 'similar-cases', label: '相似案例', detail: `补充与“${topic}”可比照的历史案例、国际惯例和关联事件。`, selected: false },
          ],
        },
        {
          id: 'analysis-focus',
          type: 'analysis_module',
          title: '研判重点',
          description: `选择“${topic}”在正文中需要重点展开的判断角度。`,
          allowMultiple: true,
          options: [
            { id: 'core-risk', label: `${primaryKeyword}风险识别`, detail: `研判“${topic}”可能带来的政治、经济、产业、安全、舆情或外溢风险。`, selected: true },
            { id: 'china-impact', label: '涉我影响', detail: `分析“${topic}”对我方利益、企业、产业链、地区合作或政策空间的影响。`, selected: true },
            { id: 'trend-scenario', label: `${primaryKeyword}趋势推演`, detail: `围绕“${topic}”设计短期、中期可能走向及关键变量。`, selected: true },
            { id: 'uncertainty-gap', label: '待核验信息', detail: `标注“${topic}”中信源不足、口径冲突或需要继续跟踪的事实缺口。`, selected: false },
          ],
        },
        {
          id: 'output-tone',
          type: 'output_module',
          title: '输出口径',
          description: `选择“${topic}”最终${reportLabel}的写作侧重和成果形式。`,
          allowMultiple: true,
          options: [
            { id: 'structured', label: `${reportLabel}结构`, detail: reportType.includes('write-hb') ? '按基本情况、涉我风险、对策建议组织，突出现场调研和决策参考。' : '按背景、风险、影响、建议组织，突出条理和可读性。', selected: true },
            { id: 'briefing', label: '结论先行', detail: `围绕“${topic}”先给出核心判断，再展开事实依据和风险说明。`, selected: true },
            { id: 'evidence', label: '来源可追溯', detail: `对“${topic}”相关事实保留来源机构、时间和可信度提示，避免堆砌网址。`, selected: true },
            { id: 'action', label: '对策建议', detail: `提出针对“${topic}”的后续跟踪、风险防范和处置建议。`, selected: false },
          ],
        },
      ],
    };
  }

  private isPlanRelevant(topic: string, plan: ReportPlanResponse): boolean {
    const terms = this.extractPlanningKeywords(topic);
    if (terms.length === 0) return true;

    const haystack = [
      plan.title,
      plan.summary,
      ...(plan.searchQueries || []),
      ...(plan.steps || []).flatMap((step) => [
        step.title,
        step.description,
        ...(step.options || []).flatMap((option) => [option.label, option.detail]),
      ]),
    ].join('\n');

    return terms.some((term) => haystack.includes(term)) || haystack.includes(String(topic).trim());
  }

  private extractPlanningKeywords(topic: string): string[] {
    const text = this.sanitizeText(String(topic || ''), 80);
    const stopWords = new Set([
      '方面',
      '情报',
      '研判',
      '报告',
      '编报',
      '关于',
      '有关',
      '情况',
      '事件',
      '影响',
      '风险',
      '最新',
    ]);
    const matches = text.match(/[\p{Script=Han}A-Za-z0-9]{2,}/gu) || [];
    const terms = new Set<string>();

    for (const match of matches) {
      if (!stopWords.has(match)) terms.add(match);
      const chineseParts = match.match(/[\p{Script=Han}]{2,}/gu) || [];
      for (const part of chineseParts) {
        if (part.length > 6) {
          for (let index = 0; index <= part.length - 2; index += 2) {
            const token = part.slice(index, Math.min(index + 4, part.length));
            if (token.length >= 2 && !stopWords.has(token)) terms.add(token);
          }
        }
      }
    }

    return Array.from(terms).slice(0, 8);
  }

  private buildPrimaryPlanningKeyword(topic: string, keywords: string[]): string {
    const normalized = this
      .sanitizeText(topic, 40)
      .replace(/方面/g, '')
      .replace(/情报|研判|报告|编报|情况|事件/g, '')
      .replace(/的$/g, '')
      .trim();
    if (normalized.length >= 2 && normalized.length <= 14) return normalized;
    const compact = keywords.find((keyword) => keyword.length >= 2 && keyword.length <= 10);
    return compact || normalized.slice(0, 12) || topic.slice(0, 12);
  }

  private async searchPlanningSources(queries: string[]): Promise<string> {
    if (!TAVILY_API_KEY || queries.length === 0) return '';
    const findings: string[] = [];

    for (const query of queries) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 8000);
      try {
        const response = await fetch('https://api.tavily.com/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            api_key: TAVILY_API_KEY,
            query,
            search_depth: 'basic',
            max_results: 3,
            include_answer: true,
          }),
        });
        clearTimeout(timer);
        if (!response.ok) continue;
        const data = (await response.json()) as {
          answer?: unknown;
          results?: Array<{ title?: unknown; content?: unknown }>;
        };
        const lines = [
          typeof data.answer === 'string' ? data.answer : '',
          ...(Array.isArray(data.results)
            ? data.results.map((item) => `${String(item.title || '')} ${String(item.content || '')}`)
            : []),
        ]
          .map((item) => this.sanitizeText(item, 180))
          .filter(Boolean)
          .slice(0, 3);
        if (lines.length) findings.push(`查询：${query}\n${lines.map((line) => `- ${line}`).join('\n')}`);
      } catch {
        clearTimeout(timer);
      }
    }

    return findings.join('\n\n').slice(0, 2400);
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
      const id =
        typeof data.toolCallId === 'string'
          ? data.toolCallId
          : typeof data.id === 'string'
            ? data.id
            : undefined;
      const name = this.extractToolName(data);
      const summary = this.summarizeToolEvent(data);
      const raw = {
        phase,
        status: summary.status,
        label: summary.label,
        summary: summary.summary,
        command: summary.command,
      };
      if (summary.status === 'failed') onEvent({ type: 'tool_error', id, name, message: summary.summary, raw });
      else if (phase === 'start' || phase === 'call') onEvent({ type: 'tool_start', id, name, raw });
      else if (phase === 'result' || phase === 'output' || phase === 'end' || phase === 'complete') onEvent({ type: 'tool_end', id, name, raw });
      else onEvent({ type: 'tool_delta', id, name, raw });
    }

    if (stream === 'lifecycle') {
      const phase = typeof data.phase === 'string' ? data.phase : '';
      const message = this.summarizeLifecycleEvent(phase, data);
      if (phase) onEvent({ type: 'stage', stage: `openclaw:${phase}`, message });
    }
  }

  private forwardSessionToolEvents(
    sessionKey: string,
    onEvent: (event: ServerEvent) => void,
    seen: Set<string>,
  ): void {
    const jsonlPath = this.resolveSessionJsonlPath(sessionKey);
    if (!jsonlPath) return;

    const toolCalls = new Map<string, { name: string; args: Record<string, unknown>; emittedStart: boolean }>();
    const lines = fs.readFileSync(jsonlPath, 'utf8').split(/\r?\n/).filter(Boolean);

    for (const line of lines) {
      const item = this.parseJsonLine(line);
      const message = item?.message && typeof item.message === 'object' ? (item.message as Record<string, unknown>) : undefined;
      if (!message) continue;

      if (message.role === 'assistant' && Array.isArray(message.content)) {
        for (const content of message.content) {
          if (!content || typeof content !== 'object') continue;
          const call = content as Record<string, unknown>;
          if (call.type !== 'toolCall') continue;
          const id = typeof call.id === 'string' ? call.id : `${String(item?.id || 'tool')}-${toolCalls.size}`;
          const name = typeof call.name === 'string' ? call.name : 'tool';
          const args = this.parseMaybeObject(call.arguments) || {};
          toolCalls.set(id, { name, args, emittedStart: false });
          if (!/read/i.test(name)) this.emitSessionToolStart(id, name, args, onEvent, seen, toolCalls);
        }
      }

      if (message.role === 'toolResult') {
        const id = typeof message.toolCallId === 'string' ? message.toolCallId : '';
        const stored = toolCalls.get(id);
        const name = typeof message.toolName === 'string' ? message.toolName : stored?.name || 'tool';
        const args = stored?.args || {};
        if (/read/i.test(name) && !stored?.emittedStart) {
          const range = this.inferReadRangeFromToolResult(message);
          this.emitSessionToolStart(id, name, { ...args, ...(range ? { startLine: range.start, endLine: range.end } : {}) }, onEvent, seen, toolCalls);
        }
        this.emitSessionToolEnd(id, name, onEvent, seen);
      }
    }
  }

  private resolveSessionJsonlPath(sessionKey: string): string | null {
    try {
      const sessionsPath = path.join(OPENCLAW_STATE_DIR, 'agents', 'report-agent', 'sessions', 'sessions.json');
      const sessions = JSON.parse(fs.readFileSync(sessionsPath, 'utf8')) as Record<string, { sessionId?: unknown }>;
      const sessionId = sessions[sessionKey]?.sessionId;
      if (typeof sessionId !== 'string' || !sessionId) return null;
      const jsonlPath = path.join(OPENCLAW_STATE_DIR, 'agents', 'report-agent', 'sessions', `${sessionId}.jsonl`);
      return fs.existsSync(jsonlPath) ? jsonlPath : null;
    } catch {
      return null;
    }
  }

  private parseJsonLine(line: string): Record<string, unknown> | null {
    try {
      const parsed = JSON.parse(line) as unknown;
      return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
    } catch {
      return null;
    }
  }

  private emitSessionToolStart(
    id: string,
    name: string,
    args: Record<string, unknown>,
    onEvent: (event: ServerEvent) => void,
    seen: Set<string>,
    toolCalls: Map<string, { name: string; args: Record<string, unknown>; emittedStart: boolean }>,
  ): void {
    const key = `start:${id}`;
    if (seen.has(key)) return;
    seen.add(key);
    const stored = toolCalls.get(id);
    if (stored) stored.emittedStart = true;
    const data = { phase: 'call', name, arguments: args };
    const summary = this.summarizeToolEvent(data);
    onEvent({
      type: 'tool_start',
      id,
      name,
      raw: {
        phase: 'call',
        status: 'started',
        label: summary.label,
        summary: summary.summary,
        command: summary.command,
      },
    });
  }

  private emitSessionToolEnd(
    id: string,
    name: string,
    onEvent: (event: ServerEvent) => void,
    seen: Set<string>,
  ): void {
    const key = `end:${id}`;
    if (seen.has(key)) return;
    seen.add(key);
    const summary = this.summarizeToolEvent({ phase: 'output', name });
    onEvent({
      type: 'tool_end',
      id,
      name,
      raw: {
        phase: 'output',
        status: 'completed',
        label: summary.label,
        summary: summary.summary,
        command: '',
      },
    });
  }

  private inferReadRangeFromToolResult(message: Record<string, unknown>): { start: number; end: number } | null {
    const content = Array.isArray(message.content) ? message.content : [];
    const text = content
      .map((item) => (item && typeof item === 'object' ? (item as Record<string, unknown>).text : undefined))
      .filter((value): value is string => typeof value === 'string')
      .join('\n');
    if (!text) return null;
    const lineCount = text.split(/\r?\n/).length;
    return { start: 1, end: Math.min(50, lineCount) };
  }

  private extractToolName(data: Record<string, unknown>): string | undefined {
    const direct = typeof data.name === 'string' ? data.name : '';
    const toolName = typeof data.toolName === 'string' ? data.toolName : '';
    const type = typeof data.type === 'string' ? data.type : '';
    const command = this.extractCommand(data);
    const candidate = direct || toolName || type;
    if (candidate) return candidate;
    if (/search\.mjs/i.test(command)) return 'tavily-search';
    if (/extract\.mjs/i.test(command)) return 'tavily-extract';
    if (this.extractReadPath(data)) return 'read';
    if (this.extractWritePath(data)) return 'write';
    if (/\bnode\b|\bpython\b|\bbash\b|\bsh\b/i.test(command)) return 'exec';
    return undefined;
  }

  private summarizeToolEvent(data: Record<string, unknown>) {
    const phase = typeof data.phase === 'string' ? data.phase : '';
    const name = this.extractToolName(data) || 'tool';
    const detail = this.describeToolCall(name, data);
    const command = this.sanitizeText(detail || this.extractCommand(data), 220);
    const output = this.extractOutputText(data);
    const status = this.detectToolStatus(data, phase, output);
    const label = this.labelTool(name, command);
    const summary = this.buildToolSummary(name, phase, status, command, output, detail);
    return { status, label, command, summary };
  }

  private summarizeLifecycleEvent(phase: string, data: Record<string, unknown>): string {
    const message = this.firstString(data, ['message', 'status', 'label']);
    if (message) return this.sanitizeText(message, 180);
    if (phase === 'start') return 'OpenClaw started the agent run.';
    if (phase === 'complete' || phase === 'done') return 'OpenClaw completed the agent run.';
    if (phase === 'error') return 'OpenClaw reported an agent run error.';
    return `OpenClaw ${phase}`;
  }

  private detectToolStatus(data: Record<string, unknown>, phase: string, output: string): 'started' | 'completed' | 'failed' | 'running' {
    const status = this.firstString(data, ['status', 'state']);
    const error = this.firstString(data, ['error', 'message']);
    if (/fail|error|rejected/i.test(status) || (phase === 'error') || /error|failed|missing/i.test(error)) return 'failed';
    if (/error|failed|missing api key|unauthorized/i.test(output)) return 'failed';
    if (phase === 'start' || phase === 'call') return 'started';
    if (phase === 'result' || phase === 'output' || phase === 'end' || phase === 'complete') return 'completed';
    return 'running';
  }

  private buildToolSummary(name: string, phase: string, status: string, command: string, output: string, detail = ''): string {
    if (status === 'started') {
      if (detail) return detail;
      if (/search\.mjs/i.test(command)) return `Searching public sources${this.extractQuotedQuery(command)}.`;
      if (/extract\.mjs/i.test(command)) return 'Extracting selected source pages.';
      if (/write/i.test(name)) return 'Writing the report file.';
      if (/read/i.test(name)) return 'Reading generated report artifacts.';
      return command ? `Running ${this.labelTool(name, command)}.` : `Starting ${this.labelTool(name, command)}.`;
    }

    if (status === 'failed') {
      const text = output || this.extractFailureHint(command) || `${this.labelTool(name, command)} failed.`;
      return this.sanitizeText(text, 220);
    }

    if (/read/i.test(name)) return 'Read completed.';
    if (/write/i.test(name)) return 'Write completed.';
    if (/exec/i.test(name)) return 'Command completed.';
    if (/search\.mjs/i.test(command) || /tavily-search/i.test(name)) {
      const count = this.countSearchResults(output);
      return count ? `Search completed with ${count} candidate sources.` : 'Search completed; candidate sources were returned.';
    }
    if (/extract\.mjs/i.test(command) || /tavily-extract/i.test(name)) {
      const failures = (output.match(/failed/gi) || []).length;
      return failures ? `Extraction completed with ${failures} failed URL(s); usable content was retained.` : 'Source extraction completed.';
    }
    if (phase) return `${this.labelTool(name, command)} ${status}.`;
    return this.sanitizeText(output || `${this.labelTool(name, command)} completed.`, 220);
  }

  private labelTool(name: string, command: string): string {
    if (/search\.mjs/i.test(command) || /tavily-search/i.test(name)) return 'Tavily Search';
    if (/extract\.mjs/i.test(command) || /tavily-extract/i.test(name)) return 'Tavily Extract';
    if (/write/i.test(name)) return 'Write';
    if (/read/i.test(name)) return 'Read';
    if (/exec/i.test(name)) return 'Exec';
    return name;
  }

  private describeToolCall(name: string, data: Record<string, unknown>): string {
    if (/read/i.test(name)) {
      const filePath = this.extractReadPath(data);
      const range = this.extractReadRange(data);
      return filePath ? `${range ? `with ${range} from ` : 'from '}${this.sanitizePathForLog(filePath)}` : '';
    }

    if (/write/i.test(name)) {
      const filePath = this.extractWritePath(data);
      return filePath ? `to ${this.sanitizePathForLog(filePath)}` : '';
    }

    const command = this.extractCommand(data);
    if (command) return this.sanitizeText(command, 220);

    const args = this.extractToolArgs(data);
    if (Object.keys(args).length === 0) return '';
    return this.sanitizeText(JSON.stringify(this.sanitizeToolArgs(args)), 220);
  }

  private extractReadPath(data: Record<string, unknown>): string {
    const args = this.extractToolArgs(data);
    return this.firstString(args, ['path', 'file', 'filepath', 'filePath', 'target', 'uri']);
  }

  private extractWritePath(data: Record<string, unknown>): string {
    const args = this.extractToolArgs(data);
    return this.firstString(args, ['path', 'file', 'filepath', 'filePath', 'target', 'uri', 'output', 'outputPath']);
  }

  private extractReadRange(data: Record<string, unknown>): string {
    const args = this.extractToolArgs(data);
    const start = this.firstNumber(args, ['start', 'lineStart', 'startLine', 'from', 'offset']);
    const end = this.firstNumber(args, ['end', 'lineEnd', 'endLine', 'to', 'limit']);
    if (start !== undefined && end !== undefined) return `lines ${start}-${end}`;
    if (start !== undefined) return `from line ${start}`;
    if (end !== undefined) return `first ${end} lines`;
    return '';
  }

  private extractToolArgs(data: Record<string, unknown>): Record<string, unknown> {
    const keys = ['params', 'arguments', 'args', 'input', 'request'];
    for (const key of keys) {
      const value = data[key];
      const parsed = this.parseMaybeObject(value);
      if (parsed) return parsed;
    }
    const toolCall = data.toolCall && typeof data.toolCall === 'object' ? (data.toolCall as Record<string, unknown>) : undefined;
    if (toolCall) {
      for (const key of keys) {
        const parsed = this.parseMaybeObject(toolCall[key]);
        if (parsed) return parsed;
      }
      const fn = toolCall.function && typeof toolCall.function === 'object' ? (toolCall.function as Record<string, unknown>) : undefined;
      const parsed = fn ? this.parseMaybeObject(fn.arguments) : undefined;
      if (parsed) return parsed;
    }
    return {};
  }

  private parseMaybeObject(value: unknown): Record<string, unknown> | undefined {
    if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, unknown>;
    if (typeof value !== 'string' || !value.trim()) return undefined;
    const text = value.trim();
    if (!text.startsWith('{')) return undefined;
    try {
      const parsed = JSON.parse(text) as unknown;
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : undefined;
    } catch {
      return undefined;
    }
  }

  private firstNumber(data: Record<string, unknown>, keys: string[]): number | undefined {
    for (const key of keys) {
      const value = data[key];
      if (typeof value === 'number' && Number.isFinite(value)) return value;
      if (typeof value === 'string' && /^\d+$/.test(value.trim())) return Number(value.trim());
    }
    return undefined;
  }

  private sanitizeToolArgs(args: Record<string, unknown>): Record<string, unknown> {
    const sanitized: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(args)) {
      if (/key|token|secret|password|authorization/i.test(key)) {
        sanitized[key] = '<redacted>';
      } else if (typeof value === 'string') {
        sanitized[key] = this.sanitizeText(value, 120);
      } else if (typeof value === 'number' || typeof value === 'boolean') {
        sanitized[key] = value;
      }
    }
    return sanitized;
  }

  private sanitizePathForLog(filePath: string): string {
    const clean = filePath.replace(/\\/g, '/');
    if (clean.startsWith('/home/node/.openclaw/workspace/')) return clean;
    if (clean.startsWith('/home/node/.openclaw/')) return clean.replace(/^\/home\/node\/\.openclaw\/[^\s]+/, '<openclaw-path>');
    if (clean.startsWith('/usr/docker/openclaw/')) return clean.replace(/^\/usr\/docker\/openclaw\/[^\s]+/, '<openclaw-host-path>');
    return this.sanitizeText(clean, 180);
  }

  private countSearchResults(output: string): number {
    const markdownItems = output.match(/^\s*-\s+\*\*/gm)?.length ?? 0;
    if (markdownItems) return markdownItems;
    const urls = output.match(/https?:\/\/\S+/g)?.length ?? 0;
    return urls;
  }

  private extractQuotedQuery(command: string): string {
    const match = command.match(/search\.mjs\s+"([^"]+)"/i) || command.match(/search\.mjs\s+'([^']+)'/i);
    return match?.[1] ? ` for "${this.sanitizeText(match[1], 60)}"` : '';
  }

  private extractFailureHint(command: string): string {
    if (/TAVILY_API_KEY/i.test(command)) return 'Tavily API key is missing.';
    return '';
  }

  private extractCommand(data: Record<string, unknown>): string {
    const command = this.firstString(data, ['command', 'cmd', 'input', 'args']);
    if (command) return command;
    const params = data.params && typeof data.params === 'object' ? (data.params as Record<string, unknown>) : undefined;
    return params ? this.firstString(params, ['command', 'cmd', 'input', 'args']) : '';
  }

  private extractOutputText(data: Record<string, unknown>): string {
    const result = data.result && typeof data.result === 'object' ? (data.result as Record<string, unknown>) : undefined;
    const output =
      this.firstString(data, ['summary', 'output', 'stdout', 'stderr', 'content', 'text', 'message', 'error']) ||
      (result ? this.firstString(result, ['summary', 'output', 'stdout', 'stderr', 'content', 'text', 'message', 'error']) : '');
    return this.sanitizeText(output, 600);
  }

  private firstString(data: Record<string, unknown>, keys: string[]): string {
    for (const key of keys) {
      const value = data[key];
      if (typeof value === 'string' && value.trim()) return value.trim();
      if (Array.isArray(value)) {
        const joined = value.filter((item) => typeof item === 'string').join(' ');
        if (joined.trim()) return joined.trim();
      }
    }
    return '';
  }

  private sanitizeText(value: string, maxLength: number): string {
    const redacted = value
      .replace(/(api[_-]?key|token|secret|authorization|password)\s*[:=]\s*["']?[^"'\s,}]+/gi, '$1=<redacted>')
      .replace(/Bearer\s+[A-Za-z0-9._~+/-]+=*/gi, 'Bearer <redacted>')
      .replace(/\/home\/node\/\.openclaw\/workspace\/[^\s"'`]+/g, '<openclaw-workspace-path>')
      .replace(/\/usr\/docker\/openclaw\/[^\s"'`]+/g, '<openclaw-host-path>')
      .replace(/\s+/g, ' ')
      .trim();
    return redacted.length > maxLength ? `${redacted.slice(0, maxLength - 1)}…` : redacted;
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

function cryptoSafeLabel(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'report';
}
