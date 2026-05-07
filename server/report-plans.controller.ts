import { Body, Controller, HttpException, HttpStatus, Post } from '@nestjs/common';
import { OpenClawService } from './openclaw.service.js';
import type { ReportPlanRequest } from './types.js';

@Controller('/api/report-plans')
export class ReportPlansController {
  constructor(private readonly openClaw: OpenClawService) {}

  @Post()
  async create(@Body() body: ReportPlanRequest) {
    if (!body?.topic || !body?.reportType) {
      throw new HttpException({ error: 'Missing topic or reportType' }, HttpStatus.BAD_REQUEST);
    }

    return this.openClaw.planReport({
      topic: String(body.topic).trim(),
      reportType: String(body.reportType).trim(),
      context: typeof body.context === 'string' ? body.context : '',
      parameters: body.parameters && typeof body.parameters === 'object' ? body.parameters : {},
    });
  }
}
