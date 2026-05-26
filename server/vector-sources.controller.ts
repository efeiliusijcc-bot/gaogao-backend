import { Body, Controller, Get, Post } from '@nestjs/common';
import { VectorSourceService } from './vector-source.service.js';

@Controller('/api/vector-sources')
export class VectorSourcesController {
  constructor(private readonly vectorSources: VectorSourceService) {}

  @Get('status')
  status() {
    return this.vectorSources.status();
  }

  @Post('reindex')
  reindex(@Body() body: { limit?: number } = {}) {
    return this.vectorSources.reindex(Number(body?.limit || 100));
  }
}
