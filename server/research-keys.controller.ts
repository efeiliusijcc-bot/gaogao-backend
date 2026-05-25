import { Body, Controller, Get, Put } from '@nestjs/common';
import { ResearchKeysService, type UpdateResearchKeysInput } from './research-keys.service.js';

@Controller('/api/research-keys')
export class ResearchKeysController {
  constructor(private readonly researchKeys: ResearchKeysService) {}

  @Get()
  getStatus() {
    return this.researchKeys.getStatus();
  }

  @Put()
  update(@Body() body: UpdateResearchKeysInput) {
    return this.researchKeys.updateKeys(body || {});
  }
}
