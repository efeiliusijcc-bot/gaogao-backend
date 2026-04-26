import { Controller, Get, HttpCode, Res } from '@nestjs/common';
import type { Response } from 'express';
import { OpenClawGatewayDeviceService } from './openclaw-gateway-device.service.js';
import { OpenClawService } from './openclaw.service.js';

@Controller('/api/openclaw')
export class HealthController {
  constructor(
    private readonly openClaw: OpenClawService,
    private readonly gatewayDevice: OpenClawGatewayDeviceService,
  ) {}

  @Get('health')
  @HttpCode(200)
  async health(@Res() res: Response) {
    const health = await this.openClaw.health();
    res.status(health.ok ? 200 : 503).json(health);
  }

  @Get('device-health')
  @HttpCode(200)
  async deviceHealth(@Res() res: Response) {
    const health = await this.gatewayDevice.health();
    res.status(health.ok ? 200 : 503).json(health);
  }
}
