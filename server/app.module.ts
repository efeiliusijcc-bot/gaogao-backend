import { Module } from '@nestjs/common';
import { ChatController } from './chat.controller.js';
import { ChatService } from './chat.service.js';
import { HealthController } from './health.controller.js';
import { OpenClawGatewayDeviceService } from './openclaw-gateway-device.service.js';
import { OpenClawService } from './openclaw.service.js';
import { RemoteFileService } from './remote-file.service.js';
import { ReportPlansController } from './report-plans.controller.js';
import { ResearchKeysController } from './research-keys.controller.js';
import { ResearchKeysService } from './research-keys.service.js';
import { ReportsController } from './reports.controller.js';
import { ReportsService } from './reports.service.js';
import { VectorSourcesController } from './vector-sources.controller.js';
import { VectorSourceService } from './vector-source.service.js';

@Module({
  controllers: [HealthController, ReportsController, ReportPlansController, ResearchKeysController, VectorSourcesController, ChatController],
  providers: [OpenClawService, OpenClawGatewayDeviceService, RemoteFileService, ReportsService, ResearchKeysService, VectorSourceService, ChatService],
})
export class AppModule {}
