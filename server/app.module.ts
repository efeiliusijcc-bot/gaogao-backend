import { Module } from '@nestjs/common';
import { ChatController } from './chat.controller.js';
import { ChatService } from './chat.service.js';
import { HealthController } from './health.controller.js';
import { OpenClawGatewayDeviceService } from './openclaw-gateway-device.service.js';
import { OpenClawService } from './openclaw.service.js';
import { RemoteFileService } from './remote-file.service.js';
import { ReportsController } from './reports.controller.js';
import { ReportsService } from './reports.service.js';

@Module({
  controllers: [HealthController, ReportsController, ChatController],
  providers: [OpenClawService, OpenClawGatewayDeviceService, RemoteFileService, ReportsService, ChatService],
})
export class AppModule {}
