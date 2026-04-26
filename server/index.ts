import 'reflect-metadata';
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import {
  HEALTH_TIMEOUT_MS,
  OPENCLAW_BASE_URL,
  OPENCLAW_HEALTH_URL,
  OPENCLAW_MODEL,
  REPORT_TIMEOUT_MS,
} from './config.js';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn', 'log'] });
  app.enableCors();

  const port = Number(process.env.PORT || 3001);
  await app.listen(port);

  console.log(`Report API server running on http://localhost:${port}`);
  console.log(`OpenClaw HTTP base URL: ${OPENCLAW_BASE_URL}`);
  console.log(`OpenClaw health URL: ${OPENCLAW_HEALTH_URL}`);
  console.log(`OpenClaw model/agent: ${OPENCLAW_MODEL}`);
  console.log(`OpenClaw timeouts: health=${HEALTH_TIMEOUT_MS}ms, run=${REPORT_TIMEOUT_MS}ms`);
}

void bootstrap();
