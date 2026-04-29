import { Body, Controller, Get, Header, HttpException, HttpStatus, Param, Post, Query, Sse } from '@nestjs/common';
import { Observable } from 'rxjs';
import { ReportsService } from './reports.service.js';
import type { CreateJobRequest } from '../src/types/report.js';
import type { ServerEvent } from './types.js';

@Controller('/api/report-jobs')
export class ReportsController {
  constructor(private readonly reports: ReportsService) {}

  @Post()
  create(@Body() body: CreateJobRequest) {
    if (!body.skill || !body.payload) {
      throw new HttpException({ error: 'Missing skill or payload' }, HttpStatus.BAD_REQUEST);
    }
    return this.reports.createJob(body);
  }

  @Get()
  list() {
    return this.reports.listJobs();
  }

  @Get(':jobId')
  get(@Param('jobId') jobId: string) {
    const job = this.reports.getJob(jobId);
    if (!job) {
      throw new HttpException({ error: 'Job not found' }, HttpStatus.NOT_FOUND);
    }
    return {
      jobId: job.jobId,
      skill: job.skill,
      payload: job.payload,
      status: job.status,
      stage: job.stage,
      errorMessage: job.errorMessage,
      resultPath: job.resultPath,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
    };
  }

  @Sse(':jobId/events')
  events(@Param('jobId') jobId: string): Observable<MessageEvent> {
    const job = this.reports.getJob(jobId);
    const stream = this.reports.getStream(jobId);

    return new Observable((subscriber) => {
      if (!job) {
        subscriber.next({ data: { type: 'error', message: 'Job not found' } as ServerEvent } as MessageEvent);
        subscriber.complete();
        return undefined;
      }

      for (const event of job.events) {
        subscriber.next({ data: event } as MessageEvent);
      }

      if (job.status === 'succeeded' || job.status === 'failed' || job.status === 'cancelled') {
        subscriber.next({ data: { type: 'done', jobId } as ServerEvent } as MessageEvent);
        subscriber.complete();
        return undefined;
      }

      if (!stream) {
        subscriber.next({
          data: { type: 'error', message: 'Job event stream is unavailable after service restart.' } as ServerEvent,
        } as MessageEvent);
        subscriber.complete();
        return undefined;
      }

      const subscription = stream.subscribe({
        next: (event) => subscriber.next({ data: event } as MessageEvent),
        complete: () => subscriber.complete(),
      });
      return () => subscription.unsubscribe();
    });
  }

  @Get(':jobId/result')
  async result(@Param('jobId') jobId: string) {
    const result = await this.reports.getResultFromDisk(jobId);
    if (result === undefined) {
      throw new HttpException({ error: 'Job not found' }, HttpStatus.NOT_FOUND);
    }
    if (result === null) {
      const job = this.reports.getJob(jobId);
      throw new HttpException({ error: 'Job not completed', status: job?.status }, HttpStatus.CONFLICT);
    }
    return result;
  }

  @Get(':jobId/download')
  @Header('Content-Type', 'text/markdown; charset=utf-8')
  async download(@Param('jobId') jobId: string, @Query('format') format = 'md') {
    const result = await this.reports.getMarkdownFromDisk(jobId);
    if (result === undefined) {
      throw new HttpException({ error: 'Job not found' }, HttpStatus.NOT_FOUND);
    }
    if (result === null || !result.markdown) {
      throw new HttpException({ error: 'Report not ready' }, HttpStatus.CONFLICT);
    }
    void format;
    return result.markdown;
  }
}
