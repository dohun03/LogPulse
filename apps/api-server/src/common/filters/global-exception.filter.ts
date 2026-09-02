import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';

import { FastifyReply, FastifyRequest } from 'fastify';

@Catch()
export class GlobalExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const reply = ctx.getResponse<FastifyReply>();
    const request = ctx.getRequest<FastifyRequest>();

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let errorCode = 'INTERNAL_ERROR';
    let message = '서버 내부 오류가 발생했습니다.';
    let details: unknown;

    if (exception instanceof HttpException) {
      status = exception.getStatus();

      const res = exception.getResponse();

      if (typeof res === 'object' && res !== null) {
        const r = res as Record<string, unknown>;

        errorCode =
          (r.errorCode as string) ??
          this.mapStatusToCode(status);

        message = (r.message as string) ?? message;

        details =
          r.details ??
          (Array.isArray(r.message) ? r.message : undefined);
      }
    }

    reply.status(status).send({
      success: false,
      errorCode,
      message,
      details,
      timestamp: new Date().toISOString(),
      path: request.url,
    });
  }

  private mapStatusToCode(status: number): string {
    switch (status) {
      case 400:
        return 'VALIDATION_ERROR';
      case 401:
        return 'UNAUTHORIZED';
      case 429:
        return 'RATE_LIMITED';
      case 503:
        return 'BROKER_UNAVAILABLE';
      default:
        return 'INTERNAL_ERROR';
    }
  }
}