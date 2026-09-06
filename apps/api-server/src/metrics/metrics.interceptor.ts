import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { FastifyRequest } from 'fastify';
import { Observable, tap } from 'rxjs';
import { httpRequestDuration, httpRequestsTotal } from './metrics';

/**
 * 모든 HTTP 요청/응답 사이클을 가로채서(Intercept) 프로메테우스 메트릭을 수집하는 인터셉터
 * 1) 요청 진입 시 고정밀 타이머(process.hrtime.bigint)로 시작 시간 측정
 * 2) 응답 완료(tap) 시 응답 상태코드(statusCode)와 소요 시간(초)을 계산하여 Counter와 Histogram에 기록
 */
@Injectable()
export class MetricsInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const req = context.switchToHttp().getRequest<FastifyRequest>();
    // 나노초 단위 고정밀 타이머로 시작 시점 기록
    const startedAt = process.hrtime.bigint();

    return next.handle().pipe(
      tap(() => {
        const res = context.switchToHttp().getResponse();
        const status = res?.statusCode ?? 200;
        // 나노초 -> 초 단위 변환
        const seconds = Number(process.hrtime.bigint() - startedAt) / 1e9;

        // 상태코드별 요청 수 1 증가 (Counter)
        httpRequestsTotal.inc({ status_code: String(status) });

        // 엔드포인트별 응답 지연시간 관측값 기록 (Histogram 버킷에 누적)
        httpRequestDuration.observe(
          {
            method: req.method,
            route: req.url,
          },
          seconds,
        );
      }),
    );
  }
}

