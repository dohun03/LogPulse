import { collectDefaultMetrics, Counter, Histogram, register } from 'prom-client';

/**
 * (Node.js 프로세스 기본 지표 수집기 활성화)
 * - CPU 사용률
 * - 힙/메모리 사용량
 * - 이벤트 루프 지연 시간
 * - 가비지 컬렉션(GC) 횟수 및 시간
 */
collectDefaultMetrics();

// API 요청 수 카운터
export const httpRequestsTotal = new Counter({
  name: 'api_http_requests_total',
  help: 'API 서버가 처리한 HTTP 요청 수 (상태코드 기준)',
  labelNames: ['status_code'],
});

// API 응답 시간 히스토그램 (p50, p95, p99 )
export const httpRequestDuration = new Histogram({
  name: 'api_http_request_duration_seconds',
  help: 'API 서버 HTTP 요청 응답 시간(초)',
  labelNames: ['method', 'route'],
});

export { register };