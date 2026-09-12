// load-test/k6/lib/thresholds.js
// PRD 2.3절 KPI를 k6 thresholds 로 그대로 인코딩해, 테스트 실행 자체가 pass/fail 을 자동 판정하도록 한다.
//
// PRD KPI:
//   - 처리량: 3,000 events/sec 이상 (순간 피크 5,000 events/sec)
//   - click-events  p95 100ms 이하, p99 300ms 이하
//   - payment-events p95 200ms 이하, p99 500ms 이하
//   - API 에러율 1% 미만 (click 기준, payment 는 유실 0% 목표이므로 더 엄격)
//
// k6는 각 시나리오에 자동으로 `scenario` 태그를 부여하므로,
// 시나리오 이름(click_valid / payment_valid 등)으로 지표를 분리해 판정한다.

export function thresholdsFor(scenarioGroup) {
  if (scenarioGroup === 'smoke') {
    // smoke 는 '스크립트가 에러 없이 도는지'만 확인하므로, 유효 요청은 에러율 0% 만 본다.
    return {
      'http_req_failed{scenario:click_valid}': ['rate==0'],
      'http_req_failed{scenario:payment_valid}': ['rate==0'],
    };
  }

  // steady-state / target-throughput / spike / soak 공통: PRD KPI 그대로
  return {
    // click 정상 트래픽: 에러율 1% 미만, p95 100ms / p99 300ms 이내
    'http_req_failed{scenario:click_valid}': ['rate<0.01'],
    'http_req_duration{scenario:click_valid}': ['p(95)<100', 'p(99)<300'],

    // payment 정상 트래픽: 유실 0% 목표이므로 에러율 매우 낮게, p95 200ms / p99 500ms
    'http_req_failed{scenario:payment_valid}': ['rate<0.001'],
    'http_req_duration{scenario:payment_valid}': ['p(95)<200', 'p(99)<500'],
  };
}