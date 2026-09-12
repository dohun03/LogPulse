// load-test/k6/scenarios/smoke.js
// 목적: 기본 동작 확인(엔드포인트 응답, 스크립트 자체 오류 여부).
// 지속시간: ~30초, 매우 낮은 처리량(click 5/s, payment 1/s).
// 통과 기준: 정상 요청 에러율 0%, 스크립트 실행 자체 성공.

import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, API_KEY } from '../lib/config.js';
import { buildValidClick, buildValidPayment } from '../lib/payload.js';
import { thresholdsFor } from '../lib/thresholds.js';

const HEADERS = { 'Content-Type': 'application/json', 'x-api-key': API_KEY };

export const options = {
  discardResponseBodies: true,
  scenarios: {
    click_valid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickValid',
      rate: 5,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 5,
      maxVUs: 20,
    },
    payment_valid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentValid',
      rate: 1,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 2,
      maxVUs: 10,
    },
  },
  thresholds: thresholdsFor('smoke'),
};

export function sendClickValid() {
  const res = http.post(`${BASE_URL}/events/click`, JSON.stringify(buildValidClick()), { headers: HEADERS });
  check(res, { 'click 202': (r) => r.status === 202 });
}

export function sendPaymentValid() {
  const res = http.post(`${BASE_URL}/events/payment`, JSON.stringify(buildValidPayment()), { headers: HEADERS });
  check(res, { 'payment 202': (r) => r.status === 202 });
}