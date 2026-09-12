// load-test/k6/scenarios/spike.js
// 목적: 순간 폭주 대응력 확인(ARCHITECTURE 8절).
// 지속시간: warm-up 후 30초 스파이크(순간 2,500 eps = click 2,425 / payment 75).
// 통과 기준: 스파이크 구간에서 5xx 없이 202/400/429 만 반환, 스파이크 종료 후 Lag 수렴.

import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, API_KEY } from '../lib/config.js';
import {
  buildValidClick,
  buildDuplicateClick,
  buildInvalidClick,
  buildValidPayment,
  buildDuplicatePayment,
  buildInvalidPayment,
} from '../lib/payload.js';
import { thresholdsFor } from '../lib/thresholds.js';

const HEADERS = { 'Content-Type': 'application/json', 'x-api-key': API_KEY };

// 순간 피크 2,500 eps = click 2,425 + payment 75 (WSL2 한계 고려)
const CLICK_SPIKE = 2425;
const PAYMENT_SPIKE = 75;

export const options = {
  discardResponseBodies: true,
  scenarios: {
    click_valid: {
      executor: 'ramping-arrival-rate',
      exec: 'sendClickValid',
      startRate: 200,
      timeUnit: '1s',
      // maxVUs: 응답 지연에도 VU 고갈로 dropped_iterations가 발생하지 않도록 넉넉히 확보
      preAllocatedVUs: 1000,
      maxVUs: 15000,
      stages: [
        { target: 1000, duration: '1m' }, // warm-up
        { target: CLICK_SPIKE, duration: '30s' }, // spike (5초 내 급상승)
        { target: 1000, duration: '30s' }, // recovery
        { target: 0, duration: '10s' },
      ],
    },
    click_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickDuplicate',
      rate: Math.round(CLICK_SPIKE * 0.03),
      timeUnit: '1s',
      duration: '2m10s',
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
    click_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickInvalid',
      rate: Math.round(CLICK_SPIKE * 0.02),
      timeUnit: '1s',
      duration: '2m10s',
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
    payment_valid: {
      executor: 'ramping-arrival-rate',
      exec: 'sendPaymentValid',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 600,
      stages: [
        { target: 30, duration: '1m' },
        { target: PAYMENT_SPIKE, duration: '30s' },
        { target: 30, duration: '30s' },
        { target: 0, duration: '10s' },
      ],
    },
    payment_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentDuplicate',
      rate: Math.max(1, Math.round(PAYMENT_SPIKE * 0.03)),
      timeUnit: '1s',
      duration: '2m10s',
      preAllocatedVUs: 5,
      maxVUs: 60,
    },
    payment_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentInvalid',
      rate: Math.max(1, Math.round(PAYMENT_SPIKE * 0.02)),
      timeUnit: '1s',
      duration: '2m10s',
      preAllocatedVUs: 5,
      maxVUs: 60,
    },
  },
  thresholds: thresholdsFor('spike'),
};

export function sendClickValid() {
  const res = http.post(`${BASE_URL}/events/click`, JSON.stringify(buildValidClick()), { headers: HEADERS });
  check(res, { 'click 2xx/4xx': (r) => r.status === 202 || r.status === 429 });
}
export function sendClickDuplicate() {
  const res = http.post(`${BASE_URL}/events/click`, JSON.stringify(buildDuplicateClick()), { headers: HEADERS });
  check(res, { 'click dup 202': (r) => r.status === 202 });
}
export function sendClickInvalid() {
  const res = http.post(`${BASE_URL}/events/click`, JSON.stringify(buildInvalidClick()), { headers: HEADERS });
  check(res, { 'click invalid 400': (r) => r.status === 400 });
}
export function sendPaymentValid() {
  const res = http.post(`${BASE_URL}/events/payment`, JSON.stringify(buildValidPayment()), { headers: HEADERS });
  check(res, { 'payment 2xx/4xx': (r) => r.status === 202 || r.status === 429 });
}
export function sendPaymentDuplicate() {
  const res = http.post(`${BASE_URL}/events/payment`, JSON.stringify(buildDuplicatePayment()), { headers: HEADERS });
  check(res, { 'payment dup 202': (r) => r.status === 202 });
}
export function sendPaymentInvalid() {
  const res = http.post(`${BASE_URL}/events/payment`, JSON.stringify(buildInvalidPayment()), { headers: HEADERS });
  check(res, { 'payment invalid 400': (r) => r.status === 400 });
}