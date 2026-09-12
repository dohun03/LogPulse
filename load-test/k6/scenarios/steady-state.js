// load-test/k6/scenarios/steady-state.js
// 목적: '평상시' 트래픽 기준선 확보.
// 지속시간: 10분. click 약 400/s, payment 약 12/s (문서화된 가정의 낮은 부하).
// 통과 기준: PRD KPI 충족 + Kafka Lag 가 평상시 1,000건 이하 유지.

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

// click 400/s 기준 정상 95% / 중복 3% / invalid 2%
const CLICK_TARGET = 400;
const PAYMENT_TARGET = 12;

export const options = {
  discardResponseBodies: true,
  scenarios: {
    click_valid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickValid',
      rate: Math.round(CLICK_TARGET * 0.95),
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
    click_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickDuplicate',
      rate: Math.round(CLICK_TARGET * 0.03),
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 10,
      maxVUs: 30,
    },
    click_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickInvalid',
      rate: Math.round(CLICK_TARGET * 0.02),
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 10,
      maxVUs: 30,
    },
    payment_valid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentValid',
      rate: Math.round(PAYMENT_TARGET * 0.95),
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 10,
      maxVUs: 30,
    },
    payment_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentDuplicate',
      rate: Math.max(1, Math.round(PAYMENT_TARGET * 0.03)),
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 2,
      maxVUs: 10,
    },
    payment_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentInvalid',
      rate: Math.max(1, Math.round(PAYMENT_TARGET * 0.02)),
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 2,
      maxVUs: 10,
    },
  },
  thresholds: thresholdsFor('steady-state'),
};

export function sendClickValid() {
  const res = http.post(`${BASE_URL}/events/click`, JSON.stringify(buildValidClick()), { headers: HEADERS });
  check(res, { 'click 202': (r) => r.status === 202 });
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
  check(res, { 'payment 202': (r) => r.status === 202 });
}
export function sendPaymentDuplicate() {
  const res = http.post(`${BASE_URL}/events/payment`, JSON.stringify(buildDuplicatePayment()), { headers: HEADERS });
  check(res, { 'payment dup 202': (r) => r.status === 202 });
}
export function sendPaymentInvalid() {
  const res = http.post(`${BASE_URL}/events/payment`, JSON.stringify(buildInvalidPayment()), { headers: HEADERS });
  check(res, { 'payment invalid 400': (r) => r.status === 400 });
}