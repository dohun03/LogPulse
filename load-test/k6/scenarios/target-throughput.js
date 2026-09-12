// load-test/k6/scenarios/target-throughput.js
// 목적: 핵심 목표 — WSL2 로컬 한계를 고려한 2,000+ eps 달성 검증.
// 지속시간: 8분(ramp 2 + sustain 5 + down 1).
// 목표: click ~1,940/s + payment ~60/s = 합계 2,000+ eps.
// 통과 기준: PRD KPI 전부 충족 + Consumer Lag 이 부하 종료 후 수렴.

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

// 목표: 전체 2,000 eps, click:payment = 97:3
const CLICK_TARGET = 1940; // 2000 * 0.97
const PAYMENT_TARGET = 60; // 2000 * 0.03

export const options = {
  // 응답 body를 검증에 쓰지 않으므로 저장하지 않아 k6 클라이언트 메모리/CPU 절약
  discardResponseBodies: true,
  scenarios: {
    click_valid: {
      executor: 'ramping-arrival-rate',
      exec: 'sendClickValid',
      startRate: 200,
      timeUnit: '1s',
      // maxVUs: 응답 지연(최대 6s 관측)에도 VU 고갈로 dropped_iterations가
      // 발생하지 않도록 target 대비 넉넉히 확보(target * 6 이상)
      preAllocatedVUs: 500,
      maxVUs: 12000,
      stages: [
        { target: Math.round(CLICK_TARGET * 0.95), duration: '2m' }, // ramp-up
        { target: Math.round(CLICK_TARGET * 0.95), duration: '5m' }, // sustain
        { target: 0, duration: '1m' }, // ramp-down
      ],
    },
    click_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickDuplicate',
      rate: Math.round(CLICK_TARGET * 0.03),
      timeUnit: '1s',
      duration: '8m',
      preAllocatedVUs: 30,
      maxVUs: 400,
    },
    click_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickInvalid',
      rate: Math.round(CLICK_TARGET * 0.02),
      timeUnit: '1s',
      duration: '8m',
      preAllocatedVUs: 30,
      maxVUs: 400,
    },
    payment_valid: {
      executor: 'ramping-arrival-rate',
      exec: 'sendPaymentValid',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 400,
      stages: [
        { target: Math.round(PAYMENT_TARGET * 0.95), duration: '2m' },
        { target: Math.round(PAYMENT_TARGET * 0.95), duration: '5m' },
        { target: 0, duration: '1m' },
      ],
    },
    payment_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentDuplicate',
      rate: Math.max(1, Math.round(PAYMENT_TARGET * 0.03)),
      timeUnit: '1s',
      duration: '8m',
      preAllocatedVUs: 5,
      maxVUs: 60,
    },
    payment_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentInvalid',
      rate: Math.max(1, Math.round(PAYMENT_TARGET * 0.02)),
      timeUnit: '1s',
      duration: '8m',
      preAllocatedVUs: 5,
      maxVUs: 60,
    },
  },
  thresholds: thresholdsFor('target-throughput'),
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