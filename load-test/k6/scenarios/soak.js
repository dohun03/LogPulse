// load-test/k6/scenarios/soak.js
// 목적: 장시간 안정성(메모리 누수, GC, Lag 누적) 확인.
// 지속시간: 30분(목표치(2,000)의 약 50% = click ~970/s 지속).
// 통과 기준: 메모리 사용량이 시간에 비례해 계속 증가하지 않고, Lag 가 시간에 비례해 계속 증가하지 않음.

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

// 목표치(2,000 eps)의 약 50% = 970 eps (click 기준)
const CLICK_TARGET = 970;
const PAYMENT_TARGET = 30;

export const options = {
  discardResponseBodies: true,
  scenarios: {
    click_valid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickValid',
      rate: Math.round(CLICK_TARGET * 0.95),
      timeUnit: '1s',
      duration: '30m',
      preAllocatedVUs: 300,
      maxVUs: 6000,
    },
    click_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickDuplicate',
      rate: Math.round(CLICK_TARGET * 0.03),
      timeUnit: '1s',
      duration: '30m',
      preAllocatedVUs: 50,
      maxVUs: 300,
    },
    click_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendClickInvalid',
      rate: Math.round(CLICK_TARGET * 0.02),
      timeUnit: '1s',
      duration: '30m',
      preAllocatedVUs: 50,
      maxVUs: 300,
    },
    payment_valid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentValid',
      rate: Math.round(PAYMENT_TARGET * 0.95),
      timeUnit: '1s',
      duration: '30m',
      preAllocatedVUs: 20,
      maxVUs: 300,
    },
    payment_duplicate: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentDuplicate',
      rate: Math.max(1, Math.round(PAYMENT_TARGET * 0.03)),
      timeUnit: '1s',
      duration: '30m',
      preAllocatedVUs: 5,
      maxVUs: 60,
    },
    payment_invalid: {
      executor: 'constant-arrival-rate',
      exec: 'sendPaymentInvalid',
      rate: Math.max(1, Math.round(PAYMENT_TARGET * 0.02)),
      timeUnit: '1s',
      duration: '30m',
      preAllocatedVUs: 5,
      maxVUs: 60,
    },
  },
  thresholds: thresholdsFor('soak'),
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