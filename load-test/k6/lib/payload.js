// load-test/k6/lib/payload.js
// click / payment 이벤트 페이로드 생성기.
//
// 트래픽 구성(PLAN.md §1.3 문서화된 가정):
//   - Click : Payment = 97 : 3
//   - 각 토픽 내 정상 : 의도적 중복 : 의도적 invalid = 95 : 3 : 2
//   - Click 내 eventType(product_click : page_view) = 70 : 30
//
// 중복 이벤트는 SharedArray 고정 풀에서 꺼내 재전송한다.
// (UUID 랜덤 생성으로는 우연히 중복이 나지 않으므로, 진짜 dedup 경로를 타려면 고정 풀 재사용이 필수)

import { SharedArray } from 'k6/data';

// ---- 유틸리티 ----

// k6 기본 crypto.randomUUID 가 일부 버전에 없을 수 있어, 자체 UUID v4 생성기를 둔다.
export function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function nowIso() {
  return new Date().toISOString();
}

// ---- 정상 Click 이벤트 (product_click 70% : page_view 30%) ----
export function buildValidClick() {
  const eventType = Math.random() < 0.7 ? 'product_click' : 'page_view';
  const body = {
    eventId: uuidv4(),
    userId: `user-${Math.floor(Math.random() * 1000000)}`,
    sessionId: `sess-${uuidv4()}`,
    eventType,
    pageUrl:
      eventType === 'product_click'
        ? `/products/${Math.floor(Math.random() * 100000)}`
        : `/pages/${Math.floor(Math.random() * 10000)}`,
    occurredAt: nowIso(),
  };
  if (eventType === 'product_click') {
    body.productId = `prod-${Math.floor(Math.random() * 100000)}`;
  }
  return body;
}

// ---- 정상 Payment 이벤트 ----
export function buildValidPayment() {
  const statuses = ['completed', 'failed', 'canceled'];
  return {
    eventId: uuidv4(),
    orderId: `order-${uuidv4()}`,
    userId: `user-${Math.floor(Math.random() * 1000000)}`,
    amount: Math.floor(Math.random() * 1000000),
    currency: 'KRW',
    paymentMethod: 'card',
    status: statuses[Math.floor(Math.random() * statuses.length)],
    occurredAt: nowIso(),
  };
}

// ---- 의도적 중복용 고정 이벤트 풀 (SharedArray) ----
// SharedArray의 팩토리 함수는 최초 1회만 실행되어, 생성된 eventId 등이
// 모든 VU 간에 고정으로 공유된다. 같은 항목을 반복 전송하면 Redis dedup 경로를 타게 된다.

export const clickDuplicatePool = new SharedArray('click-duplicate-pool', function () {
  const arr = [];
  for (let i = 0; i < 200; i++) {
    arr.push({
      eventId: uuidv4(),
      userId: `dup-user-${i}`,
      sessionId: `dup-sess-${i}`,
      eventType: i % 2 === 0 ? 'product_click' : 'page_view',
      productId: `dup-prod-${i}`,
      pageUrl: `/products/${i}`,
      occurredAt: nowIso(),
    });
  }
  return arr;
});

export const paymentDuplicatePool = new SharedArray('payment-duplicate-pool', function () {
  const arr = [];
  for (let i = 0; i < 20; i++) {
    arr.push({
      eventId: uuidv4(),
      orderId: `dup-order-${i}`,
      userId: `dup-user-${i}`,
      amount: 10000 + i,
      currency: 'KRW',
      paymentMethod: 'card',
      status: 'completed',
      occurredAt: nowIso(),
    });
  }
  return arr;
});

// ---- 의도적 중복 이벤트 빌더 (고정 풀에서 재전송) ----
export function buildDuplicateClick() {
  const pool = clickDuplicatePool;
  const item = pool[Math.floor(Math.random() * pool.length)];
  return JSON.parse(JSON.stringify(item));
}

export function buildDuplicatePayment() {
  const pool = paymentDuplicatePool;
  const item = pool[Math.floor(Math.random() * pool.length)];
  return JSON.parse(JSON.stringify(item));
}

// ---- 의도적 invalid 이벤트 빌더 ----
// 최소 3가지 변형을 순환:
//   0) 필수 필드 누락
//   1) 잘못된 enum(eventType / status)
//   2) eventId 가 UUID 가 아닌 문자열
let invalidVariant = 0;

export function buildInvalidClick() {
  const body = buildValidClick();
  const variant = invalidVariant % 3;
  invalidVariant++;
  switch (variant) {
    case 0:
      delete body.sessionId; // 필수 필드 누락
      break;
    case 1:
      body.eventType = 'invalid_type'; // 잘못된 enum
      break;
    case 2:
      body.eventId = 'not-a-uuid'; // UUID 아님
      break;
  }
  return body;
}

export function buildInvalidPayment() {
  const body = buildValidPayment();
  const variant = invalidVariant % 3;
  invalidVariant++;
  switch (variant) {
    case 0:
      delete body.orderId; // 필수 필드 누락
      break;
    case 1:
      body.status = 'invalid_status'; // 잘못된 enum
      break;
    case 2:
      body.eventId = 'not-a-uuid'; // UUID 아님
      break;
  }
  return body;
}