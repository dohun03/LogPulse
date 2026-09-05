# LogPulse — 상세 기능 및 API 명세서 (`DEVELOPMENT_SPEC.md`)

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.1 |
| 작성일 | 2026-09-02 |
| 참조 문서 | `LogPulse_PRD.md`, `LogPulse_System_Architecture.md` |
| 문서 목적 | 개발자가 별도 설계 없이 바로 코드를 작성할 수 있는 수준의 구현 명세 제공 |
| 주요 변경사항 | NestJS API 서버의 HTTP 어댑터를 **Express → Fastify**로 확정하고, **Nginx 1대 → NestJS API 서버 2대 → Kafka 3대 클러스터 → Consumer Group/Instance 1:1 매핑** 토폴로지를 명시하고, **Consumer 역할 분리 및 Click BatchBuffer flush 실패 정책을 고정** |
| 배포 기준 | Nginx 1대, API 서버 2대, Kafka 브로커 3대, Click Consumer 3대, Payment Consumer 2대 |

> 이 문서는 PRD와 시스템 아키텍처 문서에서 정의한 요구사항·구조를 그대로 계승하며, 여기서는 "어떻게 코드로 작성하고 배포하는가"에 집중한다.
>
> 본 문서의 API 계약(요청/응답), 에러 코드, Kafka/Redis/ClickHouse 파라미터, 서버 구성 및 파티션 매핑은 구현·리뷰·테스트의 기준선(baseline)으로 취급한다.

---

## 0. 목차

1. 기술 스택 확정 및 패키지 목록
2. 전체 배포 토폴로지 및 핵심 매핑
3. 프로젝트 디렉토리 구조 (파일 레벨)
4. 공통 사항 (환경 변수, 응답 포맷, 에러 코드, 공통 스키마)
5. Nginx 및 API 서버 구성
6. NestJS + Fastify 부트스트랩 설정
7. API 명세 (Endpoint 상세)
8. api-server 모듈 상세 설계 (코드 레벨)
9. consumer-worker 상세 설계 (코드 레벨)
10. ClickHouse 스키마 및 마이그레이션
11. Kafka 토픽 생성 스크립트
12. 로깅 규칙
13. 테스트 전략 및 체크리스트
14. 로컬 개발 실행 절차
15. 부록: 공통 타입 정의 전체 (`libs/shared`)

---

# 1. 기술 스택 확정 및 패키지 목록

## 1.1 확정 스택

| 영역 | 선택 | 비고 |
|---|---|---|
| 외부 진입점 | **Nginx 1대** | L7 Reverse Proxy / Load Balancer |
| API 서버 | **NestJS 10.x** | **2대**, Stateless |
| HTTP 어댑터 | **Fastify 4.x** | `@nestjs/platform-fastify` |
| Message Broker | **Apache Kafka 4.3.x (KRaft)** | **3 Broker Cluster**, `kafkajs` 클라이언트 |
| In-Memory Store | Redis 7.x | `ioredis` 클라이언트 |
| Analytics DB | ClickHouse 25.8.x LTS | `@clickhouse/client` 공식 Node 클라이언트 |
| 검증 | class-validator / class-transformer | Fastify에서도 동일하게 동작 |
| 로깅 | Pino | `nestjs-pino` 사용 |
| 컨테이너 | Docker / Docker Compose | 로컬 개발 및 단일 호스트 검증 |
| 부하 테스트 | k6 | API 및 Kafka 파이프라인 부하 검증 |

### 1.1.1 핵심 배포 구성

```text
                        External Traffic
                              │
                              ▼
                    ┌───────────────────┐
                    │   Nginx 1대       │
                    │ L7 Reverse Proxy  │
                    │ Round-Robin       │
                    └─────────┬─────────┘
                              │
                   ┌──────────┴──────────┐
                   │                     │
                   ▼                     ▼
        ┌──────────────────┐  ┌──────────────────┐
        │ NestJS API #1    │  │ NestJS API #2    │
        │ Fastify          │  │ Fastify          │
        │ Stateless        │  │ Stateless        │
        └────────┬─────────┘  └─────────┬─────────┘
                 │                      │
                 └──────────┬───────────┘
                            ▼
                 ┌──────────────────────┐
                 │ Kafka Cluster        │
                 │ Broker #1 #2 #3      │
                 │ KRaft / RF=3         │
                 └──────────┬───────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
          ▼                                   ▼
┌────────────────────────┐        ┌────────────────────────┐
│ click-events           │        │ payment-events         │
│ partitions = 3        │        │ partitions = 2         │
└───────────┬────────────┘        └───────────┬────────────┘
            │                                 │
            ▼                                 ▼
┌────────────────────────┐        ┌────────────────────────┐
│ Click Consumer Group   │        │ Payment Consumer Group  │
│ Consumer #1 → P0       │        │ Consumer #1 → P0        │
│ Consumer #2 → P1       │        │ Consumer #2 → P1        │
│ Consumer #3 → P2       │        │                         │
└───────────┬────────────┘        └───────────┬────────────┘
            │                                 │
            └────────────────┬────────────────┘
                             ▼
                   ┌───────────────────┐
                   │ Redis             │
                   │ Dedup             │
                   └─────────┬─────────┘
                             │
                             ▼
                   ┌───────────────────┐
                   │ ClickHouse        │
                   │ Analytics DB       │
                   └───────────────────┘
```

## 1.2 api-server 패키지 목록

```jsonc
// apps/api-server/package.json
{
  "dependencies": {
    "@nestjs/core": "^10.0.0",
    "@nestjs/common": "^10.0.0",
    "@nestjs/config": "^3.0.0",
    "@nestjs/platform-fastify": "^10.0.0",
    "@nestjs/terminus": "^10.0.0",
    "@fastify/helmet": "^11.0.0",
    "@fastify/cors": "^9.0.0",
    "@fastify/rate-limit": "^9.0.0",
    "fastify": "^4.26.0",
    "kafkajs": "^2.2.4",
    "class-validator": "^0.14.0",
    "class-transformer": "^0.5.1",
    "nestjs-pino": "^4.0.0",
    "pino-http": "^9.0.0",
    "uuid": "^9.0.0"
  }
}
```

> `express`, `@nestjs/platform-express`, `body-parser`, `cors`, `helmet`(Express용)은 설치하지 않는다.

## 1.3 consumer-worker 패키지 목록

```jsonc
// apps/consumer-worker/package.json
{
  "dependencies": {
    "@nestjs/core": "^10.0.0",
    "@nestjs/common": "^10.0.0",
    "@nestjs/config": "^3.0.0",
    "kafkajs": "^2.2.4",
    "ioredis": "^5.4.0",
    "@clickhouse/client": "^1.0.0",
    "nestjs-pino": "^4.0.0",
    "uuid": "^9.0.0"
  }
}
```

> `consumer-worker`는 HTTP 엔드포인트가 없는 순수 백그라운드 프로세스이므로 Fastify/Express 어댑터가 필요 없다.

---

# 2. 전체 배포 토폴로지 및 핵심 매핑

## 2.1 Nginx 1대

Nginx는 외부 요청을 직접 처리하는 유일한 진입점이다.

### 역할

1. 외부 HTTP 트래픽 수신
2. `/events/*`, `/health/*` 요청을 API 서버로 전달
3. API 서버 2대에 **Round-Robin** 방식으로 분산
4. 연결이 비정상인 API 인스턴스는 `max_fails` / `fail_timeout` 기준으로 일시 제외
5. TLS 종료가 필요한 경우 Nginx에서 처리 가능

### 기본 upstream 구성

```nginx
upstream logpulse_api {
    server api-server-1:3000 max_fails=3 fail_timeout=10s;
    server api-server-2:3000 max_fails=3 fail_timeout=10s;
}

server {
    listen 80;

    location / {
        proxy_pass http://logpulse_api;

        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_connect_timeout 2s;
        proxy_read_timeout 10s;
        proxy_send_timeout 10s;
    }
}
```

> 두 API 서버는 세션/로컬 캐시/메모리 기반 사용자 상태에 의존하지 않는 **완전한 Stateless 구조**로 구현한다.
>
> Nginx의 Round-Robin은 요청을 균등하게 분배하는 기본 정책이며, 실제 분산 비율은 연결 종료/응답 지연/장애 제외 등의 운영 상황에 따라 달라질 수 있다.

---

## 2.2 NestJS API 서버 2대

### 인스턴스 구성

| 항목 | API #1 | API #2 |
|---|---|---|
| 프로세스 | NestJS | NestJS |
| HTTP | Fastify | Fastify |
| 포트 | 3000 | 3000 |
| 상태 저장 | 없음 | 없음 |
| Kafka Producer | 자체 연결 | 자체 연결 |
| 인증 상태 | 요청 단위 API Key | 요청 단위 API Key |
| 세션 의존 | 없음 | 없음 |

### 요청 처리 흐름

```text
Client
  ↓
Nginx
  ↓
API Server #1 또는 #2
  ↓
API Key Guard
  ↓
DTO Validation
  ↓
EventsService
  ↓
KafkaProducerService
  ↓
Kafka Ack
  ↓
HTTP 202
```

### 중요 원칙

- API 서버는 ClickHouse에 직접 쓰지 않는다.
- API 서버는 Redis를 사용하지 않는다.
- API 서버 간 공유 메모리를 사용하지 않는다.
- 요청 성공 응답은 Kafka 발행 성공을 기준으로 한다.
- API 프로세스가 재시작되어도 다른 API 서버가 동일 요청을 처리할 수 있어야 한다.

---

## 2.3 Kafka Cluster — Broker 3대

Kafka는 3개의 Broker로 구성한다.

```text
Broker #1
Broker #2
Broker #3
```

### 공통 클러스터 정책

| 항목 | 값 |
|---|---|
| Broker 수 | 3 |
| 모드 | KRaft |
| 운영 replication factor | 3 |
| `min.insync.replicas` | 2 |
| 자동 Topic 생성 | 비활성화 |
| 데이터 내구성 | RF=3 기반 |
| Controller Quorum | 3 노드 구성 권장 |

### 장애 허용 기준

운영 기준에서는 Broker 1대 장애가 발생하더라도 최소 2대가 ISR에 남아 쓰기 요청을 계속 받을 수 있어야 한다.

```text
RF=3
ISR>=2

Broker #1 DOWN
   ↓
Broker #2 + Broker #3
   ↓
계속 Write 가능
```

> 로컬 단일 브로커 테스트에서는 RF=1을 사용할 수 있으나, 운영 설계 기준선은 RF=3 / `min.insync.replicas=2`다.

---

## 2.4 `click-events` 토픽 매핑

### 파티션

```text
click-events
├── Partition 0
├── Partition 1
└── Partition 2
```

총 **3개 파티션**.

### 목적

- 처리량 중심
- 최대 3개 Consumer Instance 병렬 처리
- 동일 `sessionId`를 Kafka message key로 사용하여 같은 세션의 이벤트가 동일 파티션으로 라우팅되도록 한다.

### Producer

```typescript
key = dto.sessionId
```

### Consumer Group

```text
Group ID:
logpulse-click-loader
```

### Consumer Instance

```text
click-consumer-1 → Partition 0
click-consumer-2 → Partition 1
click-consumer-3 → Partition 2
```

정상적인 **stable group 상태에서는 3개 Consumer와 3개 Partition이 1:1로 배정**된다.

### 파티션 수 축소 근거

기존 설계보다 `click-events`를 12→3, `payment-events`를 6→2로 축소한다.

> **1:1 매핑을 통해 Consumer Group Rebalance/Partition Assignment 동작을 명확히 검증하기 위해 운영 스펙 대비 파티션 수를 컨슈머 인스턴스 수와 동일하게 축소함. 실제 처리량 확장이 필요하면 파티션 재분할이 필요하며 이 경우 기존 key 기반 해시가 깨질 수 있음을 인지함.**

> 단, Kafka Consumer Group은 장애/재시작/스케일링 등 멤버십 변화가 발생하면 리밸런싱할 수 있다. 따라서 "리밸런싱이 절대 발생하지 않는다"가 아니라, **정상 상태에서 각 consumer가 한 partition씩 담당하는 최적 병렬 구조를 목표로 한다**고 정의한다.

---

## 2.5 `payment-events` 토픽 매핑

### 파티션

```text
payment-events
├── Partition 0
└── Partition 1
```

총 **2개 파티션**.

### 목적

- 정확성 및 주문/결제 이벤트 순서 보장 중심
- Consumer Instance 2대
- `orderId`를 Kafka message key로 사용
- 동일 주문의 이벤트는 동일 파티션에 순서대로 기록

### Producer

```typescript
key = dto.orderId
```

### Consumer Group

```text
Group ID:
logpulse-payment-loader
```

### Consumer Instance

```text
payment-consumer-1 → Partition 0
payment-consumer-2 → Partition 1
```

정상적인 stable group 상태에서는 2개 Consumer와 2개 Partition이 1:1로 배정된다.

### 순서 보장 범위

Kafka가 보장하는 순서는 **동일 파티션 내부**다.

따라서 다음 조건을 지킨다.

```text
동일 orderId
      ↓
동일 Kafka key
      ↓
동일 partition
      ↓
단일 active consumer가 순차 처리
```

> 전체 `payment-events` 토픽에 대해 전역 순서를 보장하는 것은 아니다. 요구사항은 **동일 주문(orderId) 단위의 순서 보장**으로 정의한다.

---

## 2.6 Consumer Group / Instance 요약

| Group | Topic | Partition | Consumer Instance | 정상 매핑 |
|---|---|---:|---:|---|
| `logpulse-click-loader` | `click-events` | 3 | 3 | 1:1 |
| `logpulse-payment-loader` | `payment-events` | 2 | 2 | 1:1 |

### 스케일링 제약

Consumer Group의 병렬 처리량을 늘리려면 Partition 수도 함께 늘려야 한다.

예:

```text
click-events
P=3 → 최대 active consumer 3개

click-events
P=6 → 최대 active consumer 6개
```

Consumer Instance가 Partition보다 많으면 일부 Consumer는 유휴 상태가 된다.

---

# 3. 프로젝트 디렉토리 구조 (파일 레벨)

```text
logpulse/
├── apps/
│   ├── api-server/
│   │   └── src/
│   │       ├── main.ts
│   │       ├── app.module.ts
│   │       ├── common/
│   │       │   ├── filters/
│   │       │   │   └── global-exception.filter.ts
│   │       │   ├── guards/
│   │       │   │   └── api-key.guard.ts
│   │       │   ├── interceptors/
│   │       │   │   └── logging.interceptor.ts
│   │       │   └── constants/
│   │       │       └── error-code.enum.ts
│   │       ├── events/
│   │       │   ├── events.module.ts
│   │       │   ├── events.controller.ts
│   │       │   ├── events.service.ts
│   │       │   └── dto/
│   │       │       ├── create-click-event.dto.ts
│   │       │       └── create-payment-event.dto.ts
│   │       ├── kafka/
│   │       │   ├── kafka.module.ts
│   │       │   ├── kafka-producer.service.ts
│   │       │   └── kafka.config.ts
│   │       └── health/
│   │           ├── health.module.ts
│   │           ├── health.controller.ts
│   │           └── kafka.health-indicator.ts
│   │
│   └── consumer-worker/
│       └── src/
│           ├── main.ts
│           ├── app.module.ts
│           ├── config/
│           │   └── consumer.config.ts
│           ├── consumers/
│           │   ├── click-events.consumer.ts
│           │   └── payment-events.consumer.ts
│           ├── redis/
│           │   ├── redis.module.ts
│           │   └── redis-dedup.service.ts
│           ├── clickhouse/
│           │   ├── clickhouse.module.ts
│           │   ├── clickhouse-writer.service.ts
│           │   └── batch-buffer.ts
│           ├── dlq/
│           │   └── dlq-producer.service.ts
│           └── common/
│               └── retry.util.ts
│
├── libs/
│   └── shared/
│       └── src/
│           ├── types/
│           │   ├── click-event.type.ts
│           │   ├── payment-event.type.ts
│           │   └── api-response.type.ts
│           ├── constants/
│           │   ├── kafka-topics.enum.ts
│           │   └── redis-key.util.ts
│           └── index.ts
│
├── infra/
│   ├── docker-compose.yml
│   ├── nginx/
│   │   └── nginx.conf
│   ├── clickhouse/
│   │   └── init.sql
│   └── kafka/
│       └── create-topics.sh
│
├── load-test/
│   └── k6/
│       ├── click-events.load.js
│       └── payment-events.load.js
│
└── docs/
    ├── LogPulse_PRD.md
    ├── LogPulse_System_Architecture.md
    └── DEVELOPMENT_SPEC.md
```

---

# 4. 공통 사항

## 4.1 환경 변수 명세

### 4.1.1 `apps/api-server/.env`

API #1과 API #2는 대부분 동일한 값을 사용하며, `KAFKA_CLIENT_ID`만 인스턴스별로 고유하게 지정할 것을 권장한다.

| 변수명 | 타입 | 기본값 | 설명 |
|---|---|---|---|
| `PORT` | number | 3000 | API 서버 포트 |
| `KAFKA_BROKERS` | string (CSV) | `localhost:9092` | Kafka Broker 목록 |
| `KAFKA_CLIENT_ID` | string | `logpulse-api-server` | API 인스턴스별 Kafka Client ID |
| `KAFKA_CLICK_TOPIC` | string | `click-events` | click 토픽 |
| `KAFKA_PAYMENT_TOPIC` | string | `payment-events` | payment 토픽 |
| `API_KEY` | string | 필수 | 내부 서비스 인증용 API Key |
| `RATE_LIMIT_MAX` | number | 5000 | API 전체 기준 IP당 분당 최대 요청 수 |
| `API_INSTANCE_COUNT` | number | 2 | API 인스턴스 수. 프로세스별 Rate Limit 계산에 사용 |
| `LOG_LEVEL` | string | `info` | Pino 로그 레벨 |

운영 환경 예시:

```dotenv
# API #1
KAFKA_CLIENT_ID=logpulse-api-server-1

# API #2
KAFKA_CLIENT_ID=logpulse-api-server-2
```

### 4.1.2 `apps/consumer-worker/.env`

| 변수명 | 타입 | 기본값 | 설명 |
|---|---|---|---|
| `CONSUMER_ROLE` | string | 필수 | `click` 또는 `payment`. 컨테이너당 하나의 Consumer 역할만 활성화 |
| `KAFKA_BROKERS` | string (CSV) | `localhost:9092` | Kafka Broker 목록 |
| `KAFKA_CLIENT_ID` | string | `logpulse-consumer-worker` | Kafka client ID |
| `CLICK_CONSUMER_GROUP_ID` | string | `logpulse-click-loader` | click consumer group |
| `PAYMENT_CONSUMER_GROUP_ID` | string | `logpulse-payment-loader` | payment consumer group |
| `REDIS_URL` | string | `redis://localhost:6379` | Redis 연결 URL |
| `REDIS_CLICK_DEDUP_TTL_SEC` | number | 600 | click 멱등성 키 TTL |
| `REDIS_PAYMENT_DEDUP_TTL_SEC` | number | 86400 | payment 멱등성 키 TTL |
| `CLICKHOUSE_URL` | string | `http://localhost:8123` | ClickHouse HTTP 엔드포인트 |
| `CLICKHOUSE_DATABASE` | string | `logpulse` | DB명 |
| `CLICKHOUSE_USERNAME` | string | `logpulse_writer` | 적재용 계정 |
| `CLICKHOUSE_PASSWORD` | string | 필수 | 적재용 계정 비밀번호 |
| `CLICK_BATCH_MAX_SIZE` | number | 500 | click 배치 최대 건수 |
| `CLICK_BATCH_FLUSH_MS` | number | 1000 | click 배치 최대 대기 시간 |
| `PAYMENT_MAX_RETRY` | number | 3 | payment 적재 실패 최대 재시도 |
| `KAFKA_PAYMENT_DLQ_TOPIC` | string | `payment-events-dlq` | payment DLQ 토픽 |

---

## 4.2 공통 응답 포맷

### 성공 — 202 Accepted

```json
{
  "success": true,
  "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "topic": "click-events",
  "acceptedAt": "2026-09-02T05:12:00.123Z"
}
```

### 실패 — 4xx / 5xx

```json
{
  "success": false,
  "errorCode": "VALIDATION_ERROR",
  "message": "요청 본문 검증에 실패했습니다.",
  "details": [
    {
      "field": "occurredAt",
      "reason": "occurredAt must be a valid ISO 8601 date string"
    }
  ],
  "timestamp": "2026-09-02T05:12:00.123Z",
  "path": "/events/click"
}
```

---

## 4.3 에러 코드 정의표

| errorCode | HTTP Status | 발생 조건 |
|---|---:|---|
| `VALIDATION_ERROR` | 400 | DTO 검증 규칙 위반 |
| `UNAUTHORIZED` | 401 | `x-api-key` 누락 또는 불일치 |
| `RATE_LIMITED` | 429 | IP 단위 요청 한도 초과 |
| `BROKER_UNAVAILABLE` | 503 | Kafka Producer 발행 실패 |
| `INTERNAL_ERROR` | 500 | 그 외 예기치 못한 오류 |

---

## 4.4 공통 이벤트 스키마

Kafka 메시지 `value`는 다음 Envelope 구조를 사용한다.

```typescript
export interface EventEnvelope<T> {
  eventId: string;
  ingestedAt: string;
  payload: T;
}
```

---

# 5. Nginx 및 API 서버 구성

## 5.1 Nginx 구성 파일

위치:

```text
infra/nginx/nginx.conf
```

권장 기본 설정:

```nginx
worker_processes auto;

events {
    worker_connections 4096;
}

http {
    upstream logpulse_api {
        server api-server-1:3000 max_fails=3 fail_timeout=10s;
        server api-server-2:3000 max_fails=3 fail_timeout=10s;
    }

    server {
        listen 80;

        location / {
            proxy_pass http://logpulse_api;

            proxy_http_version 1.1;

            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            proxy_connect_timeout 2s;
            proxy_read_timeout 10s;
            proxy_send_timeout 10s;
        }
    }
}
```

## 5.2 `trustProxy`

Nginx 뒤에서 동작하므로 Fastify는 Proxy Header를 신뢰하도록 설정한다.

```typescript
new FastifyAdapter({
  trustProxy: true,
});
```

> `trustProxy: true`를 사용하는 경우 실제 운영 네트워크에서 어떤 프록시가 신뢰 가능한지 명확하게 관리한다. Rate Limit의 IP 식별 및 `X-Forwarded-For` 기반 주소 처리와 직접 관련된다.

## 5.3 API 서버 Health Check

Nginx와 오케스트레이터는 다음 엔드포인트를 사용한다.

```text
GET /health/liveness
GET /health/readiness
```

권장 흐름:

```text
liveness
  └─ 프로세스 생존 여부만 확인

readiness
  └─ Kafka Producer 연결 준비 상태 확인
```

---

# 6. NestJS + Fastify 부트스트랩 설정

## 6.1 API `main.ts`

```typescript
// apps/api-server/src/main.ts

import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { ValidationPipe } from '@nestjs/common';
import fastifyHelmet from '@fastify/helmet';
import fastifyCors from '@fastify/cors';
import fastifyRateLimit from '@fastify/rate-limit';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { GlobalExceptionFilter } from './common/filters/global-exception.filter';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({
      trustProxy: true,
    }),
    { bufferLogs: true },
  );

  app.useLogger(app.get(Logger));

  await app.register(fastifyHelmet);

  await app.register(fastifyCors, {
    origin: true,
  });

  const aggregateRateLimit =
    Number(process.env.RATE_LIMIT_MAX ?? 5000);

  const apiInstanceCount =
    Math.max(
      1,
      Number(process.env.API_INSTANCE_COUNT ?? 2),
    );

  await app.register(fastifyRateLimit, {
    max: Math.ceil(
      aggregateRateLimit / apiInstanceCount,
    ),
    timeWindow: '1 minute',
  });

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  app.useGlobalFilters(new GlobalExceptionFilter());

  const port = Number(process.env.PORT ?? 3000);

  await app.listen({
    port,
    host: '0.0.0.0',
  });
}

bootstrap();
```

## 6.2 Fastify 전환 시 유의사항

| 항목 | Express | Fastify |
|---|---|---|
| Helmet | `helmet()` | `@fastify/helmet` |
| CORS | `app.enableCors()` / Express middleware | `@fastify/cors` |
| Rate Limit | Express middleware | `@fastify/rate-limit` |
| Body Parsing | `body-parser` 의존 가능 | Fastify 내장 |
| Static | `express.static` | 본 프로젝트 불필요 |
| Request/Response | Express 객체 | Fastify 객체 |
| Nest DTO | 사용 | 동일 사용 |

---

## 6.3 Consumer Worker `main.ts`

```typescript
// apps/consumer-worker/src/main.ts

import { NestFactory } from '@nestjs/core';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule, {
    bufferLogs: true,
  });

  app.useLogger(app.get(Logger));

  const shutdown = async () => {
    await app.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

bootstrap();
```

---

# 7. API 명세

## 7.1 `POST /events/click`

| 항목 | 내용 |
|---|---|
| 설명 | 이커머스 상품 클릭/페이지 이동 이벤트를 수신해 `click-events`에 발행 |
| 인증 | `x-api-key` |
| 응답 | Kafka `acks=1` 성공 후 202 |
| Producer Key | `sessionId` |
| Topic | `click-events` |
| Partitions | 3 |
| Compression | LZ4 |
| Kafka Replication | 운영 RF=3 |

### DTO

```typescript
// apps/api-server/src/events/dto/create-click-event.dto.ts

import {
  IsUUID,
  IsString,
  IsOptional,
  IsISO8601,
  IsIn,
  ValidateNested,
  IsNotEmpty,
} from 'class-validator';
import { Type } from 'class-transformer';

export class ClickEventMetadataDto {
  @IsOptional()
  @IsString()
  referrer?: string;

  @IsOptional()
  @IsString()
  device?: string;

  @IsOptional()
  @IsString()
  ip?: string;
}

export class CreateClickEventDto {
  @IsUUID('4')
  eventId: string;

  @IsString()
  @IsNotEmpty()
  userId: string;

  @IsString()
  @IsNotEmpty()
  sessionId: string;

  @IsIn(['product_click', 'page_view'])
  eventType: 'product_click' | 'page_view';

  @IsOptional()
  @IsString()
  productId?: string;

  @IsString()
  @IsNotEmpty()
  pageUrl: string;

  @IsISO8601()
  occurredAt: string;

  @IsOptional()
  @ValidateNested()
  @Type(() => ClickEventMetadataDto)
  metadata?: ClickEventMetadataDto;
}
```

### Request

```json
{
  "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "userId": "user-1001",
  "sessionId": "sess-88af2",
  "eventType": "product_click",
  "productId": "prod-2024",
  "pageUrl": "/products/2024",
  "occurredAt": "2026-09-02T05:11:59.900Z",
  "metadata": {
    "referrer": "https://google.com",
    "device": "mobile"
  }
}
```

### Response — 202

```json
{
  "success": true,
  "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "topic": "click-events",
  "acceptedAt": "2026-09-02T05:12:00.123Z"
}
```

### Kafka 발행

```typescript
await clickProducer.send({
  topic: 'click-events',
  acks: 1,
  compression: CompressionTypes.LZ4,
  messages: [
    {
      key: dto.sessionId,
      value: JSON.stringify({
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      }),
    },
  ],
});
```

---

## 7.2 `POST /events/payment`

| 항목 | 내용 |
|---|---|
| 설명 | 주문/결제 이벤트를 수신해 `payment-events`에 발행 |
| 인증 | `x-api-key` |
| 응답 | Kafka `acks=all` 성공 후 202 |
| Producer Key | `orderId` |
| Topic | `payment-events` |
| Partitions | 2 |
| Compression | LZ4 |
| Kafka Replication | 운영 RF=3 |
| `min.insync.replicas` | 2 |
| Idempotent Producer | true |

### DTO

```typescript
// apps/api-server/src/events/dto/create-payment-event.dto.ts

import {
  IsUUID,
  IsString,
  IsNumber,
  Min,
  IsIn,
  IsISO8601,
  IsNotEmpty,
} from 'class-validator';

export class CreatePaymentEventDto {
  @IsUUID('4')
  eventId: string;

  @IsString()
  @IsNotEmpty()
  orderId: string;

  @IsString()
  @IsNotEmpty()
  userId: string;

  @IsNumber()
  @Min(0)
  amount: number;

  @IsString()
  @IsNotEmpty()
  currency: string;

  @IsString()
  @IsNotEmpty()
  paymentMethod: string;

  @IsIn(['completed', 'failed', 'canceled'])
  status: 'completed' | 'failed' | 'canceled';

  @IsISO8601()
  occurredAt: string;
}
```

### Kafka 발행 파라미터

```typescript
await paymentProducer.send({
  topic: 'payment-events',
  acks: -1,
  compression: CompressionTypes.LZ4,
  messages: [
    {
      key: dto.orderId,
      value: JSON.stringify({
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      }),
    },
  ],
});
```

> `idempotent: true` Producer와 `acks=-1`을 함께 사용한다.
>
> `maxInFlightRequests=5`는 KafkaJS 설정값으로 지정한다.

---

## 7.3 `GET /health/liveness`

```json
{
  "status": "ok"
}
```

외부 의존성을 확인하지 않는다.

---

## 7.4 `GET /health/readiness`

정상:

```json
{
  "status": "ok",
  "info": {
    "kafka": {
      "status": "up"
    }
  },
  "details": {
    "kafka": {
      "status": "up"
    }
  }
}
```

비정상:

```json
{
  "status": "error",
  "error": {
    "kafka": {
      "status": "down",
      "message": "connection timeout"
    }
  }
}
```

---

# 8. api-server 모듈 상세 설계

## 8.1 `EventsModule`

```typescript
// apps/api-server/src/events/events.module.ts

import { Module } from '@nestjs/common';
import { EventsController } from './events.controller';
import { EventsService } from './events.service';
import { KafkaModule } from '../kafka/kafka.module';

@Module({
  imports: [KafkaModule],
  controllers: [EventsController],
  providers: [EventsService],
})
export class EventsModule {}
```

## 8.2 `EventsController`

```typescript
// apps/api-server/src/events/events.controller.ts

import {
  Body,
  Controller,
  HttpCode,
  HttpStatus,
  Post,
  UseGuards,
} from '@nestjs/common';

import { EventsService } from './events.service';
import { CreateClickEventDto } from './dto/create-click-event.dto';
import { CreatePaymentEventDto } from './dto/create-payment-event.dto';
import { ApiKeyGuard } from '../common/guards/api-key.guard';

@UseGuards(ApiKeyGuard)
@Controller('events')
export class EventsController {
  constructor(
    private readonly eventsService: EventsService,
  ) {}

  @Post('click')
  @HttpCode(HttpStatus.ACCEPTED)
  createClickEvent(@Body() dto: CreateClickEventDto) {
    return this.eventsService.publishClickEvent(dto);
  }

  @Post('payment')
  @HttpCode(HttpStatus.ACCEPTED)
  createPaymentEvent(@Body() dto: CreatePaymentEventDto) {
    return this.eventsService.publishPaymentEvent(dto);
  }
}
```

## 8.3 `EventsService`

```typescript
// apps/api-server/src/events/events.service.ts

import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { CreateClickEventDto } from './dto/create-click-event.dto';
import { CreatePaymentEventDto } from './dto/create-payment-event.dto';

@Injectable()
export class EventsService {
  constructor(
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async publishClickEvent(dto: CreateClickEventDto) {
    const ingestedAt = new Date().toISOString();

    await this.kafkaProducer.sendClickEvent({
      key: dto.sessionId,
      value: {
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      },
    });

    return {
      success: true,
      eventId: dto.eventId,
      topic: process.env.KAFKA_CLICK_TOPIC ?? 'click-events',
      acceptedAt: ingestedAt,
    };
  }

  async publishPaymentEvent(dto: CreatePaymentEventDto) {
    const ingestedAt = new Date().toISOString();

    await this.kafkaProducer.sendPaymentEvent({
      key: dto.orderId,
      value: {
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      },
    });

    return {
      success: true,
      eventId: dto.eventId,
      topic: process.env.KAFKA_PAYMENT_TOPIC ?? 'payment-events',
      acceptedAt: ingestedAt,
    };
  }
}
```

## 8.4 `KafkaProducerService`

```typescript
// apps/api-server/src/kafka/kafka-producer.service.ts

import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  ServiceUnavailableException,
} from '@nestjs/common';

import {
  Kafka,
  Producer,
  CompressionTypes,
} from 'kafkajs';

interface SendArgs<T> {
  key: string;
  value: T;
}

@Injectable()
export class KafkaProducerService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly kafka = new Kafka({
    clientId:
      process.env.KAFKA_CLIENT_ID ??
      'logpulse-api-server',

    brokers: (
      process.env.KAFKA_BROKERS ??
      'localhost:9092'
    ).split(','),
  });

  private readonly clickProducer: Producer =
    this.kafka.producer({
      allowAutoTopicCreation: false,
    });

  private readonly paymentProducer: Producer =
    this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      allowAutoTopicCreation: false,
    });

  private clickConnected = false;
  private paymentConnected = false;

  async onModuleInit() {
    await Promise.all([
      this.connectClickProducer(),
      this.connectPaymentProducer(),
    ]);
  }

  async onModuleDestroy() {
    await Promise.allSettled([
      this.clickProducer.disconnect(),
      this.paymentProducer.disconnect(),
    ]);

    this.clickConnected = false;
    this.paymentConnected = false;
  }

  private async connectClickProducer() {
    try {
      await this.clickProducer.connect();
      this.clickConnected = true;
    } catch (err) {
      this.clickConnected = false;
      throw err;
    }
  }

  private async connectPaymentProducer() {
    try {
      await this.paymentProducer.connect();
      this.paymentConnected = true;
    } catch (err) {
      this.paymentConnected = false;
      throw err;
    }
  }

  isConnected(): boolean {
    return this.clickConnected && this.paymentConnected;
  }

  async sendClickEvent<T>({
    key,
    value,
  }: SendArgs<T>) {
    try {
      await this.clickProducer.send({
        topic:
          process.env.KAFKA_CLICK_TOPIC ??
          'click-events',
        acks: 1,
        compression: CompressionTypes.LZ4,
        messages: [
          {
            key,
            value: JSON.stringify(value),
          },
        ],
      });
    } catch (err) {
      this.clickConnected = false;

      throw new ServiceUnavailableException({
        errorCode: 'BROKER_UNAVAILABLE',
        message:
          'click-events 발행에 실패했습니다.',
      });
    }
  }

  async sendPaymentEvent<T>({
    key,
    value,
  }: SendArgs<T>) {
    try {
      await this.paymentProducer.send({
        topic:
          process.env.KAFKA_PAYMENT_TOPIC ??
          'payment-events',
        acks: -1,
        compression: CompressionTypes.LZ4,
        messages: [
          {
            key,
            value: JSON.stringify(value),
          },
        ],
      });
    } catch (err) {
      this.paymentConnected = false;

      throw new ServiceUnavailableException({
        errorCode: 'BROKER_UNAVAILABLE',
        message:
          'payment-events 발행에 실패했습니다.',
      });
    }
  }
}
```

> 중요: `isConnected()`는 단순 Boolean 캐시이므로 실제 Broker 상태와 순간적으로 차이가 날 수 있다. Readiness는 프로세스의 Producer 준비 상태를 확인하는 용도로 사용하고, 실제 이벤트 발행 성공 여부는 반드시 `send()` 결과를 기준으로 판단한다.

## 8.5 `GlobalExceptionFilter`

```typescript
// apps/api-server/src/common/filters/global-exception.filter.ts

import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';

import {
  FastifyReply,
  FastifyRequest,
} from 'fastify';

@Catch()
export class GlobalExceptionFilter
  implements ExceptionFilter
{
  catch(
    exception: unknown,
    host: ArgumentsHost,
  ) {
    const ctx = host.switchToHttp();

    const reply =
      ctx.getResponse<FastifyReply>();

    const request =
      ctx.getRequest<FastifyRequest>();

    let status =
      HttpStatus.INTERNAL_SERVER_ERROR;

    let errorCode = 'INTERNAL_ERROR';

    let message =
      '서버 내부 오류가 발생했습니다.';

    let details: unknown;

    if (exception instanceof HttpException) {
      status = exception.getStatus();

      const res = exception.getResponse();

      if (
        typeof res === 'object' &&
        res !== null
      ) {
        const r =
          res as Record<string, unknown>;

        errorCode =
          (r.errorCode as string) ??
          this.mapStatusToCode(status);

        message =
          (r.message as string) ??
          message;

        details =
          r.details ??
          (Array.isArray(r.message)
            ? r.message
            : undefined);
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

  private mapStatusToCode(
    status: number,
  ): string {
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
```

## 8.6 `ApiKeyGuard`

```typescript
// apps/api-server/src/common/guards/api-key.guard.ts

import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';

import { FastifyRequest } from 'fastify';

@Injectable()
export class ApiKeyGuard
  implements CanActivate
{
  canActivate(
    context: ExecutionContext,
  ): boolean {
    const request =
      context
        .switchToHttp()
        .getRequest<FastifyRequest>();

    const apiKey =
      request.headers['x-api-key'];

    if (
      apiKey !== process.env.API_KEY
    ) {
      throw new UnauthorizedException({
        errorCode: 'UNAUTHORIZED',
        message:
          'API Key가 유효하지 않습니다.',
      });
    }

    return true;
  }
}
```

## 8.7 Health Module

### `KafkaHealthIndicator`

```typescript
// apps/api-server/src/health/kafka.health-indicator.ts

import { Injectable } from '@nestjs/common';
import {
  HealthIndicator,
  HealthIndicatorResult,
  HealthCheckError,
} from '@nestjs/terminus';

import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Injectable()
export class KafkaHealthIndicator
  extends HealthIndicator
{
  constructor(
    private readonly kafkaProducer: KafkaProducerService,
  ) {
    super();
  }

  async isHealthy(
    key: string,
  ): Promise<HealthIndicatorResult> {
    const isConnected =
      this.kafkaProducer.isConnected();

    const result =
      this.getStatus(
        key,
        isConnected,
      );

    if (isConnected) {
      return result;
    }

    throw new HealthCheckError(
      'Kafka connection failed',
      result,
    );
  }
}
```

### `HealthController`

```typescript
// apps/api-server/src/health/health.controller.ts

import {
  Controller,
  Get,
  HttpCode,
} from '@nestjs/common';

import {
  HealthCheck,
  HealthCheckService,
} from '@nestjs/terminus';

import { KafkaHealthIndicator } from './kafka.health-indicator';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly kafkaIndicator:
      KafkaHealthIndicator,
  ) {}

  @Get('liveness')
  @HttpCode(200)
  liveness() {
    return { status: 'ok' };
  }

  @Get('readiness')
  @HealthCheck()
  readiness() {
    return this.health.check([
      () =>
        this.kafkaIndicator
          .isHealthy('kafka'),
    ]);
  }
}
```

---

# 9. consumer-worker 상세 설계

## 9.1 Consumer 역할

### 9.1.1 Consumer 역할 분리 방식

`consumer-worker` 애플리케이션은 하나의 이미지/코드를 공유할 수 있지만,
**컨테이너 하나당 하나의 Consumer 역할만 실행**한다.

환경 변수:

```dotenv
CONSUMER_ROLE=click
```

또는

```dotenv
CONSUMER_ROLE=payment
```

`apps/consumer-worker/src/app.module.ts`는 `CONSUMER_ROLE` 값을 기준으로
다음 중 하나만 등록한다.

```text
CONSUMER_ROLE=click
  -> ClickEventsConsumer만 등록

CONSUMER_ROLE=payment
  -> PaymentEventsConsumer만 등록
```

`click`과 `payment`를 동시에 실행하는 단일 Worker 컨테이너를 운영 토폴로지에서 사용하지 않는다.

Docker Compose 서비스는 다음처럼 **총 5개**를 명시한다.

```text
worker-click-1
worker-click-2
worker-click-3

worker-payment-1
worker-payment-2
```

모든 서비스는 같은 consumer-worker 이미지/코드를 사용하고,
`CONSUMER_ROLE`과 `KAFKA_CLIENT_ID`만 인스턴스별로 다르게 지정한다.

역할에 맞지 않는 Consumer Provider가 등록되지 않았는지 startup log와 실제 Kafka Consumer Group 상태로 검증한다.

### Click Consumer

```text
Kafka click-events
    ↓
Redis dedup
    ↓
BatchBuffer
    ↓
ClickHouse
```

정책:

- 처리량 우선
- `autoCommit: true`
- Redis 장애 시 fail-open
- ClickHouse는 배치 삽입
- 정상 상태에서 Consumer 3대가 Partition 3개와 1:1 매핑

### Payment Consumer

```text
Kafka payment-events
    ↓
Redis 중복 확인
    ↓
ClickHouse 저장
    ↓
저장 성공 후 Redis 처리 완료 표시
    ↓
성공 시 commit
    ↓
실패 지속 시 DLQ
```

Payment Dedup 순서는 다음과 같이 고정한다.

```text
1. Redis에서 eventId 중복 여부 확인
2. 중복이 아니면 ClickHouse 저장 시도
3. ClickHouse 저장 성공 후 Redis에 처리 완료 키를 기록
4. 그 다음 Kafka Offset Commit
```

> **ClickHouse 저장 전에 Redis `SET NX`로 처리 완료를 먼저 표시하지 않는다.**
> 그렇게 하면 `Redis NEW → ClickHouse 실패 → Retry → DUPLICATE` 순서로 실제 데이터가 유실될 수 있다.
>
> ClickHouse 저장 성공 후 Redis 처리 완료 표시 전에 프로세스가 비정상 종료되는 경우에는
> 동일 `eventId`가 다시 처리될 수 있으므로, `payment_events`의 `ReplacingMergeTree`와 `(order_id, event_id)` 정렬 키를 이용해 재적재가 최종적으로 중복 결과를 남기지 않도록 검증한다.

정책:

- 정확성 우선
- `autoCommit: false`
- Redis 장애 시 fail-closed
- ClickHouse 삽입 후 offset commit
- 최대 3회 적재 재시도
- 최종 실패 시 DLQ 전송 후 commit
- 정상 상태에서 Consumer 2대가 Partition 2개와 1:1 매핑

---

## 9.2 ClickEventsConsumer

```typescript
// apps/consumer-worker/src/consumers/click-events.consumer.ts

import {
  Injectable,
  Logger,
  OnModuleInit,
} from '@nestjs/common';

import {
  Kafka,
  EachBatchPayload,
} from 'kafkajs';

import { RedisDedupService } from '../redis/redis-dedup.service';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { BatchBuffer } from '../clickhouse/batch-buffer';
import { retryWithBackoff } from '../common/retry.util';

// Kafka에서 가져오는 '클릭 이벤트' 인터페이스
interface ClickEventEnvelope {
  eventId: string;
  ingestedAt: string;
  payload: {
    userId: string;
    sessionId: string;
    eventType: 'product_click' | 'page_view';
    productId?: string;
    pageUrl: string;
    occurredAt: string;
    metadata?: Record<string, unknown>;
  };
}

@Injectable()
export class ClickEventsConsumer
  implements OnModuleInit
{
  private readonly logger =
    new Logger(ClickEventsConsumer.name);

  private readonly kafka =
    new Kafka({
      clientId:
        process.env.KAFKA_CLIENT_ID ??
        'logpulse-consumer-worker',

      brokers: (
        process.env.KAFKA_BROKERS ??
        'localhost:9092'
      ).split(','),
    });

  private readonly consumer =
    this.kafka.consumer({
      groupId:
        process.env.CLICK_CONSUMER_GROUP_ID ??
        'logpulse-click-loader',
    });

  private readonly buffer =
    new BatchBuffer(
      Number(
        process.env.CLICK_BATCH_MAX_SIZE ??
        500,
      ),
      Number(
        process.env.CLICK_BATCH_FLUSH_MS ??
        1000,
      ),
      (rows) =>
        this.clickhouseWriter
          .insertClickEvents(rows),
    );

  constructor(
    private readonly redisDedup:
      RedisDedupService,

    private readonly clickhouseWriter:
      ClickHouseWriterService,
  ) {}

  async onModuleInit() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: 'click-events',
      fromBeginning: false,
    });

    await this.consumer.run({
      autoCommit: true,

      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
      }: EachBatchPayload) => {
        // 배치 전체를 파싱한다.
        const envelopes =
          batch.messages.map(
            (message) =>
              JSON.parse(
                message.value!.toString(),
              ),
          );

        // Kafka batch 단위 Redis 배치 Dedup 후
        // 선점 성공 이벤트만 버퍼에 적재한다.
        await this.processClickBatch(
          envelopes,
        );

        // 전체 메시지 오프셋을 resolve한다.
        for (const message of batch.messages) {
          resolveOffset(message.offset);
        }

        await heartbeat();
      },
    });
  }

  // Kafka batch 단위로 Redis 배치 Dedup 후
  // 선점 성공 이벤트만 BatchBuffer에 추가한다.
  private async processClickBatch(
    envelopes: ClickEventEnvelope[],
  ): Promise<void> {
    const claimed =
      await this.dedupeAndClaim(envelopes);

    for (const envelope of claimed) {
      this.buffer.add(this.toClickRow(envelope));
    }
  }

  // 내부 중복 제거 → Redis MGET →
  // 신규 후보 Pipeline SET NX 선점 순으로 진행한다.
  private async dedupeAndClaim(
    envelopes: ClickEventEnvelope[],
  ): Promise<ClickEventEnvelope[]> {
    // 1) 동일 batch 내부 eventId 중복 제거
    const unique =
      new Map<string, ClickEventEnvelope>();

    for (const envelope of envelopes) {
      if (!unique.has(envelope.eventId)) {
        unique.set(envelope.eventId, envelope);
      }
    }

    const uniqueList = [...unique.values()];
    const eventIds = uniqueList.map(
      (envelope) => envelope.eventId,
    );

    // 2) Redis Batch Read(MGET) 조회.
    //    실패 시 fail-open 처리.
    let existing: Set<string>;
    try {
      existing = await retryWithBackoff(
        () =>
          this.redisDedup.batchGetExisting(
            'click',
            eventIds,
          ),
        3,
        0,
      );
    } catch (err) {
      this.logger.warn(
        'Redis Batch Read 실패(fail-open)',
      );
      return uniqueList;
    }

    // 3) 기존 key(DUPLICATE) 제외 → 신규 후보
    const candidates = uniqueList.filter(
      (envelope) =>
        !existing.has(envelope.eventId),
    );

    if (candidates.length === 0) {
      return [];
    }

    // 4) Pipeline SET NX 선점.
    //    실패 시 fail-open 처리.
    let claimed: Set<string>;
    try {
      claimed =
        await this.redisDedup.batchMarkIfAbsent(
          'click',
          candidates.map(
            (envelope) => envelope.eventId,
          ),
          Number(
            process.env
              .REDIS_CLICK_DEDUP_TTL_SEC ??
            600,
          ),
        );
    } catch (err) {
      this.logger.warn(
        'Redis Pipeline 선점 실패(fail-open)',
      );
      return candidates;
    }

    // 5) 최종 선점 성공 이벤트만 반환
    return candidates.filter(
      (envelope) =>
        claimed.has(envelope.eventId),
    );
  }

  private toClickRow(
    envelope: ClickEventEnvelope,
  ) {
    const p = envelope.payload;

    return {
      event_id: envelope.eventId,
      user_id: p.userId,
      session_id: p.sessionId,
      event_type: p.eventType,
      product_id: p.productId ?? null,
      page_url: p.pageUrl,
      occurred_at: p.occurredAt,
      ingested_at: envelope.ingestedAt,
      metadata: JSON.stringify(
        p.metadata ?? {},
      ),
    };
  }
}
```

> `BatchBuffer.add()`가 비동기 flush를 시작하므로 실제 운영 구현에서는 **동시 flush에 대한 동기화/직렬화**를 추가하는 것을 권장한다. 특히 이벤트 유입량이 많을 때 `flush()`가 겹쳐 동일 배열을 동시에 처리하지 않도록 보호해야 한다.

---

## 9.3 PaymentEventsConsumer

```typescript
// apps/consumer-worker/src/consumers/payment-events.consumer.ts

import {
  Injectable,
  Logger,
  OnModuleInit,
} from '@nestjs/common';

import {
  Kafka,
  EachBatchPayload,
} from 'kafkajs';

import { RedisDedupService } from '../redis/redis-dedup.service';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { DlqProducerService } from '../dlq/dlq-producer.service';
import { retryWithBackoff } from '../common/retry.util';

// Kafka에서 가져오는 '결제 이벤트' 인터페이스
interface PaymentEventEnvelope {
  eventId: string;
  ingestedAt: string;
  payload: {
    eventId: string;
    orderId: string;
    userId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
    status: 'completed' | 'failed' | 'canceled';
    occurredAt: string;
  };
}

@Injectable()
export class PaymentEventsConsumer
  implements OnModuleInit
{
  private readonly logger =
    new Logger(PaymentEventsConsumer.name);

  private readonly kafka =
    new Kafka({
      clientId:
        process.env.KAFKA_CLIENT_ID ??
        'logpulse-consumer-worker',

      brokers: (
        process.env.KAFKA_BROKERS ??
        'localhost:9092'
      ).split(','),
    });

  private readonly consumer =
    this.kafka.consumer({
      groupId:
        process.env
          .PAYMENT_CONSUMER_GROUP_ID ??
        'logpulse-payment-loader',
    });

  constructor(
    private readonly redisDedup:
      RedisDedupService,

    private readonly clickhouseWriter:
      ClickHouseWriterService,

    private readonly dlqProducer:
      DlqProducerService,
  ) {}

  async onModuleInit() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: 'payment-events',
      fromBeginning: false,
    });

    await this.consumer.run({
      autoCommit: false,

      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
      }: EachBatchPayload) => {
        // 배치 전체를 파싱한다.
        const envelopes =
          batch.messages.map(
            (message) =>
              JSON.parse(
                message.value!.toString(),
              ) as PaymentEventEnvelope,
          );

        // Kafka batch 자체를 처리 단위로 사용한다.
        // Fail-Closed 실패 시 예외가 전파되어 offset을 커밋하지 않는다.
        await this.processBatch(envelopes);

        await heartbeat();

        // 정상 저장 / DUPLICATE / DLQ 성공으로
        // 안전 종료된 offset을 resolve 후 명시적으로 커밋한다.
        for (const message of batch.messages) {
          resolveOffset(message.offset);
        }
        await commitOffsetsIfNecessary(
          uncommittedOffsets(),
        );
      },
    });
  }

  // 내부 중복 제거 → Redis Batch Read →
  // ClickHouse Bulk Insert 1회 →
  // Redis 완료 마킹 Pipeline 순서를 지킨다.
  private async processBatch(
    envelopes: PaymentEventEnvelope[],
  ): Promise<void> {
    // 1) 동일 batch 내부 eventId 중복 제거
    const unique =
      new Map<string, PaymentEventEnvelope>();

    for (const envelope of envelopes) {
      if (!unique.has(envelope.eventId)) {
        unique.set(envelope.eventId, envelope);
      }
    }

    const uniqueList = [...unique.values()];

    // 2) Redis Batch Read(MGET). Fail-Closed.
    const existing = await retryWithBackoff(
      () =>
        this.redisDedup.batchGetExisting(
          'payment',
          uniqueList.map(
            (envelope) => envelope.eventId,
          ),
        ),
      Number(
        process.env.PAYMENT_MAX_RETRY ?? 3,
      ),
    );

    // 3) 기존 key(DUPLICATE) 제외 → 신규 이벤트
    const newEvents = uniqueList.filter(
      (envelope) =>
        !existing.has(envelope.eventId),
    );

    if (newEvents.length === 0) {
      return;
    }

    // 4) ClickHouse Bulk Insert 1회
    const rows = newEvents.map(
      (envelope) =>
        this.toPaymentRow(envelope),
    );

    try {
      await retryWithBackoff(
        () =>
          this.clickhouseWriter
            .insertPaymentEvents(rows),
        Number(
          process.env.PAYMENT_MAX_RETRY ?? 3,
        ),
      );
    } catch (err) {
      // Bulk Insert 최종 실패 → 유효 이벤트 DLQ 이관.
      // (배치 내부 DUPLICATE 이벤트는 DLQ로 보내지 않는다.)
      const reason = (err as Error).message;

      // 민감한 원본 값(orderId/amount 등)은
      // 로그에 남기지 않는다.
      this.logger.error(
        {
          eventCount: newEvents.length,
          topic: 'payment-events',
          error: reason,
        },
        'ClickHouse Bulk Insert 최종 실패, 유효 이벤트 DLQ 이관',
      );

      await this.sendToDlq(newEvents, reason);
      return;
    }

    // 5) ClickHouse 성공 후 Redis 완료 마킹 Pipeline.
    //    Fail-Closed: 실패 시 예외 전파 → offset 미커밋.
    await retryWithBackoff(
      () =>
        this.redisDedup.batchMarkIfAbsent(
          'payment',
          newEvents.map(
            (envelope) => envelope.eventId,
          ),
          Number(
            process.env
              .REDIS_PAYMENT_DEDUP_TTL_SEC ??
            86400,
          ),
        ),
      Number(
        process.env.PAYMENT_MAX_RETRY ?? 3,
      ),
    );
  }

  // 유효 이벤트를 DLQ로 이관한다.
  // Retry 후에도 실패하면 예외를 전파해 offset을 커밋하지 않는다.
  private async sendToDlq(
    events: PaymentEventEnvelope[],
    reason: string,
  ): Promise<void> {
    for (const event of events) {
      await retryWithBackoff(
        () =>
          this.dlqProducer.send(event, reason),
        Number(
          process.env.PAYMENT_MAX_RETRY ?? 3,
        ),
      );
    }
  }

  private toPaymentRow(
    envelope: PaymentEventEnvelope,
  ) {
    const p = envelope.payload;

    return {
      event_id: envelope.eventId,
      order_id: p.orderId,
      user_id: p.userId,
      amount: p.amount,
      currency: p.currency,
      payment_method: p.paymentMethod,
      status: p.status,
      occurred_at: p.occurredAt,
      ingested_at:
        envelope.ingestedAt,
    };
  }
}
```

---

## 9.4 RedisDedupService

```typescript
// apps/consumer-worker/src/redis/redis-dedup.service.ts

import {
  Injectable,
  Logger,
} from '@nestjs/common';

import Redis from 'ioredis';

export type DedupResult =
  | 'NEW'
  | 'DUPLICATE'
  | 'ERROR';

@Injectable()
export class RedisDedupService {
  private readonly logger =
    new Logger(
      RedisDedupService.name,
    );

  private readonly redis =
    new Redis(
      process.env.REDIS_URL ??
      'redis://localhost:6379',
    );

  async checkAndMark(
    topic: string,
    eventId: string,
    ttlSec: number,
  ): Promise<DedupResult> {
    try {
      const result =
        await this.redis.set(
          `dedup:${topic}:${eventId}`,
          '1',
          'EX',
          ttlSec,
          'NX',
        );

      return result === 'OK'
        ? 'NEW'
        : 'DUPLICATE';
    } catch (err) {
      this.logger.warn(
        `Redis dedup 체크 실패(fail-open): ${eventId}`,
      );

      return 'ERROR';
    }
  }

  async isDuplicate(
    topic: string,
    eventId: string,
  ): Promise<boolean> {
    const result = await this.redis.exists(
      `dedup:${topic}:${eventId}`,
    );

    return result === 1;
  }

  async markProcessed(
    topic: string,
    eventId: string,
    ttlSec: number,
  ): Promise<void> {
    await this.redis.set(
      `dedup:${topic}:${eventId}`,
      '1',
      'EX',
      ttlSec,
      'NX',
    );
  }

  // Kafka batch 단위 Redis Batch Read(MGET):
  // 이미 존재하는 eventId 집합을 반환한다.
  async batchGetExisting(
    topic: string,
    eventIds: string[],
  ): Promise<Set<string>> {
    if (eventIds.length === 0) {
      return new Set();
    }

    const keys = eventIds.map(
      (eventId) =>
        `dedup:${topic}:${eventId}`,
    );

    const values = await this.redis.mget(...keys);

    const existing = new Set<string>();
    values.forEach((value, index) => {
      if (value !== null) {
        existing.add(eventIds[index]);
      }
    });

    return existing;
  }

  // Kafka batch 단위 Redis 선점(Pipeline SET NX):
  // 신규 선점에 성공한 eventId 집합을 반환한다.
  async batchMarkIfAbsent(
    topic: string,
    eventIds: string[],
    ttlSec: number,
  ): Promise<Set<string>> {
    if (eventIds.length === 0) {
      return new Set();
    }

    const pipeline = this.redis.pipeline();
    for (const eventId of eventIds) {
      pipeline.set(
        `dedup:${topic}:${eventId}`,
        '1',
        'EX',
        ttlSec,
        'NX',
      );
    }

    const results = await pipeline.exec();

    const claimed = new Set<string>();
    (results ?? []).forEach(
      ([error, result], index) => {
        if (!error && result === 'OK') {
          claimed.add(eventIds[index]);
        }
      },
    );

    return claimed;
  }
}
```

---

## 9.5 BatchBuffer

```typescript
// apps/consumer-worker/src/clickhouse/batch-buffer.ts

export class BatchBuffer<T> {
  private rows: T[] = [];
  private timer: NodeJS.Timeout | null = null;
  private flushing = false;

  constructor(
    private readonly maxSize: number,
    private readonly flushIntervalMs: number,
    private readonly onFlush: (
      rows: T[],
    ) => Promise<void>,
  ) {}

  add(row: T) {
    this.rows.push(row);

    if (
      this.rows.length >=
      this.maxSize
    ) {
      void this.flush();
      return;
    }

    if (!this.timer) {
      this.timer = setTimeout(
        () => void this.flush(),
        this.flushIntervalMs,
      );
    }
  }

  private async flush() {
    if (this.flushing) {
      return;
    }

    this.flushing = true;

    try {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }

      if (this.rows.length === 0) {
        return;
      }

      const batch = this.rows;
      this.rows = [];

      await this.onFlush(batch);
    } finally {
      this.flushing = false;

      if (
        this.rows.length > 0 &&
        !this.timer
      ) {
        this.timer = setTimeout(
          () => void this.flush(),
          this.flushIntervalMs,
        );
      }
    }
  }
}
```

### 9.5.1 Flush 실패 정책

Click 이벤트는 Best-effort / Fail-Open 정책을 사용한다.

`BatchBuffer`의 ClickHouse flush는 다음 정책을 따른다.

```text
1차 flush 실패
  ↓
즉시 재시도 1회
  ↓
실패
  ↓
즉시 재시도 1회 추가
  ↓
실패
  ↓
해당 batch 폐기
  ↓
warn 로그 기록
  ↓
실패 건수 metric/count 증가
```

- 총 재시도 횟수는 **2회**로 고정한다.
- 최종 실패한 Click batch는 v1에서 별도 재처리하지 않는다.
- 프로세스를 중단하여 재처리를 유도하지 않는다.
- `click-events-retry` 토픽은 **v1에서는 사용하지 않으며**, 향후 확장을 위한 토픽으로만 유지한다.
- 구현 완료 기준은 **재시도 2회 → 최종 폐기 → warn 로그/실패 건수 기록**의 명시적 동작이다.

> `click-events-retry`를 실제 재처리 경로로 사용하려면 별도 Retry Consumer와 재처리 정책이 필요하므로 v1 스코프에서는 포함하지 않는다.

---

## 9.6 ClickHouseWriterService

```typescript
// apps/consumer-worker/src/clickhouse/clickhouse-writer.service.ts

import { Injectable } from '@nestjs/common';
import {
  createClient,
  ClickHouseClient,
} from '@clickhouse/client';

@Injectable()
export class ClickHouseWriterService {
  private readonly client:
    ClickHouseClient =
      createClient({
        url:
          process.env
            .CLICKHOUSE_URL ??
          'http://localhost:8123',

        database:
          process.env
            .CLICKHOUSE_DATABASE ??
          'logpulse',

        username:
          process.env
            .CLICKHOUSE_USERNAME ??
          'logpulse_writer',

        password:
          process.env
            .CLICKHOUSE_PASSWORD,
      });

  async insertClickEvents(
    rows: Record<string, unknown>[],
  ) {
    if (rows.length === 0) {
      return;
    }

    await this.client.insert({
      table: 'click_events',
      values: rows,
      format: 'JSONEachRow',
    });
  }

  async insertPaymentEvents(
    rows: Record<string, unknown>[],
  ) {
    if (rows.length === 0) {
      return;
    }

    await this.client.insert({
      table: 'payment_events',
      values: rows,
      format: 'JSONEachRow',
    });
  }
}
```

---

## 9.7 DlqProducerService

```typescript
// apps/consumer-worker/src/dlq/dlq-producer.service.ts

import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import {
  Kafka,
  Producer,
} from 'kafkajs';

@Injectable()
export class DlqProducerService
  implements
    OnModuleInit,
    OnModuleDestroy
{
  private readonly kafka =
    new Kafka({
      clientId:
        'logpulse-dlq-producer',

      brokers: (
        process.env.KAFKA_BROKERS ??
        'localhost:9092'
      ).split(','),
    });

  private readonly producer: Producer =
    this.kafka.producer({
      idempotent: true,
      allowAutoTopicCreation: false,
    });

  async onModuleInit() {
    await this.producer.connect();
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }

  async send(
    originalEnvelope: unknown,
    failureReason: string,
  ) {
    await this.producer.send({
      topic:
        process.env
          .KAFKA_PAYMENT_DLQ_TOPIC ??
        'payment-events-dlq',

      messages: [
        {
          value: JSON.stringify({
            originalEnvelope,
            failureReason,
            failedAt:
              new Date()
                .toISOString(),
          }),
        },
      ],
    });
  }
}
```

---

## 9.8 retryWithBackoff

```typescript
// apps/consumer-worker/src/common/retry.util.ts

export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number,
  baseDelayMs = 200,
): Promise<T> {
  let lastError: unknown;

  for (
    let attempt = 0;
    attempt <= maxRetries;
    attempt++
  ) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;

      if (
        attempt === maxRetries
      ) {
        break;
      }

      const delay =
        baseDelayMs *
        2 ** attempt;

      await new Promise(
        (resolve) =>
          setTimeout(
            resolve,
            delay,
          ),
      );
    }
  }

  throw lastError;
}
```

---

# 10. ClickHouse 스키마 및 마이그레이션

`infra/clickhouse/init.sql`

```sql
CREATE DATABASE IF NOT EXISTS logpulse;

USE logpulse;

CREATE TABLE IF NOT EXISTS click_events
(
    event_id      String,
    user_id       String,
    session_id    String,
    event_type    LowCardinality(String),
    product_id    Nullable(String),
    page_url      String,
    occurred_at   DateTime64(3),
    ingested_at   DateTime64(3) DEFAULT now64(3),
    metadata      String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (session_id, occurred_at)
TTL toDateTime(occurred_at) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS payment_events
(
    event_id       String,
    order_id       String,
    user_id        String,
    amount         Decimal64(2),
    currency       LowCardinality(String),
    payment_method LowCardinality(String),
    status         LowCardinality(String),
    occurred_at    DateTime64(3),
    ingested_at    DateTime64(3) DEFAULT now64(3),
    version        UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (order_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS payment_events_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour_ts)
ORDER BY (hour_ts, status)
AS
SELECT
    toStartOfHour(occurred_at) AS hour_ts,
    status,
    count() AS event_count,
    sum(amount) AS total_amount
FROM payment_events
GROUP BY hour_ts, status;

CREATE USER IF NOT EXISTS logpulse_writer
IDENTIFIED WITH plaintext_password BY '{{WRITER_PASSWORD}}';

GRANT INSERT, SELECT
ON logpulse.*
TO logpulse_writer;

CREATE USER IF NOT EXISTS logpulse_reader
IDENTIFIED WITH plaintext_password BY '{{READER_PASSWORD}}';

GRANT SELECT
ON logpulse.*
TO logpulse_reader;
```

> 운영에서는 `{{WRITER_PASSWORD}}`, `{{READER_PASSWORD}}`를 Secret 또는 배포 환경 변수로 주입한다.

---

# 11. Kafka 토픽 생성 스크립트

`infra/kafka/create-topics.sh`

## 11.1 운영 기준 토픽

```bash
#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka-1:9092}"
KAFKA_BIN="/opt/kafka/bin"

# click-events
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic click-events \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

# payment-events
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic payment-events \
  --partitions 2 \
  --replication-factor 3 \
  --config retention.ms=1209600000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

# payment DLQ
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic payment-events-dlq \
  --partitions 2 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

# click retry
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic click-events-retry \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

echo "LogPulse Kafka topics created."
```

## 11.2 토픽 구성 요약

| Topic | Partitions | RF | min ISR | 주 목적 |
|---|---:|---:|---:|---|
| `click-events` | 3 | 3 | 2 | 처리량 우선 |
| `payment-events` | 2 | 3 | 2 | 정확성/순서 |
| `payment-events-dlq` | 2 | 3 | 2 | payment 최종 실패 격리 |
| `click-events-retry` | 3 | 3 | 2 | v1 미사용. 향후 click 재처리 확장용 |

> 로컬 개발 환경에서 Broker 1대만 기동하는 경우 운영 토픽 생성 스크립트의 RF=3은 사용할 수 없으므로, 로컬 전용 override에서 RF=1 / min.insync.replicas=1을 사용한다.

---

# 12. 로깅 규칙

모든 서비스는 Pino 기반 구조화 로그(JSON)를 남긴다.

## 12.1 최소 공통 필드

```json
{
  "level": "info",
  "time": "2026-09-02T05:14:00.000Z",
  "service": "consumer-worker",
  "context": "PaymentEventsConsumer",
  "eventId": "9c858901-8a57-4791-81fe-4c455b099bc9",
  "topic": "payment-events",
  "msg": "ClickHouse 적재 성공"
}
```

| 필드 | 필수 | 설명 |
|---|---|---|
| `level` | Y | Pino 표준 로그 레벨 |
| `time` | Y | 로그 발생 시각 |
| `service` | Y | `api-server` \| `consumer-worker` |
| `context` | Y | 클래스/모듈명 |
| `eventId` | 이벤트 처리 시 Y | 이벤트 추적 키 |
| `topic` | 이벤트 처리 시 Y | Kafka 토픽 |
| `msg` | Y | 메시지 |

### 중요 이벤트

다음은 반드시 `warn` 이상:

- DLQ 이관
- Redis fail-open
- Redis fail-closed retry
- ClickHouse 재시도 소진
- Kafka Producer 장애
- readiness 실패
- Consumer 재시작
- Kafka rebalance 발생

개인정보와 결제 관련 민감 값은 원문 로깅하지 않는다.

---

# 13. 테스트 전략 및 체크리스트

| 구분 | 대상 | 테스트 항목 |
|---|---|---|
| 단위 | DTO | 필수 필드/UUID/ISO8601 검증 |
| 단위 | ApiKeyGuard | 정상/누락/불일치 |
| 단위 | KafkaProducerService | click/payment 발행 파라미터 및 실패 처리 |
| 단위 | RedisDedupService | `NEW → DUPLICATE` |
| 단위 | BatchBuffer | size/interval flush, 동시 flush |
| 통합 | Nginx → API | Round-Robin으로 두 API에 요청 분산 |
| 통합 | API → Kafka | 실제 Broker 3대 기준 발행/ACK 확인 |
| 통합 | Click Consumer | Partition 3개 ↔ Consumer 3대 매핑 확인 |
| 통합 | Payment Consumer | Partition 2개 ↔ Consumer 2대 매핑 및 orderId 순서 확인 |
| 통합 | Redis + ClickHouse | 중복 이벤트 제거 확인 |
| 통합 | Payment + DLQ | 최종 적재 실패 → DLQ → commit 확인 |
| E2E | 전체 | HTTP → Kafka → Consumer → ClickHouse |
| 부하 | API | 목표 TPS, p95/p99 |
| 부하 | Kafka | Partition별 lag 측정 |
| 장애 주입 | Nginx | API 1대 장애 시 나머지 API 처리 |
| 장애 주입 | Kafka | Broker 1대 장애 시 write/read 지속 |
| 장애 주입 | Redis | click fail-open / payment fail-closed |
| 장애 주입 | ClickHouse | retry / DLQ 경로 확인 |

## 13.1 반드시 확인해야 하는 매핑 테스트

### Click

```text
click-events P0 ← click-consumer-1
click-events P1 ← click-consumer-2
click-events P2 ← click-consumer-3
```

확인 명령 예시:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --group logpulse-click-loader
```

### Payment

```text
payment-events P0 ← payment-consumer-1
payment-events P1 ← payment-consumer-2
```

확인:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --group logpulse-payment-loader
```

### Order 순서

같은 `orderId`에 대해 다음이 유지되는지 확인한다.

```text
event #1
event #2
event #3

→ 동일 key(orderId)
→ 동일 partition
→ Kafka offset 증가 순서 유지
→ consumer 처리 순서 유지
```

---

# 14. 로컬 개발 실행 절차

## 14.1 운영 토폴로지와 로컬 토폴로지의 차이

로컬에서는 리소스 절약을 위해 축소 구성을 사용한다.

### 운영

```text
Nginx 1
API 2
Kafka Broker 3
Click Consumer 3
Payment Consumer 2
```

### 최소 로컬

```text
Nginx 1
API 1
Kafka Broker 1
Click Consumer 1
Payment Consumer 1
Redis 1
ClickHouse 1
```

### 전체 토폴로지 검증용 로컬

Docker Compose 리소스가 허용되면 다음 구성으로 검증한다.

```text
Nginx 1
API 2
Kafka Broker 3
Click Consumer 3
Payment Consumer 2
Redis 1
ClickHouse 1
```

---

## 14.2 인프라 기동

```bash
docker compose \
  -f infra/docker-compose.yml \
  up -d
```

필요 시 상태 확인:

```bash
docker compose \
  -f infra/docker-compose.yml \
  ps
```

---

## 14.3 Kafka Topic 생성

운영 토폴로지:

```bash
docker exec -it logpulse-kafka-1 \
  bash /infra/kafka/create-topics.sh
```

토픽 확인:

```bash
docker exec -it logpulse-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list
```

---

## 14.4 ClickHouse Schema 적용

```bash
docker exec -i logpulse-clickhouse \
  clickhouse-client \
  --multiquery \
  < infra/clickhouse/init.sql
```

---

## 14.5 API 서버 실행

API #1:

```bash
cd apps/api-server
npm install
npm run start:dev
```

API #2는 다른 프로세스/컨테이너에서 실행한다.

```dotenv
PORT=3000
KAFKA_CLIENT_ID=logpulse-api-server-2
```

> 실제 Docker Compose 구성에서는 API #1/#2가 같은 이미지와 애플리케이션 코드를 사용하되 각각 독립 컨테이너로 실행한다.

---

## 14.6 Consumer 실행

Click Consumer 3개와 Payment Consumer 2개는 **동일 worker 이미지/코드를 공유하되 역할별 컨테이너를 분리**한다.

권장 환경 구성:

```text
worker-click-1
worker-click-2
worker-click-3

worker-payment-1
worker-payment-2
```

동일한 Consumer Group ID를 사용하면 Kafka가 Partition을 자동 분배한다.

```dotenv
CLICK_CONSUMER_GROUP_ID=logpulse-click-loader
PAYMENT_CONSUMER_GROUP_ID=logpulse-payment-loader
```

> **중요:** Consumer Process 내부에서 인위적으로 "P0/P1/P2를 고정 지정"하지 않는다. Kafka Consumer Group이 실제 Partition assignment를 관리하게 한다. 구현 목표는 "정상 상태에서 1 Consumer : 1 Partition"이지, 애플리케이션 코드로 Partition을 수동 하드코딩하는 것이 아니다.

---

## 14.7 Health Check

직접 API 접근:

```bash
curl http://localhost:3000/health/liveness
```

```bash
curl http://localhost:3000/health/readiness
```

Nginx 경유:

```bash
curl http://localhost/health/readiness
```

---

## 14.8 Click 이벤트 테스트

```bash
curl -X POST http://localhost/events/click \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d '{
    "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "userId": "u1",
    "sessionId": "s1",
    "eventType": "product_click",
    "pageUrl": "/p/1",
    "occurredAt": "2026-09-02T05:00:00.000Z"
  }'
```

---

## 14.9 Payment 이벤트 테스트

```bash
curl -X POST http://localhost/events/payment \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d '{
    "eventId": "9c858901-8a57-4791-81fe-4c455b099bc9",
    "orderId": "order-55231",
    "userId": "user-1001",
    "amount": 49900,
    "currency": "KRW",
    "paymentMethod": "card",
    "status": "completed",
    "occurredAt": "2026-09-02T05:13:10.500Z"
  }'
```

---

## 14.10 ClickHouse 적재 확인

```bash
docker exec -it logpulse-clickhouse \
  clickhouse-client \
  -q "SELECT count() FROM logpulse.click_events"
```

```bash
docker exec -it logpulse-clickhouse \
  clickhouse-client \
  -q "SELECT count() FROM logpulse.payment_events"
```

---

# 15. 부록: 공통 타입 정의 전체 (`libs/shared`)

## 15.1 Click Event

```typescript
// libs/shared/src/types/click-event.type.ts

export interface ClickEventMetadata {
  referrer?: string;
  device?: string;
  ip?: string;
}

export interface ClickEventPayload {
  eventId: string;
  userId: string;
  sessionId: string;
  eventType:
    | 'product_click'
    | 'page_view';
  productId?: string;
  pageUrl: string;
  occurredAt: string;
  metadata?: ClickEventMetadata;
}
```

## 15.2 Payment Event

```typescript
// libs/shared/src/types/payment-event.type.ts

export interface PaymentEventPayload {
  eventId: string;
  orderId: string;
  userId: string;
  amount: number;
  currency: string;
  paymentMethod: string;
  status:
    | 'completed'
    | 'failed'
    | 'canceled';
  occurredAt: string;
}
```

## 15.3 API Response

```typescript
// libs/shared/src/types/api-response.type.ts

export interface EventEnvelope<T> {
  eventId: string;
  ingestedAt: string;
  payload: T;
}

export interface AcceptedEventResponse {
  success: true;
  eventId: string;
  topic: string;
  acceptedAt: string;
}

export interface ErrorResponse {
  success: false;
  errorCode: ErrorCode;
  message: string;
  details?: unknown;
  timestamp: string;
  path: string;
}

export type ErrorCode =
  | 'VALIDATION_ERROR'
  | 'UNAUTHORIZED'
  | 'RATE_LIMITED'
  | 'BROKER_UNAVAILABLE'
  | 'INTERNAL_ERROR';
```

## 15.4 Kafka Topics

```typescript
// libs/shared/src/constants/kafka-topics.enum.ts

export enum KafkaTopic {
  CLICK_EVENTS = 'click-events',
  PAYMENT_EVENTS = 'payment-events',
  PAYMENT_EVENTS_DLQ = 'payment-events-dlq',
  CLICK_EVENTS_RETRY = 'click-events-retry',
}
```

## 15.5 Redis Key

```typescript
// libs/shared/src/constants/redis-key.util.ts

export function buildDedupKey(
  topic: 'click' | 'payment',
  eventId: string,
): string {
  return `dedup:${topic}:${eventId}`;
}
```

---

# 16. 최종 구성 검증 기준

구현 완료 후 아래 표가 모두 만족되면 현재 아키텍처와 개발 명세가 일치한다.

| 검증 항목 | 기대값 |
|---|---|
| Nginx | 1대 |
| API 서버 | 2대 |
| API 상태 | Stateless |
| API 전체 Rate Limit 기준 | `RATE_LIMIT_MAX`를 API 인스턴스 수로 나눈 프로세스별 제한 |
| API HTTP Adapter | Fastify |
| Nginx 분산 | Round-Robin |
| Kafka Broker | 3대 |
| Kafka Mode | KRaft |
| 운영 RF | 3 |
| 운영 min ISR | 2 |
| `click-events` partitions | 3 |
| Click Consumer Instances | 3 |
| Click Consumer 역할 | `CONSUMER_ROLE=click` |
| Click Consumer Group | 1개 |
| Click 정상 매핑 | Consumer 1 : Partition 1 |
| `payment-events` partitions | 2 |
| Payment Consumer Instances | 2 |
| Payment Consumer 역할 | `CONSUMER_ROLE=payment` |
| Payment Consumer Group | 1개 |
| Payment 정상 매핑 | Consumer 1 : Partition 1 |
| Click key | `sessionId` |
| Payment key | `orderId` |
| Click 처리 전략 | Best-effort / fail-open |
| Payment 처리 전략 | Guaranteed / fail-closed |
| Payment commit | ClickHouse 적재 또는 DLQ 전송 후 |
| API 성공 응답 | 202 Accepted |
| API 실패 표준화 | GlobalExceptionFilter |
| 외부 DB 직접 접근 | API 서버는 하지 않음 |
| 분석 DB | ClickHouse |
| 중복 방지 | Redis |
| 민감 로그 | 마스킹 |

---

## 문서 변경 시 기준

구현 중 아래 항목을 변경해야 하는 경우 상위 문서와의 정합성을 먼저 확인한다.

1. API Request / Response Schema
2. Kafka Topic / Partition / Key
3. Consumer Group ID
4. Redis Dedup 정책
5. ClickHouse 적재 규칙
6. Error Code
7. Nginx Load Balancing 구조
8. API 서버 Stateless 원칙

이 중 하나라도 변경되면 `LogPulse_PRD.md`, `LogPulse_System_Architecture.md`, `DEVELOPMENT_SPEC.md`의 관련 내용을 함께 갱신한다.
