# LogPulse 개발 계획

기준 문서: `DEVELOPMENT_SPEC.md`

## 최종 목표

LogPulse를 단순히 소스 코드만 작성된 상태가 아니라,
실제로 전체 인프라를 기동하고 이벤트를 넣으면 Kafka를 거쳐 Consumer가 처리하고
Redis 중복 방지를 거친 뒤 ClickHouse에 실제 로그 데이터가 저장되는 상태까지 완성한다.

다음 조건을 모두 만족해야 프로젝트 완료로 판단한다.

- Nginx -> API -> Kafka -> Consumer -> Redis / ClickHouse 전체 흐름이 실제로 동작한다.
- Click 이벤트가 ClickHouse `click_events`에 실제 저장된다.
- Payment 이벤트가 ClickHouse `payment_events`에 실제 저장된다.
- 중복 이벤트가 명세에 맞게 처리된다.
- Payment 처리 실패 시 Retry가 동작하고 최종 실패 시 DLQ로 이동한다.
- Kafka Partition과 Consumer Group이 Kafka의 정상적인 Assignment 방식으로 동작한다.
- API Server 2대가 Nginx를 통해 분산 처리된다.
- Redis, Kafka, ClickHouse, API, Consumer 장애 상황과 복구가 실제로 확인된다.
- k6 부하 테스트와 Kafka Lag 확인이 완료된다.
- 코드와 문서의 내용이 `DEVELOPMENT_SPEC.md`와 일치한다.

---

# 진행 상태

- [x] Step 0. 명세 및 기존 프로젝트 확인
- [x] Step 1. 프로젝트 구조 및 Workspace 구성
- [x] Step 2. 공통 계약 및 설정 구성
- [x] Step 3. Docker 인프라 기본 구성
- [x] Step 4. Kafka 클러스터 및 Topic 구성
- [x] Step 5. ClickHouse 및 Redis 구성/검증
- [ ] Step 6. API Server 구현
- [ ] Step 7. Nginx + API Server 2대 구성
- [ ] Step 8. Consumer 공통 기반 구현
- [ ] Step 9. Click Consumer 구현
- [ ] Step 10. Payment Consumer 구현
- [ ] Step 11. 전체 End-to-End 검증
- [ ] Step 12. 장애 및 복구 테스트
- [ ] Step 13. 부하 테스트 및 운영 검증
- [ ] Step 14. 최종 정리 및 문서화

---

# 전체 작업 규칙

1. `DEVELOPMENT_SPEC.md`를 최우선 기준 문서로 사용한다.
2. 코드를 수정하기 전에 반드시 현재 프로젝트 구조와 기존 코드를 먼저 확인한다.
3. 기존 구조를 확인하지 않고 파일이나 모듈을 새로 만들지 않는다.
4. 명세에 없는 구조 변경을 임의로 하지 않는다.
5. API 요청/응답 형식, Event 형식, Kafka Topic, Partition 수, Key, Consumer Group ID, Redis 정책, ClickHouse 적재 방식 등을 임의로 변경하지 않는다.
6. Kafka Partition을 코드에서 직접 지정하지 않는다.
7. `P0`, `P1`, `P2`와 같은 Partition을 직접 Consumer에 하드코딩하지 않는다.
8. Kafka Consumer Group의 Partition Assignment를 Kafka가 담당하도록 구현한다.
9. API Server는 Stateless 구조를 유지한다.
10. API Server가 ClickHouse에 직접 로그를 저장하지 않는다.
11. API Server가 Consumer용 Redis Dedup 처리를 직접 수행하지 않는다.
12. Click 이벤트 Kafka Key는 반드시 `sessionId`를 사용한다.
13. Payment 이벤트 Kafka Key는 반드시 `orderId`를 사용한다.
14. Click 처리의 Redis 오류 정책은 명세의 Fail-Open 정책을 따른다.
15. Payment 처리의 Redis 오류 정책은 명세의 Fail-Closed 정책을 따른다.
16. Payment는 실제 저장 성공 또는 명세에 맞는 DLQ 처리 전에 Offset을 Commit하지 않는다.
17. 민감한 Payment 원본 값을 로그에 남기지 않는다.
18. 구조화된 Pino 로그를 사용하고 명세에서 요구하는 필드를 포함한다.
19. 필요하지 않은 추상화, Wrapper, 패키지, 파일을 추가하지 않는다.
20. Build가 성공했다고 해서 Step을 완료 처리하지 않는다.
21. 현재 Step의 검증이 실패하면 다음 Step으로 넘어가지 않는다.
22. 문제가 발견되면 숨기지 말고 원인을 수정한 뒤 다시 검증한다.
23. 의도적으로 명세와 다른 구현을 할 경우 이유를 문서에 남긴다.

---

# 모든 Step의 공통 진행 순서

각 Step은 항상 다음 순서로 진행한다.

1. 현재 프로젝트 상태를 확인한다.
2. 현재 Step에 해당하는 것만 구현한다.
3. Format / Lint / TypeCheck / Build를 실행한다.
4. 필요한 Unit / Integration Test를 실행한다.
5. 실제 실행 환경에서 Runtime 검증을 한다.
6. 실제 결과를 확인한다.
7. 문제가 있으면 수정한다.
8. 수정 후 다시 검증한다.
9. 모든 완료 조건이 충족되었을 때만 Step 체크박스를 체크한다.
10. 그 다음 Step으로 이동한다.

---

# Step 0. 명세 및 기존 프로젝트 확인

## 작업 내용

- `DEVELOPMENT_SPEC.md` 전체 내용을 확인한다.
- 현재 Repository 구조를 확인한다.
- 현재 `package.json`을 확인한다.
- 현재 NestJS 애플리케이션 구조를 확인한다.
- 기존 Docker 설정을 확인한다.
- 기존 Nginx 설정을 확인한다.
- 기존 Kafka 설정을 확인한다.
- 기존 Redis 설정을 확인한다.
- 기존 ClickHouse 설정을 확인한다.
- 현재 구현과 명세가 충돌하는 부분을 찾는다.
- 아직 없는 모듈과 인프라를 확인한다.

## 최종 목표 구조

### 실제 운영 형태

- Nginx 1대
- API Server 2대
- Kafka Broker 3대
- Click Consumer 3대
- Payment Consumer 2대
- Redis 1대
- ClickHouse 1대

### 최소 로컬 실행 형태

- Nginx 1대
- API Server 1대
- Kafka Broker 1대
- Click Consumer 1대
- Payment Consumer 1대
- Redis 1대
- ClickHouse 1대

### 전체 로컬 검증 형태 (로컬 사양이 충분하므로, 최소 실행 구조 대신 이 아래 구조로 가져가겠다.)

- Nginx 1대
- API Server 2대
- Kafka Broker 3대
- Click Consumer 3대
- Payment Consumer 2대
- Redis 1대
- ClickHouse 1대

## 완료 조건

- 현재 프로젝트 구조를 이해했다.
- 명세와 기존 코드의 충돌을 확인했다.
- 필요한 구현 범위를 확인했다.
- 프로젝트 구조를 모르는 상태에서 구현을 시작하지 않았다.

- [ ] Step 0 완료

---

# Step 1. 프로젝트 구조 및 Workspace 구성

## 작업 내용

필요한 경우 프로젝트 구조를 다음 방향으로 정리한다.

```text
apps/
  api-server/
  consumer-worker/

libs/
  shared/

infra/
  nginx/
  kafka/
  clickhouse/

load-test/

docker-compose.yml
package.json
tsconfig.json
```

구성할 내용:

- Workspace 설정
- Shared Library 설정
- API Server 기본 실행 구조
- Consumer Worker 기본 실행 구조
- 공통 TypeScript 설정
- 환경 변수 설정 구조

이 단계에서는 아직 실제 비즈니스 로직을 구현하지 않는다.

## 검증

다음을 모두 확인한다.

- dependency 설치 성공
- Workspace 해석 성공
- API Server Build 성공
- Consumer Worker Build 성공
- Shared Library Build 성공

## 완료 조건

각 애플리케이션을 독립적으로 Build할 수 있다.

- [ ] Step 1 완료

---

# Step 2. 공통 계약 및 설정 구성

## 작업 내용

명세에 맞는 공통 타입과 상수를 구현한다.

포함 대상:

- `ClickEventPayload`
- `PaymentEventPayload`
- `EventEnvelope`
- `AcceptedEventResponse`
- `ErrorResponse`
- Kafka Topic 상수
- Consumer Group ID 상수
- Redis Key 생성 유틸리티
- 필요한 공통 환경 변수 타입

Validation 대상:

- 필수 필드
- 데이터 타입
- Enum 값
- 명세에서 요구하는 Event Version 정보

API와 Consumer에서 동일한 Event 계약을 각각 중복 정의하지 않는다.

## 검증

- Shared Build 성공
- API에서 Shared 타입 Import 성공
- Consumer에서 Shared 타입 Import 성공
- 잘못된 DTO가 Validation에서 거절되는 테스트 통과
- 정상 DTO가 Validation을 통과하는 테스트 통과

## 완료 조건

공통 Event 계약의 기준이 하나로 통일되어 있다.

- [ ] Step 2 완료

---

# Step 3. Docker 인프라 기본 구성

## 작업 내용

전체 실행 환경의 기본 Docker 구성을 만든다.

구성 대상:

- Docker Compose
- Kafka Broker 1
- Kafka Broker 2
- Kafka Broker 3
- Redis
- ClickHouse
- Nginx
- 필요한 Health Check
- 필요한 Persistent Volume
- Docker Network
- 환경 변수 구성

Kafka는 명세에 맞는 KRaft 방식으로 구성한다.

이 단계에서는 애플리케이션보다 먼저 인프라 연결 상태를 확실하게 만든다.

## 실행

```bash
docker compose up -d
```

## 검증

실행 후 실제 상태를 확인한다.

- 필요한 Container가 모두 실행된다.
- Kafka Broker가 계속 정상 실행 상태를 유지한다.
- Redis 연결이 가능하다.
- ClickHouse 연결이 가능하다.
- Nginx가 설정 오류 없이 실행된다.
- Container가 계속 재시작되는 문제가 없다.
- 각 Container 로그에 치명적인 Startup Error가 없다.

## 완료 조건

전체 기반 인프라가 정상 기동되고 안정적으로 유지된다.

- [ ] Step 3 완료

---

# Step 4. Kafka 클러스터 및 Topic 구성

## 작업 내용

명세에 맞게 Kafka를 구성한다.

### Kafka Cluster

- KRaft Broker 3대
- Replication Factor = 3
- `min.insync.replicas = 2`가 필요한 부분은 명세대로 설정

### Topic

- `click-events`
- `payment-events`
- `payment-events-dlq`
- `click-events-retry`

### Partition

- `click-events` = 3
- `payment-events` = 2
- `payment-events-dlq` = 2
- `click-events-retry` = 3

### Producer 설정

Click:

- `acks=1`
- Compression = LZ4

Payment:

- `acks=all`
- Idempotent Producer

## 검증

Kafka CLI 또는 동등한 방법으로 실제 상태를 확인한다.

- Broker 3대가 모두 Cluster에 존재한다.
- KRaft Controller / Quorum이 정상이다.
- Topic이 모두 생성되어 있다.
- Partition 수가 정확하다.
- Replication Factor가 정확하다.
- ISR 상태가 정상이다.
- Topic 설정이 명세와 일치한다.

애플리케이션 Consumer를 만들기 전에 테스트 메시지를 직접 Producer/Consumer로 넣고 읽어본다.

## 완료 조건

Kafka가 실제 3 Broker Cluster로 정상 동작하고 필요한 Topic 구성이 모두 검증되었다.

- [ ] Step 4 완료

---

# Step 5. ClickHouse 및 Redis 구성/검증

## ClickHouse 작업

명세에 맞게 ClickHouse 스키마를 구성한다.

필수 테이블:

- `click_events`
- `payment_events`

적용 사항:

- Click 이벤트는 MergeTree
- Payment 이벤트는 Version이 포함된 ReplacingMergeTree
- 필요한 Partition
- 필요한 ORDER BY
- 필요한 TTL
- 명세에 있는 Payment Materialized View / 집계 구성

## Redis 작업

Redis 연결을 구성한다.

Dedup Key 형식:

```text
dedup:${topic}:${eventId}
```

필수 명령 형태:

```text
SET key 1 EX ttl NX
```

## 중요한 Payment 설계 검증

Payment에서 다음과 같은 상황이 발생하면 안 된다.

```text
Redis에 먼저 처리 완료 표시
        ->
ClickHouse 저장 실패
        ->
Kafka Retry
        ->
Redis가 DUPLICATE로 판단
        ->
실제 데이터가 저장되지 않음
```

즉, Redis 중복 처리가 ClickHouse 실패와 결합되었을 때 데이터가 유실되지 않도록 실제 처리 순서를 설계하고 검증한다.

## 검증

- ClickHouse 테이블 생성 성공
- Schema가 명세와 일치
- Partition / ORDER BY / TTL 확인
- ClickHouse에 직접 테스트 데이터 Insert 성공
- Redis 연결 성공
- Redis Dedup Key 저장 성공
- TTL 동작 확인
- `NEW -> DUPLICATE` 동작 확인
- Payment 실패 시 Retry가 가능한 구조인지 확인

## 완료 조건

Redis와 ClickHouse가 독립적으로 정상 연결되고 필요한 Schema와 Dedup 정책이 검증되었다.

- [ ] Step 5 완료

---

# Step 6. API Server 구현

## 작업 내용

Fastify 기반 NestJS API Server를 구현한다.

포함:

- Fastify Bootstrap
- ValidationPipe
- Helmet
- CORS
- Rate Limit
- ApiKeyGuard
- GlobalExceptionFilter
- Health API
- Readiness API
- Kafka Producer
- Events Controller
- Events Service

API:

```text
POST /events/click
POST /events/payment
```

## 동작 규칙

1. Request Validation
2. API Key 인증
3. Kafka Publish
4. Kafka Publish 성공 후 `202 Accepted`
5. ClickHouse에 직접 저장하지 않는다.
6. Consumer용 Redis Dedup을 API에서 직접 수행하지 않는다.

Kafka Key:

- Click -> `sessionId`
- Payment -> `orderId`

## Rate Limit 검증

API Server가 2대이므로 프로세스별 Rate Limit을 그대로 사용하면 실제 전체 한도가 의도보다 2배가 될 수 있다.

따라서 명세에 맞는 Rate Limit 범위를 결정하고 구현한다.

가능한 범위:

- API 프로세스별
- Nginx 기준
- Redis를 사용하는 공통 Rate Limit

의도하지 않은 2배 제한이 발생하지 않도록 한다.

## Nginx Proxy 고려

Nginx 뒤에서 API가 실행되므로 Client IP 처리가 올바르게 되도록 `trustProxy` 설정을 확인한다.

## 검증

Unit / Integration Test:

- DTO Validation
- 잘못된 API Key
- 정상 API Key
- Click Kafka Publish
- Payment Kafka Publish
- Kafka Producer 실패
- HTTP 상태 코드
- Error Response 형식

실제 Runtime:

```text
HTTP Request
 -> API
 -> Kafka Topic
```

Kafka에 실제 메시지가 들어갔는지 확인한다.

## 완료 조건

두 API가 실제 Kafka Cluster와 연결되고 Kafka Publish 성공 시에만 `202`를 반환한다.

- [ ] Step 6 완료

---

# Step 7. Nginx + API Server 2대 구성

## 작업 내용

구성:

- API Server #1
- API Server #2
- Nginx

Nginx 설정:

- upstream
- Round Robin
- Forward Header
- 필요한 Proxy Timeout
- 필요한 Request Size
- Step 6에서 결정한 Rate Limit 적용 범위

API Server는 Stateless여야 한다.

## 검증

Nginx를 통해 반복 요청을 보낸다.

확인 사항:

- API #1과 API #2 모두 요청을 처리한다.
- 특정 한 인스턴스에만 고정되지 않는다.
- Nginx를 통해 정상적으로 API가 동작한다.
- API #1을 종료해도 API #2를 통해 서비스가 계속된다.
- API #2를 종료해도 API #1을 통해 서비스가 계속된다.

단순히 Nginx에서 `202`가 반환되는지만 확인하지 않는다.

어떤 API 인스턴스가 처리했는지를 구조화 로그로 안전하게 확인한다.

## 완료 조건

Nginx가 API 2대에 정상적으로 Round Robin을 수행하고,
API 한 대가 장애가 나도 다른 인스턴스로 서비스가 계속된다.

- [ ] Step 7 완료

---

# Step 8. Consumer 공통 기반 구현

## 작업 내용

공통 Consumer 기반을 구현한다.

포함:

- Kafka Consumer 설정
- Consumer Group 설정
- Redis Module
- Redis Dedup Service
- ClickHouse Writer
- Retry Utility
- DLQ Producer
- Graceful Shutdown
- Structured Logging

Consumer 실행 역할은 분리할 수 있어야 한다.

권장 구성:

```text
click-consumer-1
click-consumer-2
click-consumer-3

payment-consumer-1
payment-consumer-2
```

한 Worker가 무조건 두 Topic을 모두 처리해야 하는 구조로 만들지 않는다.

Partition Assignment는 반드시 Kafka Consumer Group이 담당한다.

## 로그 규칙

Pino Structured JSON 사용.

필수 정보 예시:

- level
- time
- service
- context
- eventId
- topic
- msg

다음 상황은 warn 이상으로 기록한다.

- DLQ
- Redis Fail-Open / Fail-Closed 관련 Retry
- ClickHouse Retry 최종 실패
- Kafka Producer 실패
- Readiness 실패
- Consumer 재시작
- Kafka Rebalance

민감한 Payment 원본 값을 로그에 출력하지 않는다.

## 검증

- Consumer 시작 성공
- Kafka 연결 성공
- Redis 연결 성공
- ClickHouse 연결 성공
- Graceful Shutdown 동작
- 로그가 JSON 형식으로 출력됨

## 완료 조건

Consumer가 실제 인프라에 연결되고 공통 기능이 정상 동작한다.

- [ ] Step 8 완료

---

# Step 9. Click Consumer 구현

## 작업 내용

Click 이벤트 Consumer를 구현한다.

필수 동작:

- 명세에 맞는 Consumer Group
- `autoCommit=true`
- Redis Fail-Open
- Batch Buffer
- Size 기준 Flush
- Interval 기준 Flush
- ClickHouse Batch Insert
- 동시 Flush 방지
- Flush 실패 처리

## BatchBuffer 주의사항

Flush 도중 오류가 발생했을 때 이벤트가 조용히 버려지는 구조를 만들지 않는다.

다음 상황의 동작을 코드로 명확하게 정의한다.

- Size Trigger Flush
- Interval Trigger Flush
- Flush 중 추가 Flush 요청
- Flush 실패
- Flush 재시도 또는 실패 처리

## Consumer 수

- Click Consumer 3대
- 동일 Consumer Group
- `click-events` Partition 3개

정상적인 최종 상태:

```text
Partition 3개
Consumer 3개
Consumer Group 1개
```

대략 1 Consumer : 1 Partition이 되는 것을 목표로 한다.

단, 이것은 정상 상태의 목표일 뿐이며 Consumer 장애가 발생했을 때 Rebalance까지 없어지는 것을 의미하지 않는다.

## 검증

실제 API를 통해 Click 이벤트를 전송한다.

전체 흐름을 확인한다.

```text
API
 -> Kafka click-events
 -> Click Consumer
 -> Redis Dedup
 -> ClickHouse
```

확인:

- Consumer 3대가 모두 실행된다.
- Kafka가 정상적으로 Partition을 분배한다.
- Partition을 코드에서 직접 지정하지 않았다.
- 중복 이벤트가 명세대로 처리된다.
- ClickHouse Row가 실제로 생성된다.
- Batch Size에 의한 Flush가 동작한다.
- Interval에 의한 Flush가 동작한다.
- 동시에 두 Flush가 실행되지 않는다.
- Redis 장애 시 Click은 Fail-Open으로 동작한다.
- Flush 실패 시 데이터 유실이 발생하지 않도록 처리된다.

ClickHouse Query로 실제 저장 결과를 확인한다.

## 완료 조건

Click 이벤트가 HTTP부터 ClickHouse까지 실제로 전달되고 저장되는 것을 확인한다.

- [ ] Step 9 완료

---

# Step 10. Payment Consumer 구현

## 작업 내용

Payment 이벤트 Consumer를 구현한다.

필수 동작:

- 명세에 맞는 Consumer Group
- `autoCommit=false`
- Redis Fail-Closed
- Exponential Backoff Retry
- 명세의 최대 Retry 횟수
- ClickHouse 저장
- DLQ Producer
- 성공 또는 안전한 DLQ 처리 후 Offset Commit

Kafka Key:

```text
orderId
```

같은 `orderId`를 가진 이벤트는 동일 Partition 내에서 순서를 유지할 수 있어야 한다.

단, 모든 Payment 이벤트의 전역적인 순서를 보장하는 것은 아니다.

## 매우 중요한 처리 순서 검증

다음 상황을 모두 명확하게 처리해야 한다.

1. Redis 연결 자체가 불가능한 경우
2. Redis가 DUPLICATE를 반환하는 경우
3. Redis에 처리 표시 후 ClickHouse 저장이 실패하는 경우
4. ClickHouse Retry가 모두 실패한 경우
5. DLQ Publish가 성공한 경우
6. DLQ Publish가 실패한 경우
7. Kafka Offset Commit이 성공/실패한 경우

Payment에서 데이터가 유실되거나,
실제로 저장되지 않았는데 DUPLICATE로 인해 재처리가 막히는 상황이 없어야 한다.

## Consumer 수

- Payment Consumer 2대
- 동일 Consumer Group
- `payment-events` Partition 2개

정상적인 최종 상태:

```text
Partition 2개
Consumer 2개
Consumer Group 1개
```

## 검증

실제 API를 통해 Payment 이벤트를 전송한다.

전체 흐름:

```text
API
 -> Kafka payment-events
 -> Payment Consumer
 -> Redis Dedup
 -> ClickHouse
```

실패 흐름도 실제 테스트한다.

```text
ClickHouse 실패
 -> Retry
 -> Retry 모두 실패
 -> payment-events-dlq
 -> Offset Commit
```

추가로 확인:

- 같은 `orderId`의 이벤트 순서
- 중복 Payment 이벤트
- Redis 장애 시 Fail-Closed
- 실제 Kafka Offset
- ClickHouse 실제 Row

단순 로그만 보고 성공했다고 판단하지 않는다.

## 완료 조건

Payment 이벤트의 정상 처리, 중복 처리, Retry, DLQ, Offset Commit이 실제 실행 환경에서 모두 검증된다.

- [ ] Step 10 완료

---

# Step 11. 전체 End-to-End 검증

## 목적

중간 서비스를 수동으로 대신 처리하지 않고,
실제 HTTP 요청 하나가 전체 시스템을 통과하는지 검증한다.

## Click 전체 검증

Nginx를 통해 Click 이벤트를 전송한다.

```text
Client
 -> Nginx
 -> API #1 또는 API #2
 -> Kafka click-events
 -> Click Consumer
 -> Redis
 -> ClickHouse click_events
```

ClickHouse에 실제 Row가 존재하는지 확인한다.

## Payment 전체 검증

Nginx를 통해 Payment 이벤트를 전송한다.

```text
Client
 -> Nginx
 -> API #1 또는 API #2
 -> Kafka payment-events
 -> Payment Consumer
 -> Redis
 -> ClickHouse payment_events
```

ClickHouse에 실제 Row가 존재하는지 확인한다.

## 중복 검증

동일한 Event를 여러 번 전송한다.

확인:

- Click 중복 처리
- Payment 중복 처리
- 의도하지 않은 추가 저장이 없는지

Payment의 `ReplacingMergeTree`는 Merge가 비동기로 진행될 수 있으므로
단순 `count()`만 보고 즉시 중복 제거가 끝났다고 판단하지 않는다.

필요하면 `FINAL` 또는 동등한 검증 방법을 사용한다.

## 완료 조건

Click과 Payment 모두 다음 흐름이 실제로 검증된다.

```text
HTTP
 -> Nginx
 -> API
 -> Kafka
 -> Consumer
 -> Redis
 -> ClickHouse
```

- [ ] Step 11 완료

---

# Step 12. 장애 및 복구 테스트

## API Server 장애

API #1을 종료한다.

확인:

- Nginx가 계속 요청을 받는다.
- API #2가 계속 처리한다.

API #1을 다시 실행한다.

확인:

- 정상적으로 다시 Cluster에 복귀한다.
- 다시 요청 분산이 이루어진다.

## Kafka Broker 장애

Kafka Broker 1대를 종료한다.

확인:

- RF / min ISR 설정 범위에서 Cluster가 정상적으로 동작한다.
- Producer가 명세에 맞게 동작한다.
- Consumer가 명세에 맞게 동작한다.
- 필요한 Partition Recovery / Reassignment가 발생한다.

Broker를 다시 실행한다.

확인:

- Broker가 다시 Cluster에 참여한다.
- Replica 상태가 회복된다.

## Redis 장애

Click:

- Fail-Open 확인

Payment:

- Fail-Closed 확인

Redis를 복구한 후 다시 정상 처리되는지 확인한다.

## ClickHouse 장애

ClickHouse를 종료한다.

Click:

- Batch Flush 실패 시 명세와 구현한 실패 정책대로 동작하는지 확인

Payment:

- Retry 동작
- Offset 조기 Commit이 없는지 확인
- Retry 최종 실패 시 DLQ 동작 확인

ClickHouse를 복구한 후 Consumer가 다시 정상 처리되는지 확인한다.

## Consumer Rebalance

Click Consumer 하나를 종료한다.

확인:

- Kafka가 남은 Consumer에게 Partition을 재할당한다.

Payment Consumer 하나를 종료한다.

확인:

- Kafka가 남은 Payment Consumer에게 Partition을 재할당한다.

Consumer를 다시 실행하고 정상적으로 Group에 복귀하는지 확인한다.

Partition을 수동으로 지정해서 문제를 해결하지 않는다.

## 완료 조건

필요한 장애와 복구 시나리오를 실제 환경에서 재현했고,
로그만 보는 것이 아니라 Kafka / Redis / ClickHouse / API의 실제 상태까지 확인했다.

- [ ] Step 12 완료

---

# Step 13. 부하 테스트 및 운영 검증

## 작업 내용

k6 테스트를 구성하거나 기존 테스트를 완성한다.

테스트 대상:

- Click 이벤트 수집
- Payment 이벤트 수집
- 지속적인 정상 트래픽
- 동시 API 요청

수집할 지표:

- 총 Request 수
- 성공률
- p50
- p95
- p99
- 최대 지연 시간
- Throughput
- Kafka Consumer Lag
- CPU
- Memory

## Backpressure 확인

부하를 주면서 확인한다.

- Kafka Lag이 어떻게 증가하는지
- Consumer가 Lag을 다시 줄이는지
- ClickHouse Batch Insert가 정상 동작하는지
- Payment Retry / DLQ가 비정상적으로 증가하지 않는지

## 로그 검증

- Structured JSON 로그가 정상적으로 생성되는지
- 필요한 Context가 포함되는지
- 민감한 값이 노출되지 않는지

## 완료 조건

- 부하 테스트가 통과한다.
- p95 / p99 결과를 기록했다.
- Kafka Lag을 확인했다.
- CPU / Memory 사용량을 확인했다.
- 부하 상황에서 제어되지 않는 메모리 증가나 CPU 폭주가 없는지 확인했다.

- [ ] Step 13 완료

---

# Step 14. 최종 정리 및 문서화

## 코드 정리

다음을 검토한다.

- 사용하지 않는 파일
- 사용하지 않는 dependency
- 중복된 Event 계약
- 중복 Utility
- Dead Code
- 잘못된 환경 변수
- 설정 불일치
- 불필요한 추상화

## 문서 정합성

다음 내용은 코드와 문서가 반드시 일치해야 한다.

- API Schema
- Kafka Topic 이름
- Partition 수
- Replication 설정
- Kafka Message Key
- Consumer Group ID
- Redis Dedup 정책
- ClickHouse Schema
- ClickHouse 적재 방식
- Error Code
- Nginx Load Balancing
- API Stateless 구조
- Retry 정책
- DLQ 정책

## 최종 전체 실행

전체 로컬 검증 형태를 실행한다.

```text
Nginx 1
API 2
Kafka 3
Click Consumer 3
Payment Consumer 2
Redis 1
ClickHouse 1
```

가능하면 테스트 데이터를 정리한 깨끗한 상태에서 최종 테스트를 1회 수행한다.

## 최종 Click 검증

```text
HTTP
 -> Nginx
 -> API
 -> Kafka
 -> Click Consumer
 -> Redis
 -> ClickHouse
```

ClickHouse `click_events`에 실제 데이터가 저장되는 것을 확인한다.

## 최종 Payment 검증

```text
HTTP
 -> Nginx
 -> API
 -> Kafka
 -> Payment Consumer
 -> Redis
 -> ClickHouse
```

ClickHouse `payment_events`에 실제 데이터가 저장되는 것을 확인한다.

## 최종 Payment 실패 검증

```text
Payment
 -> ClickHouse 실패
 -> Retry
 -> Retry 실패
 -> DLQ
 -> Offset Commit
```

실제 Kafka DLQ Topic과 Offset 상태를 확인한다.

## 최종 완료 조건

다음 조건을 모두 충족해야 프로젝트 완료로 판단한다.

- 모든 Step 체크 완료
- Build 통과
- TypeCheck 통과
- Test 통과
- Docker 인프라 정상 기동
- Kafka 3 Broker Cluster 정상
- Kafka Topic / Partition / RF / ISR 검증 완료
- API Server 2대 정상
- Nginx Round Robin 정상
- Click Consumer 3대 정상
- Payment Consumer 2대 정상
- Redis Dedup 정상
- ClickHouse 실제 데이터 저장 확인
- Click 중복 처리 확인
- Payment 중복 처리 확인
- Payment Retry 확인
- Payment DLQ 확인
- Offset Commit 검증
- Consumer Rebalance 확인
- API 장애 복구 확인
- Kafka Broker 장애 복구 확인
- Redis 장애 복구 확인
- ClickHouse 장애 복구 확인
- k6 부하 테스트 완료
- Kafka Lag 확인
- 로그 구조 및 민감정보 노출 여부 확인
- 문서와 실제 구현 내용 일치

- [ ] Step 14 완료

---

# 최종 완료 기준

다음 중 하나만 성공했다고 프로젝트 완료로 판단하지 않는다.

- 소스 코드가 만들어졌다.
- TypeScript Compile이 된다.
- Docker Compose가 올라온다.
- Kafka Topic이 생성된다.
- Unit Test가 통과한다.

반드시 아래 순서까지 모두 통과해야 한다.

```text
코드 구현
  ->
Build / TypeCheck
  ->
Unit / Integration Test
  ->
인프라 실제 기동
  ->
Kafka Cluster 실제 검증
  ->
API 실제 검증
  ->
Nginx + API 2대 실제 검증
  ->
Click Consumer 실제 검증
  ->
Payment Consumer 실제 검증
  ->
Redis 실제 검증
  ->
ClickHouse 실제 저장 검증
  ->
Click E2E
  ->
Payment E2E
  ->
중복 처리 검증
  ->
장애 / 복구 검증
  ->
Retry / DLQ 검증
  ->
Consumer Rebalance 검증
  ->
부하 테스트
  ->
문서 정합성 확인
  ->
프로젝트 완료
```

각 Step은 구현이 끝난 시점이 아니라,
해당 Step의 검증 조건까지 모두 통과한 시점에만 체크한다.
