# LogPulse 개발 계획

기준 문서: `DEVELOPMENT_SPEC.md`

> 이번 개정의 핵심은 **Click Consumer의 Redis Dedup 배치화**와 **Payment Consumer의 Kafka 배치 단위 마이크로 배치화**를 서로 다른 작업 단위로 분리하는 것이다.
>
> - Click: Kafka `eachBatch`에서 Redis 중복 조회/선점을 배치화하고, 기존 ClickHouse용 `BatchBuffer`는 유지한다.
> - Payment: Kafka `eachBatch`의 `batch.messages` 자체를 처리 단위로 사용하고, ClickHouse Bulk Insert를 수행한다.
> - 두 작업은 반드시 별도 Step 및 별도 Git Commit으로 수행한다.

## 최종 목표

LogPulse를 실제로 전체 인프라에 기동하고 이벤트를 넣었을 때 다음 흐름이 검증되는 상태로 완성한다.

```text
Client
  -> Nginx
  -> API Server 2대
  -> Kafka
  -> Click / Payment Consumer
  -> Redis Dedup
  -> ClickHouse
```

다음 조건을 모두 만족해야 프로젝트 완료로 판단한다.

- Click 이벤트가 ClickHouse `click_events`에 저장된다.
- Payment 이벤트가 ClickHouse `payment_events`에 저장된다.
- Click과 Payment 모두 Kafka batch 단위 Redis Dedup을 사용한다.
- Click은 Best-effort / Fail-Open 정책을 유지한다.
- Payment는 Fail-Closed 정책을 유지한다.
- Payment는 ClickHouse 저장 성공 전에 Redis 완료 마킹을 하지 않는다.
- Payment ClickHouse 저장 실패 시 Retry 후 DLQ로 이동한다.
- DLQ 발행 실패 시 Offset을 커밋하지 않고 재처리할 수 있다.
- Kafka Consumer Group이 Partition Assignment를 담당한다.
- API Server 2대와 Consumer 5대가 정상 동작한다.
- 장애 / 복구 / Rebalance / Lag / 부하 테스트가 완료된다.
- 코드와 `DEVELOPMENT_SPEC.md`가 일치한다.

## 핵심 설계 판단

배치 안에서 이벤트 N개를 한 번씩 순회하므로 연산량 자체는 O(N)이다. 이것은 정상적인 비용이며 목표가 아니다.

실제 문제는 현재 Click Consumer처럼 이벤트마다 `await Redis`를 수행하면서 **N번의 순차 Network Round Trip**이 발생하는 것이다. 현재 코드는 `batch.messages`를 순회하며 각 메시지마다 `checkAndMark()`를 호출한다.

변경 목표는 O(N)을 없애는 것이 아니라 다음처럼 Network I/O를 배치화하는 것이다.

```text
기존
Kafka batch N개
  -> Redis 호출 N회(순차 await)

변경
Kafka batch N개
  -> 배치 내부 중복 제거 O(N)
  -> Redis batch read 1회 수준
  -> Redis Pipeline write 1회 수준
```

Redis Pipeline이 내부적으로 여러 명령을 실행하는 것은 정상이다. 핵심은 애플리케이션과 Redis 사이의 순차적인 왕복을 줄이는 것이다.

### Click과 Payment의 처리 단위

| 구분 | Click | Payment |
|---|---|---|
| Kafka 입력 단위 | `eachBatch` | `eachBatch` |
| 배치 내부 중복 제거 | 사용 | 사용 |
| Redis 조회 | MGET / Batch Read | MGET / Batch Read |
| Redis 처리 완료/선점 | ClickHouse 전에 선점 | ClickHouse 성공 후 완료 마킹 |
| ClickHouse 적재 | 기존 `BatchBuffer` 유지 | Kafka batch 기반 Bulk Insert |
| Redis 오류 | Fail-Open | Fail-Closed |
| Offset | 기존 `autoCommit=true` 유지 | `autoCommit=false` |
| DLQ | 사용하지 않음 | 사용 |

Click의 Kafka batch와 `BatchBuffer`는 같은 개념이 아니다.

```text
Kafka batch
  = Redis Dedup 최적화 단위

BatchBuffer
  = ClickHouse 적재량 / 시간 제어 단위
```

Payment는 별도의 휘발성 메모리 버퍼를 새로 만들지 않는다. Kafka가 이미 보유한 batch 자체를 처리 단위로 사용한다.

---

# 진행 상태

- [x] Step 0. 명세 및 기존 프로젝트 확인
- [x] Step 1. 프로젝트 구조 및 Workspace 구성
- [x] Step 2. 공통 계약 및 설정 구성
- [x] Step 3. Docker 인프라 기본 구성
- [x] Step 4. Kafka 클러스터 및 Topic 구성
- [x] Step 5. ClickHouse 및 Redis 구성/검증
- [x] Step 6. API Server 구현
- [x] Step 7. Nginx + API Server 2대 구성
- [x] Step 8. Consumer 공통 기반 구현
- [x] Step 9. Click Consumer 구현
- [x] Step 10. Payment Consumer 구현
- [x] Step 11. 전체 End-to-End 검증
- [ ] Step 11.5. Click Consumer Redis 배치 Dedup 리팩터링
- [ ] Step 11.6. Payment Consumer Kafka 배치 마이크로 배치 리팩터링
- [ ] Step 11.7. Prometheus & Grafana 모니터링 구축
- [ ] Step 12. 장애 및 복구 테스트
- [ ] Step 13. 부하 테스트 및 운영 검증
- [ ] Step 14. 최종 정리 및 문서화

---

# 전체 작업 규칙

1. `DEVELOPMENT_SPEC.md`를 최우선 기준 문서로 사용한다.
2. 코드를 수정하기 전에 현재 프로젝트 구조와 기존 코드를 먼저 확인한다.
3. 기존 구조를 확인하지 않고 파일이나 모듈을 새로 만들지 않는다.
4. 명세에 없는 구조 변경을 임의로 하지 않는다.
5. API 계약, Event 형식, Topic, Partition, Key, Consumer Group, Redis 정책, ClickHouse 적재 정책을 임의로 바꾸지 않는다.
6. Kafka Partition을 코드에서 직접 지정하지 않는다.
7. P0/P1/P2와 같은 Partition을 Consumer에 하드코딩하지 않는다.
8. Kafka Consumer Group의 Partition Assignment를 Kafka가 담당하도록 구현한다.
9. API Server는 Stateless 구조를 유지한다.
10. API Server는 ClickHouse에 직접 저장하지 않는다.
11. API Server는 Consumer용 Redis Dedup을 직접 수행하지 않는다.
12. Click Kafka Key는 `sessionId`를 사용한다.
13. Payment Kafka Key는 `orderId`를 사용한다.
14. Click Redis 오류는 Fail-Open 정책을 유지한다.
15. Payment Redis 오류는 Fail-Closed 정책을 유지한다.
16. Payment는 저장 성공 또는 안전한 DLQ 처리 전에 Offset을 Commit하지 않는다.
17. 민감한 Payment 원본 값을 로그에 남기지 않는다.
18. Pino Structured JSON 로그를 사용한다.
19. 필요하지 않은 추상화 / Wrapper / dependency / 파일을 추가하지 않는다.
20. Build 성공만으로 Step을 완료 처리하지 않는다.
21. 현재 Step 검증이 실패하면 다음 Step으로 넘어가지 않는다.
22. 문제를 숨기지 말고 원인을 수정한 뒤 재검증한다.
23. 명세와 다른 구현은 이유를 문서에 남긴다.
24. Consumer는 `CONSUMER_ROLE=click | payment`로 컨테이너 역할을 분리한다.
25. Click BatchBuffer flush 최종 실패는 재시도 2회 후 폐기하며 v1에서 `click-events-retry`를 사용하지 않는다.
26. Payment는 ClickHouse 저장 전에 Redis 완료 키를 기록하지 않는다.
27. API Rate Limit은 `RATE_LIMIT_MAX / API_INSTANCE_COUNT` 기준으로 인스턴스별 제한을 계산한다.
28. Click과 Payment 모두 Kafka batch 내부에서 동일 `eventId`를 먼저 제거한다.
29. Redis Batch Read는 MGET 또는 동등한 Batch Read 방식을 우선한다.
30. Redis 여러 key의 NX 선점 / 완료 마킹은 Pipeline 등으로 묶어 Network Round Trip을 줄인다.
31. O(N) 순회 자체를 문제로 보지 말고, 순차 Network I/O를 제거하는 것을 최적화 목표로 한다.
32. Click은 Redis 선점 후 기존 BatchBuffer에 넣으며 기존 Best-effort 저장 정책을 유지한다.
33. Payment는 ClickHouse 성공 후 Redis 완료 마킹을 수행한다.
34. Payment DLQ 발행은 Retry하고, Retry 소진 시 Offset을 커밋하지 않고 예외를 전파한다.
35. Payment Bulk Insert는 배치 단위 처리이며, 단일 poison event가 배치 전체 DLQ 범위에 영향을 줄 수 있다는 트레이드오프를 인지한다.
36. Payment 배치 크기를 과도하게 키우지 않는다.
37. Click Kafka batch와 ClickHouse BatchBuffer를 하나로 합치지 않는다.
38. Payment에 별도 휘발성 메모리 버퍼를 새로 추가하지 않는다.
39. 모니터링은 검증에 직접 필요한 지표만 노출한다.
40. 부하 테스트는 k6로 통일한다.
41. Consumer 리팩터링은 논리적 작업 단위별로 분리하고 별도 Git Commit으로 기록한다.
42. Click Redis Batch Dedup과 Payment Kafka Micro Batch를 하나의 Commit에 섞지 않는다.
43. Step 체크박스는 구현 + 테스트 + Runtime 검증 + Git Commit 완료 후에만 변경한다.

---

# 모든 Step의 공통 진행 순서

1. 현재 프로젝트 상태 확인
2. 현재 Step 관련 코드 / 설정 확인
3. 해당 Step 범위만 구현
4. Format / Lint / TypeCheck / Build
5. Unit Test
6. 필요한 Integration Test
7. 실제 Runtime 검증
8. 실제 결과 확인
9. 문제 수정
10. 수정 후 재검증
11. 완료 조건 전부 충족
12. 별도 Git Commit
13. 다음 Step으로 이동
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

- [x] Step 0 완료

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

- [x] Step 1 완료

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

- [x] Step 2 완료

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

- [x] Step 3 완료

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

- [x] Step 4 완료

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

- [x] Step 5 완료

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

API Server가 2대이므로 동일한 `RATE_LIMIT_MAX`를 각 프로세스에 그대로 적용하면
실질적인 전체 한도가 의도보다 2배가 될 수 있다.

이번 프로젝트에서는 Redis 기반 공통 Rate Limit을 추가하지 않고,
**`RATE_LIMIT_MAX`를 API 인스턴스 수로 나눈 값을 각 API 프로세스의 제한으로 사용한다.**

예:

```text
RATE_LIMIT_MAX=5000
API_INSTANCE_COUNT=2
API #1 = 2500 req/min/IP
API #2 = 2500 req/min/IP
--------------------------------
의도한 전체 한도 ≈ 5000 req/min/IP
```

따라서 `DEVELOPMENT_SPEC.md`의 기본값과 동일하게 `API_INSTANCE_COUNT=2`를 사용한다.
Step 7에서 API 2대가 모두 떠 있는 실제 환경에서 합산 동작을 검증한다.

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

- [x] Step 6 완료

---

# Step 7. Nginx + API Server 2대 구성

## 작업 내용

구성:

- API Server #1
- API Server #2
- Nginx

### Rate Limit 인스턴스 분배

API 프로세스별 `RATE_LIMIT_MAX`를 API 인스턴스 수로 나눈다.

```text
RATE_LIMIT_MAX / API_INSTANCE_COUNT
```

현재 전체 목표 한도는 5000 req/min/IP,
API 인스턴스 수는 2대이므로 각 API 프로세스는 2500 req/min/IP를 사용한다.

Redis 기반 공통 Rate Limit으로 확장하지 않는다.

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

- [x] Step 7 완료

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

Consumer 실행 역할은 **컨테이너 단위로 분리**한다.
동일한 `consumer-worker` 이미지/코드를 사용하되 `CONSUMER_ROLE` 환경 변수로
각 컨테이너가 하나의 역할만 실행하도록 만든다.

```dotenv
CONSUMER_ROLE=click
```

또는

```dotenv
CONSUMER_ROLE=payment
```

`apps/consumer-worker/src/app.module.ts`에서 다음처럼 조건부로 등록한다.

```text
CONSUMER_ROLE=click
  -> ClickEventsConsumer만 등록
CONSUMER_ROLE=payment
  -> PaymentEventsConsumer만 등록
```

Docker Compose 운영/전체 로컬 검증 구성은 정확히 다음 5개다.

```text
worker-click-1
worker-click-2
worker-click-3
worker-payment-1
worker-payment-2
```

`click`과 `payment` Consumer가 동시에 실행되는 단일 Worker 컨테이너를 사용하지 않는다.

Partition Assignment는 반드시 Kafka Consumer Group이 담당한다.
P0/P1/P2를 애플리케이션 코드에서 직접 지정하지 않는다.

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

- [x] Step 8 완료

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

다음 정책으로 동작을 고정한다.

```text
flush 실패
  ↓
즉시 재시도 1회
  ↓
실패
  ↓
즉시 재시도 1회 추가
  ↓
실패
  ↓
batch 폐기
  ↓
warn 로그 + 실패 건수 증가
```

- 총 재시도 횟수는 2회다.
- Click은 Best-effort / Fail-Open 정책이므로 최종 실패 batch를 별도 재처리하지 않는다.
- `click-events-retry` 토픽은 v1에서 **사용하지 않는다**.
- `click-events-retry` Consumer도 이번 Step 9에서는 구현하지 않는다.
- 향후 확장용 토픽으로만 유지한다.

다음 상황의 동작도 코드와 테스트로 명확하게 정의한다.

- Size Trigger Flush
- Interval Trigger Flush
- Flush 중 추가 Flush 요청
- Flush 실패
- 동시 Flush 방지
- flush 재시도 2회
- 최종 실패 시 batch 폐기 + warn 로그 + 실패 건수 기록

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
- Flush 실패 시 정의된 정책(재시도 2회 → batch 폐기 → warn 로그/실패 건수 기록)대로 처리된다.

ClickHouse Query로 실제 저장 결과를 확인한다.

## 완료 조건

Click 이벤트가 HTTP부터 ClickHouse까지 실제로 전달되고 저장되는 것을 확인한다.

- [x] Step 9 완료

## 보충 설명 (사후 기록)

Step 11 이후 추가로 확인된 개선점은 Click의 Redis 호출 방식이다. 기존 Click 코드는 Kafka batch를 순회하면서 각 메시지마다 Redis `checkAndMark()`를 순차 `await`한다. 따라서 이번 개정에서 Click Redis Dedup을 Kafka batch 단위로 변경한다.

단, ClickHouse 적재용 `BatchBuffer` 자체는 유지한다. Kafka batch는 Redis 최적화 단위이고 `BatchBuffer`는 ClickHouse 적재 최적화 단위이므로 둘을 하나로 합치지 않는다.

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

Payment Dedup은 **저장 전에 처리 완료를 표시하지 않는다.**

정확한 처리 순서:

```text
1. Redis 중복 여부 확인
2. DUPLICATE면 Offset 처리
3. NEW면 ClickHouse 저장 시도
4. ClickHouse 저장 성공
5. Redis에 처리 완료 키 기록
6. Offset Commit
```

다음 상황을 모두 명확하게 처리해야 한다.

1. Redis 연결 자체가 불가능한 경우
2. Redis가 DUPLICATE를 반환하는 경우
3. ClickHouse 저장이 실패하는 경우
4. ClickHouse Retry가 모두 실패한 경우
5. DLQ Publish가 성공한 경우
6. DLQ Publish가 실패한 경우
7. ClickHouse 저장 성공 후 Redis 완료 표시 전에 프로세스가 종료되는 경우
8. Kafka Offset Commit이 성공/실패한 경우

**금지 사항:**

```text
Redis SET NX 완료
  ↓
ClickHouse 저장
```

순서로 구현하지 않는다.

그 순서에서는 ClickHouse 저장 실패 후 재처리 시 Redis DUPLICATE가 되어
실제 데이터 저장이 영구적으로 누락될 수 있다.

ClickHouse 저장 성공 후 Redis 처리 완료 표시 전에 재시작되어 중복 적재가 발생할 수 있는 경우에는
`payment_events`의 `ReplacingMergeTree` 및 `(order_id, event_id)` 기준으로 최종 중복이 해소되는지 검증한다.

## Consumer 수

- Payment Consumer 2대
- 동일 Consumer Group
- `CONSUMER_ROLE=payment`
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

- [x] Step 10 완료

## 보충 설명 (사후 기록 — 중요)

Step 10의 건별 Payment INSERT는 이후 Step 11.6에서 Kafka batch 기반 Bulk Insert로 대체한다. 변경 이유는 대량 이벤트를 건별로 INSERT할 때 ClickHouse의 작은 Data Part가 과도하게 생성될 수 있기 때문이다.

다만 Step 10에서 확정한 원칙은 유지한다. 특히 `ClickHouse 저장 성공 -> Redis 완료 마킹 -> Offset Commit`, Redis Fail-Closed, DLQ 처리 전 Offset 미커밋, `ReplacingMergeTree` 기반 최종 중복 해소는 변경하지 않는다.

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

- [x] Step 11 완료

## 보충 설명 (사후 기록)

Step 11에서 확인한 Payment E2E는 Step 10의 건별 처리 구현 기준이었다. 이후 Step 11.5 / 11.6에서 Click / Payment Consumer의 배치 처리 방식이 각각 변경되므로 해당 Consumer에 대한 회귀 검증을 각 Step에서 다시 수행한다.

---

# Step 11.5. Click Consumer Redis 배치 Dedup 리팩터링

## 목적

Click 이벤트는 Payment보다 높은 트래픽을 받을 가능성이 있으므로 현재 메시지별 Redis `await` 호출을 제거한다.

이번 Step에서는 **Redis Dedup 호출 방식만 변경하고 ClickHouse 적재 구조는 변경하지 않는다.**

## 변경 전

```text
Kafka batch N개
  -> message마다 Redis checkAndMark()
  -> BatchBuffer
```

## 변경 후

```text
Kafka batch N개
  -> batch 내부 eventId 중복 제거
  -> Redis MGET / Batch Read
  -> 신규 후보만 Redis Pipeline SET NX
  -> 선점 성공 이벤트만 BatchBuffer
```

예:

```text
Kafka 100개
  -> 내부 중복 제거 95개
  -> Redis Batch Read
  -> 기존 DUPLICATE 5개 제외
  -> 신규 후보 90개
  -> Redis Pipeline SET NX
  -> 실제 선점 성공 88개
  -> BatchBuffer에 88개 추가
```

## 왜 이렇게 하는가

연산량은 여전히 O(N)이다. 하지만 기존에는 N번의 순차 Redis Round Trip이 발생할 수 있었고, 변경 후에는 Batch Read와 Pipeline을 사용해 Network I/O를 배치화한다.

즉:

```text
O(N) 연산은 유지
Network Round Trip은 크게 감소
```

## 배치 내부 중복 제거

Redis를 조회하기 전에 `Set<eventId>` 등을 사용해 같은 Kafka batch 안의 동일 `eventId`를 먼저 제거한다.

이는 Redis 상태와 관계없이 동일 batch에서 중복 이벤트가 ClickHouse 적재 대상으로 중복 추가되는 것을 방지하기 위한 1차 필터다.

## Redis Batch Read

가능하면 MGET 또는 현재 Redis Client에서 지원하는 동등한 Batch Read를 사용한다.

```text
eventId 1
 eventId 2
 eventId 3
 ...
 eventId N
      |
      v
    MGET
      |
      v
한 번의 batch 응답
```

## Redis 선점

Click은 기존 정책을 유지하기 위해 ClickHouse 적재 전에 Redis 선점을 수행한다.

```text
Redis MGET
  -> NEW 후보
  -> Pipeline SET NX
  -> 성공한 key만 처리
  -> BatchBuffer
```

MGET 결과가 NEW여도 최종 선점 여부는 `SET NX` 결과를 기준으로 한다.

## Redis 장애 정책

Click은 Fail-Open이다.

```text
Redis Batch Read 실패
  -> Retry 정책 적용
  -> 계속 실패
  -> warn
  -> 해당 이벤트를 BatchBuffer에 포함
```

Redis가 없는 상태에서 Click은 처리를 계속할 수 있다. 이 경우 중복 저장 가능성은 기존 Best-effort 정책의 허용 범위로 기록한다.

## ClickHouse BatchBuffer는 유지

이번 Step에서 다음 구조는 제거하지 않는다.

```text
BatchBuffer
  - maxSize
  - flushInterval
  - flushing flag
  - ClickHouse Bulk Insert
  - flush Retry 2회
```

역할은 다음과 같이 분리한다.

```text
Kafka eachBatch
  = Redis Dedup batch

BatchBuffer
  = ClickHouse write batch
```

## 예상 변경 파일

기존 구조를 먼저 확인한 후 최소한으로 수정한다.

```text
apps/consumer-worker/src/click/click-events.consumer.ts
```

기존 `RedisDedupService`에 batch 기능이 없다면 해당 서비스에 필요한 최소 메서드만 추가한다.

예상 기능:

```text
batchGetExisting(...)
batchMarkIfAbsent(...)
```

범용 Dedup Framework나 별도 추상화 계층을 새로 만들지 않는다.

## 테스트

### Unit Test

- 동일 batch 내부 eventId 중복 제거
- Redis Batch Read 결과에 따른 DUPLICATE 필터링
- Pipeline SET NX 결과에 따른 최종 선점 필터링
- Redis Fail-Open
- BatchBuffer 전달
- 기존 flush Retry 2회 정책 유지

### Integration Test

- 실제 Redis에 batch read
- Redis 중복 key 포함 batch
- 동일 batch 중복 eventId
- Redis 장애
- ClickHouse BatchBuffer 정상 동작

### 회귀 테스트

- Click 정상 저장
- Click 중복 처리
- Redis Fail-Open
- BatchBuffer Size Flush
- BatchBuffer Interval Flush
- 동시 Flush 방지
- flush Retry 2회
- 최종 실패 batch 폐기

## 성능 검증

기존:

```text
N messages
-> Redis sequential call N회
```

변경:

```text
N messages
-> Redis batch read
-> Redis pipeline
```

비교 항목:

- Consumer 처리 시간
- Kafka Lag
- Redis latency
- Throughput
- CPU

## Git Commit

```text
refactor(consumer): batch click redis dedup per kafka batch
```

이 Commit에는 Payment Consumer 변경을 포함하지 않는다.

## 완료 조건

- 메시지별 Redis `await` 호출이 제거됨
- Kafka batch 단위 Redis Batch Read 적용
- Redis Pipeline 기반 선점 적용
- Click Fail-Open 유지
- ClickHouse BatchBuffer 유지
- Unit / Integration / Runtime 검증 통과
- 기존 Step 9 회귀 검증 통과
- 별도 Git Commit 완료

- [ ] Step 11.5 완료

---

# Step 11.6. Payment Consumer Kafka 배치 마이크로 배치 리팩터링

## 목적

현재 Payment Consumer는 메시지마다 ClickHouse INSERT를 한 번씩 수행한다. 이를 Kafka `eachBatch`의 `batch.messages` 자체를 처리 단위로 사용하도록 변경한다.

별도의 휘발성 메모리 BatchBuffer는 추가하지 않는다.

## 변경 전

```text
message 1
  -> Redis 조회
  -> ClickHouse INSERT 1회
  -> Redis 완료 마킹
  -> Offset

message 2
  -> Redis 조회
  -> ClickHouse INSERT 1회
  -> Redis 완료 마킹
  -> Offset
```

## 변경 후

```text
Kafka batch 100개
  -> 배치 내부 eventId 중복 제거
  -> Redis Batch Read
  -> DUPLICATE 제거
  -> 유효 이벤트 90개
  -> ClickHouse Bulk Insert 1회
  -> 성공 후 Redis 완료 마킹 Pipeline
  -> Offset Commit
```

## Step별 처리

### 1. Kafka batch 수신

`eachBatch`의 `batch.messages`를 그대로 처리 단위로 사용한다.

### 2. Envelope 파싱

각 메시지를 Payment Event Envelope로 파싱한다.

### 3. 배치 내부 중복 제거

Redis 조회 전에 `Set<eventId>` 등을 사용해 동일 batch 내 중복을 제거한다.

### 4. Redis Batch Read

가능하면 MGET 또는 동등한 batch read를 사용한다.

```text
100 messages
 -> unique 97
 -> Redis MGET
 -> DUPLICATE 7
 -> NEW 90
```

Payment Redis는 Fail-Closed 정책이므로 Batch Read 자체가 안전하게 완료되지 않으면 다음 단계로 진행하지 않는다.

### 5. ClickHouse Bulk Insert

NEW 이벤트를 Row 배열로 변환하여 `insertPaymentEvents(rows)`를 **1회** 호출한다.

```text
90 rows
 -> ClickHouse INSERT 1회
```

Retry는 기존 Payment Retry 정책을 유지한다.

### 6. ClickHouse 성공 후 Redis 완료 마킹

반드시 다음 순서를 유지한다.

```text
ClickHouse 성공
  -> Redis Pipeline 완료 마킹
  -> Offset 처리
```

절대 다음 순서를 사용하지 않는다.

```text
Redis 완료 마킹
  -> ClickHouse
```

### 7. Redis 완료 마킹 실패

Payment는 Fail-Closed다.

```text
ClickHouse 성공
  -> Redis 완료 마킹 실패
  -> Retry
  -> 최종 실패
  -> Offset 미커밋
  -> 재처리
```

ClickHouse에 이미 저장된 데이터가 다시 들어올 수 있으므로 `payment_events`의 `ReplacingMergeTree` 및 `(order_id, event_id)` 기준 최종 중복 해소를 확인한다.

### 8. Offset Commit

해당 batch의 이벤트가 다음 중 하나로 안전하게 종료된 경우에만 Offset을 처리한다.

```text
정상 ClickHouse 저장 + Redis 완료 마킹
또는
Redis DUPLICATE
또는
DLQ 발행 성공
```

## ClickHouse Bulk Insert 실패

```text
Bulk Insert
  -> Retry
  -> Retry 소진
  -> 해당 유효 이벤트 DLQ
```

배치 내부 Redis DUPLICATE 이벤트는 다시 DLQ로 보내지 않는다.

## DLQ 정책

DLQ 발행에도 Retry를 적용한다.

```text
DLQ send
  -> Retry
  -> Retry
  -> 성공
  -> Offset Commit
```

DLQ Retry가 모두 실패하면:

```text
Offset 미커밋
  -> 예외 전파
  -> Consumer 재처리
```

이미 일부 DLQ가 성공한 상태에서 재처리되어 동일 메시지가 다시 DLQ로 발행될 수 있다. 데이터 유실보다 중복이 안전한 트레이드오프로 기록한다.

## Poison Event 트레이드오프

Bulk Insert는 batch 단위 요청이므로 배치의 한 이벤트가 스키마 오류를 일으키면 정상 이벤트까지 같은 배치 실패에 묶일 수 있다.

예:

```text
100 events
 -> valid 99 + poison 1
 -> Bulk Insert 실패
 -> 유효 대상 전체가 DLQ 대상이 될 수 있음
```

v1에서는 추가적인 poison event 격리 알고리즘을 구현하지 않는다. 대신 Payment batch 크기를 과도하게 키우지 않는다.

## `commitOffsetsIfNecessary()` 검증

설치된 kafkajs 버전의 Type / Runtime 동작을 기준으로 실제 검증한다.

다음 중 하나를 채택하되 임의로 혼용하지 않는다.

```text
commitOffsetsIfNecessary()
```

또는

```text
commitOffsetsIfNecessary(uncommittedOffsets())
```

## 환경 변수 정리

Payment가 별도 메모리 BatchBuffer를 사용하지 않으므로 다음 값은 제거하거나 미사용으로 명시한다.

```text
PAYMENT_BATCH_MAX_SIZE
PAYMENT_BATCH_FLUSH_MS
```

## 테스트

### Unit Test

- batch 내부 동일 eventId 제거
- Redis DUPLICATE 제거
- Redis Batch Read 실패 시 Fail-Closed
- ClickHouse Bulk Insert가 1회 호출되는지
- ClickHouse 성공 전 Redis 완료 마킹이 발생하지 않는지
- ClickHouse 성공 후 Redis 완료 마킹
- Redis 완료 마킹 실패 시 Offset 미처리
- ClickHouse Retry
- Retry 소진 후 DLQ
- DLQ 전부 성공 후 Offset 처리
- DLQ 최종 실패 시 Offset 미처리 + 예외 전파

### Integration Test

- 실제 Kafka batch
- 실제 Redis Batch Read
- 실제 ClickHouse Bulk Insert
- ClickHouse 장애
- Redis 장애
- DLQ 정상
- DLQ 장애
- Consumer 재시작

### 회귀 테스트

Step 10:

- 정상 Payment
- 중복 Payment
- Retry
- DLQ
- Offset Commit

Step 11:

- Payment E2E
- Payment 중복
- ClickHouse 실제 Row

Payment ReplacingMergeTree 검증에서는 `FINAL` 또는 동등한 방법을 사용한다.

## 성능 검증

기존:

```text
N events
 -> Redis N회
 -> ClickHouse INSERT N회
```

변경:

```text
N events
 -> Redis Batch Read
 -> ClickHouse INSERT 1회
 -> Redis Pipeline
```

비교:

- Consumer processing time
- Kafka Lag
- Redis latency
- ClickHouse INSERT 요청 수
- Throughput
- CPU / Memory

## Git Commit

```text
refactor(consumer): batch payment events by kafka batch
```

이 Commit에는 Click Consumer 변경을 포함하지 않는다.

## 완료 조건

- Kafka `eachBatch` 기반 Payment 처리 구현
- 배치 내부 중복 제거
- Redis Batch Read
- ClickHouse Bulk Insert 1회
- ClickHouse 성공 후 Redis 완료 마킹 Pipeline
- Retry / DLQ 구현
- Offset Commit 검증
- Step 10 / Step 11 Payment 회귀 검증 통과
- `DEVELOPMENT_SPEC.md` 및 환경 변수 문서 갱신
- Unit / Integration / Runtime 검증 통과
- 별도 Git Commit 완료

- [ ] Step 11.6 완료

---

# Step 11.7. Prometheus & Grafana 모니터링 구축

## 목적

Step 12 장애 검증과 Step 13 부하 테스트에서 배치 처리 변경의 영향을 정량적으로 확인한다.

## 작업 내용

- API `/metrics`
- Consumer `/metrics`
- Prometheus
- Grafana
- Dashboard 1개

## 핵심 지표

| 지표 | 목적 |
|---|---|
| Kafka Consumer Lag | Click / Payment backlog |
| `click_redis_batch_dedup_total` | Click Batch Dedup 처리량 |
| `redis_dedup_fail_open_total` | Click Fail-Open 횟수 |
| `redis_dedup_fail_closed_retry_total` | Payment Redis Retry 횟수 |
| `payment_batch_insert_total` | Payment Bulk Insert 횟수 |
| `payment_batch_insert_failures_total` | Payment Bulk Insert 최종 실패 |
| `payment_dlq_sent_total` | DLQ 성공 |
| `payment_dlq_send_failures_total` | DLQ 실패 |
| `click_batch_dropped_total` | Click BatchBuffer 폐기 이벤트 |
| API 요청 수 / 상태코드 | API 트래픽 |
| API 응답시간 Histogram | p50 / p95 / p99 |

과도한 범용 지표나 불필요한 대시보드 패널은 추가하지 않는다.

## 검증

- API Target `up`
- Consumer 5개 Target `up`
- Grafana Dashboard 정상
- Click Redis Batch 지표 증가
- Payment Bulk Insert 지표 증가
- Payment DLQ 지표 증가
- Click Drop 지표 변화
- Kafka Lag 표시

## Git Commit

```text
feat(monitoring): add prometheus metrics and grafana dashboard
```

- [ ] Step 11.7 완료

---

# Step 12. 장애 및 복구 테스트

> 이 Step은 Step 11.5 / 11.6 / 11.6의 Click / Payment 배치 로직을 기준으로 수행하며, Step 11.7에서 구축한 Grafana 대시보드로 지표 변화도 함께 관찰한다.

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

- Fail-Closed 확인 (마이크로 배치 기준 — 배치 내 Redis 조회가 실패하는 경우 재시도 후에도 실패하면 처리가 지연되는지 확인)

Redis를 복구한 후 다시 정상 처리되는지 확인한다.

## ClickHouse 장애

ClickHouse를 종료한다.

Click:

- Batch Flush 실패 시 명세와 구현한 실패 정책대로 동작하는지 확인

Payment:

- 배치 Retry 동작
- Offset 조기 Commit이 없는지 확인
- Retry 최종 실패 시 배치 전체가 DLQ로 이관되는지 확인 (포이즌 이벤트 트레이드오프 재확인)
- DLQ 이관까지 실패하는 경우 offset 미커밋 + 프로세스 재시작이 실제로 발생하는지 확인

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
로그만 보는 것이 아니라 Kafka / Redis / ClickHouse / API의 실제 상태와 Grafana 지표까지 확인했다.

- [ ] Step 12 완료

---

# Step 13. 부하 테스트 및 운영 검증

## 작업 내용

k6 테스트를 구성하거나 기존 테스트를 완성한다. 부하 테스트 도구는 k6로 통일한다(규칙 32).

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
- Payment 배치 실패/DLQ 발행 횟수 (Step 11.6 지표)
- Click BatchBuffer flush drop 횟수 (Step 11.6 지표)

### 측정 도구

Step 11.6에서 구축한 Prometheus/Grafana 대시보드를 1차 관측 수단으로 사용한다. 세부 확인이 필요할 때 보조적으로 다음을 사용한다.

- CPU / Memory 세부 확인: `docker stats`
- Kafka Lag 원시 값 확인: `kafka-consumer-groups.sh --describe`

부하 테스트 종료 후 결과를 기록하고, 이전 기준과 비교 가능하도록 대시보드 스크린샷 또는 값과 k6 리포트를 함께 남긴다.

## Backpressure 확인

부하를 주면서 확인한다.

- Kafka Lag이 어떻게 증가하는지
- Consumer가 Lag을 다시 줄이는지
- ClickHouse Batch Insert가 정상 동작하는지
- Payment Retry / DLQ가 비정상적으로 증가하지 않는지
- 배치 크기가 커질수록 포이즌 이벤트 트레이드오프(규칙 30)로 인한 DLQ 발행이 비정상적으로 늘지 않는지

## 로그 검증

- Structured JSON 로그가 정상적으로 생성되는지
- 필요한 Context가 포함되는지
- 민감한 값이 노출되지 않는지

## 완료 조건

- 부하 테스트가 통과한다.
- p95 / p99 결과를 기록했다.
- Kafka Lag을 Grafana와 CLI 양쪽에서 확인했다.
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
- 잘못된 환경 변수 (`PAYMENT_BATCH_MAX_SIZE`, `PAYMENT_BATCH_FLUSH_MS` 등 Step 11.5 / 11.6로 미사용 처리된 값 포함)
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
- Payment 마이크로 배치 처리 방식 (Step 11.5 / 11.6)
- 모니터링 지표 구성 (Step 11.7)

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
Prometheus 1
Grafana 1
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
 -> Payment Consumer (Kafka batch 마이크로 배치)
 -> Redis
 -> ClickHouse
```

ClickHouse `payment_events`에 실제 데이터가 저장되는 것을 확인한다.

## 최종 Payment 실패 검증

```text
Payment
 -> ClickHouse 배치 적재 실패
 -> Retry
 -> Retry 실패
 -> DLQ (재시도 포함)
 -> Offset Commit
```

DLQ 발행 자체가 실패하는 경로(offset 미커밋 + 프로세스 재시작)도 함께 확인한다.

실제 Kafka DLQ Topic과 Offset 상태를 확인한다.

## 최종 완료 조건

다음 조건을 모두 충족해야 프로젝트 완료로 판단한다.

- 모든 Step 체크 완료 (Step 11.5 / 11.6, 11.6, 11.7 포함)
- Build 통과
- TypeCheck 통과
- Test 통과
- Docker 인프라 정상 기동
- Kafka 3 Broker Cluster 정상
- Kafka Topic / Partition / RF / ISR 검증 완료
- API Server 2대 정상
- Nginx Round Robin 정상
- Click Consumer 3대 정상
- Payment Consumer 2대 정상 (마이크로 배치 기준)
- Redis Dedup 정상
- ClickHouse 실제 데이터 저장 확인
- Click 중복 처리 확인
- Payment 중복 처리 확인 (배치 내부 중복 포함)
- Payment Retry 확인
- Payment DLQ 확인 (재시도 포함, 발행 자체 실패 경로 포함)
- Offset Commit 검증
- Consumer Rebalance 확인
- API 장애 복구 확인
- API 2대 환경에서 Rate Limit 합산 동작 확인
- Kafka Broker 장애 복구 확인
- Redis 장애 복구 확인
- ClickHouse 장애 복구 확인
- Prometheus/Grafana 지표 노출 및 대시보드 확인
- k6 부하 테스트 완료
- Kafka Lag 확인
- 로그 구조 및 민감정보 노출 여부 확인
- 문서와 실제 구현 내용 일치 (`DEVELOPMENT_SPEC.md` Payment Consumer 섹션 갱신 포함)

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
Payment 마이크로 배치 리팩터링 검증 (Step 11.5 / 11.6)
  ->
모니터링(Prometheus/Grafana) 구축 확인 (Step 11.7)
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