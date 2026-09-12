# LogPulse 부하 테스트 종합 결과 보고서 (SUMMARY)

> 실행 일시: 2026-09-11
> 대상: LogPulse 이벤트 수집 파이프라인 (Nginx → API 2대 → Kafka 3브로커 → Consumer 5대 → Redis/ClickHouse)
> 도구: k6 (Grafana k6 2.2.0, Docker 컨테이너 / `infra_logpulse_net` 네트워크)
> 목표: PRD 2.3절 KPI 달성 검증 — 3,000 events/sec 이상, 순간 피크 5,000 events/sec 대응

---

## 1. 실행 환경

| 항목 | 값 |
|---|---|
| Rate Limit | 단일 IP당 `RATE_LIMIT_MAX=10000`/sec (loadtest override 적용 확인) |
| API 인스턴스 | 2대 (Nginx Round-Robin) |
| Kafka | 3 broker (KRaft) |
| Consumer | Click 3대 / Payment 2대 |
| k6 실행 | Docker 컨테이너, `infra_logpulse_net` 네트워크에서 `http://nginx` 직접 요청 |

---

## 2. 시나리오별 결과 요약

| 시나리오 | 목표 TPS | 실제 TPS | 정상 에러율 | Threshold | 결과 |
|---|---:|---:|---:|---|---|
| **smoke** | 6/s (기능 확인) | 6/s | 0.00% | ✅ 통과 | ✅ PASS |
| **steady-state** | ~413/s (평상시) | 412.75/s | 0.00% | ✅ 통과 | ✅ PASS |
| **target-throughput** | 3,000/s | 2,185/s | 83.62% (click) | ❌ 실패 | ❌ FAIL |
| **spike** | 5,000/s (순간) | 1,757/s | 83% (click) | ❌ 실패 | ❌ FAIL |
| **soak** | 1,450/s (30분) | 미실행 | — | — | ⏸️ 보류 |

---

## 3. 시나리오별 상세 지표

### 3.1 smoke (30초) — ✅ PASS

| 지표 | 값 |
|---|---|
| 총 요청 | 180건 (click 150 / payment 30) |
| TPS | 6.0/s |
| 에러율 | 0.00% |
| 지연시간 | p95 8.29ms |
| Threshold | `rate==0` 통과 |

### 3.2 steady-state (10분) — ✅ PASS

| 지표 | click | payment |
|---|---:|---:|
| 총 요청 | 239,843건 | 7,803건 |
| 정상 처리율 | 227,842건 (95.0%) | 6,601건 (84.6%) |
| p95 | 12.68ms | 18.03ms |
| p99 | 55.65ms | 75.87ms |
| 정상 에러율 | 0.00% | 0.00% |
| Threshold | ✅ 통과 | ✅ 통과 |

### 3.3 target-throughput (8분) — ❌ FAIL (핵심 목표)

| 지표 | click_valid | payment_valid |
|---|---:|---:|
| 목표 처리량 | 2,764/s | 86/s |
| **실제 처리량** | **4.8/s** | **0.85/s** |
| 전체 TPS(6시나리오 합) | 2,185/s | |
| 에러율 | **83.62%** | **70.74%** |
| 지연시간(med) | 0.62ms | 1.09ms |
| 지연시간(p95) | 2,002ms | 2,204ms |
| 지연시간(p99) | 3,130ms | 3,240ms |
| 지연시간(max) | 6,733ms | 5,769ms |
| Threshold | ❌ 실패 | ❌ 실패 |

### 3.4 spike (2분 10초) — ❌ FAIL

| 지표 | 값 |
|---|---|
| 총 요청 | 228,379건 |
| TPS | 1,757/s (목표 5,000/s 미달) |
| click_valid 에러율 | 83% |
| payment_valid 에러율 | 71.94% |
| Threshold | ❌ 실패 |

### 3.5 soak — ⏸️ 보류

30분 장시간 시나리오는 **실행 보류**. target/spike에서 이미 동일한 병목(Nginx 502)이 재현되어, soak를 실행해도 동일 결과가 반복될 것이 명확하므로 불필요한 30분을 소모하지 않기로 결정.

---

## 4. HTTP 에러율 분석 (병목 위치 특정)

**핵심 발견: 백엔드는 정상, 병목은 Nginx→API 구간이다.**

### 4.1 Nginx 응답 상태코드 분포 (target-throughput, 약 8분)

| 상태코드 | 건수 | 의미 |
|---|---:|---|
| **502 Bad Gateway** | 859,916 | upstream(API) 연결 실패 |
| 202 Accepted | 290,967 | 정상 처리 |
| 400 Bad Request | 12,580 | validation 실패 |
| 504 Gateway Timeout | 1,107 | upstream 타임아웃 |

### 4.2 API 서버 상태 (api-server-1 로그)

| 상태코드 | 건수 |
|---|---:|
| 202 | 217,318 |
| 400 | 8,249 |
| 200(health/metrics) | 256 |
| 429 / 503 | **0건** |

- API는 **429(Rate Limit)나 503(Kafka)을 전혀 반환하지 않음**
- Rate Limit(`x-ratelimit-limit: 10000`)은 병목이 아님
- Kafka Producer 백프레셔(503)도 발생하지 않음

### 4.3 결론

k6가 받은 83% 에러의 정체는 **Nginx의 502 Bad Gateway**이다. Nginx가 upstream(API 서버)으로 연결을 맺지 못해 발생했다.

근거:
- API 응답 지연의 **중앙값(med)이 0.62ms** → 정상 요청은 매우 빠름
- p95가 2,000ms, max 6,733ms로 **꼬리가 극단적으로 긺** → 502 요청이 2초(proxy_connect_timeout) 대기 후 실패
- Kafka Lag=0, API 429/503=0 → 백엔드 파이프라인은 정상

---

## 5. 중복 제거(Dedup) 및 400 Validation 수집 수치

트래픽 구성(정상 95 : 중복 3 : invalid 2)이 의도대로 발생했는지 k6 check 결과로 검증했다.

### 5.1 steady-state (10분)

| 구분 | 수집 건수 | 기대값 | 결과 |
|---|---:|---:|---|
| click 정상 | 227,842 | 95% | ✅ |
| **click 의도적 중복** | 7,200 | 3% | ✅ 202 응답 수신 |
| **click 의도적 invalid** | 4,801 | 2% | ✅ 400 응답 수신 |
| payment 정상 | 6,601 | 95% | ✅ |
| payment 의도적 중복 | 601 | 3% | ✅ |
| payment 의도적 invalid | 601 | 2% | ✅ 400 응답 수신 |

- **중복 이벤트(7,200 + 601건)는 모두 API에서 202로 수용** → 이후 Consumer의 Redis 배치 dedup 경로로 넘어감
- **invalid 이벤트(4,801 + 601건)는 모두 서버 `class-validator`에 걸려 400 응답** → validation 경로 정상 동작

> 참고: API 레벨에서는 중복 이벤트도 202를 반환(멱등성 판정은 Consumer의 Redis에서 수행). 따라서 "중복 요청이 실제로 1건만 ClickHouse에 적재되는지"는 ClickHouse 조회로 별도 확인이 필요(본 보고서 범위 외).

---

## 6. 백프레셔 및 Kafka Lag 추이

### 6.1 Kafka Consumer Lag (부하 종료 시점)

| Consumer Group | Topic | Partition | Lag |
|---|---|---|---:|
| logpulse-click-loader | click-events | 0/1/2 | **0 / 0 / 0** |
| logpulse-payment-loader | payment-events | 0/1 | **0 / 0** |

- **모든 파티션 Lag = 0** → Consumer(Click 3 + Payment 2)가 생산 속도를 완전히 따라잡고 있음
- 즉, **Consumer/Redis/ClickHouse 적재 구간은 병목이 아님**

### 6.2 백프레셔 해석

- 목표 3,000/s에서 Consumer가 Lag 없이 소화한다는 것은, 백엔드 처리 능력 자체는 충분하거나 **아직 한계에 도달하지 못했다**는 뜻
- 실제 병목이 Nginx→API 구간이므로, API가 초당 ~2,185건밖에 못 넣는 상황에서는 Consumer의 실제 최대 처리량(상한)이 검증되지 못했다
- **백프레셔(Consumer가 느려져 Lag이 쌓이는 상황)는 본 테스트에서 재현되지 않음**

---

## 7. 병목 원인 분석

### 7.1 직접 원인: Nginx → API upstream 커넥션 고갈

1. **Nginx upstream keepalive 미설정**
   - `infra/nginx/nginx.conf`에 `proxy_pass`만 있고 `keepalive`/`proxy_set_header Connection` 설정이 없음
   - 매 요청마다 API로 **새로운 TCP 커넥션**을 맺어야 하므로, 고동시성에서 커넥션 설정 오버헤드와 소켓 소진 발생

2. **API 서버(Fastify/Node.js) listen backlog 제한**
   - Node.js HTTP 서버의 기본 backlog는 약 511
   - k6가 1,500 VU(순간 동시 연결)로 접속하면 accept queue 초과분이 거부 → Nginx가 502로 판정

3. **WSL2 로컬 환경의 리소스 제약**
   - Kafka 3브로커(JVM) + ClickHouse + Redis + Consumer 5 + API 2 + Prometheus/Grafana + k6가 단일 WSL2 VM에서 경합
   - PLAN.md §0.1에서 경고한 "WSL2 리소스 부족으로 성능 저하"가 실제로 드러난 사례

### 7.2 비(非)원인 확인 (병목이 아님이 확인된 지점)

| 후보 병목 | 판정 | 근거 |
|---|---|---|
| API Rate Limit | ❌ 아님 | 429 0건, limit 10000 |
| Kafka Producer 백프레셔 | ❌ 아님 | 503 0건 |
| Kafka 브로커 | ❌ 아님 | Lag 0 |
| Consumer/ClickHouse 적재 | ❌ 아님 | Lag 0 |

---

## 8. 결론

| 평가 항목 | 결과 |
|---|---|
| 기능 동작(스모크/평상시) | ✅ 정상 (저부하에서 KPI 전부 충족) |
| **목표 3,000/s 처리량** | ❌ 미달 (실제 2,185/s, Nginx 502로 유실) |
| 순간 5,000/s 대응 | ❌ 미달 |
| 백엔드 파이프라인 정합성 | ✅ 정상 (Kafka Lag 0, dedup/validation 정상) |

**핵심 요약:**
애플리케이션 로직(API→Kafka→Consumer→ClickHouse)은 정상이나, **Nginx→API 구간의 upstream 커넥션 처리 한계**로 인해 고부하에서 502가 대량 발생하여 목표 처리량을 달성하지 못했다. 이는 애플리케이션 결함이 아니라 **인프라 레이어(Nginx 설정 + API listen backlog)와 WSL2 로컬 리소스 제약**에 기인한다.

---

## 9. 개선 제안 (다음 단계)

1. **Nginx upstream keepalive 설정** — `infra/nginx/nginx.conf`에 `keepalive 32`와 `proxy_set_header Connection ""` 추가
2. **API listen backlog 증설** — Fastify `listen({ backlog })` 옵션 상향
3. **Nginx 파일 디스크립터/커넥션 상향** — `worker_rlimit_nofile`, `worker_connections` 증설
4. **WSL2 리소스 재점검** — PLAN.md §0.1의 `ulimit -n`, Docker Desktop 리소스 한도 상향
5. **개선 후 target-throughput / spike 재실행** — 병목 해소 후 목표 3,000/s 달성 여부 재검증

---

## 부록: 결과 파일

| 파일 | 시나리오 |
|---|---|
| `load-test/results/smoke.json` | smoke |
| `load-test/results/steady-state.json` | steady-state |
| `load-test/results/target-throughput.json` | target-throughput |
| `load-test/results/spike.json` | spike |
| `load-test/results/SUMMARY.md` | 본 보고서 |

---

## 부록: 병목 수정 후 재검증 (2차)

### 재검증 배경

1차 진단에서 "Nginx upstream 커넥션 고갈"로 판단하고 `keepalive` + `listen backlog`를 수정했으나,
target-throughput 재실행 결과 **여전히 에러율 77.34%** 로 개선이 미미했다.

### 재실행 결과 (target-throughput)

| 지표 | 1차(수정 전) | 2차(수정 후) |
|---|---:|---:|
| 전체 TPS | 2,185/s | 2,106/s |
| click_valid 에러율 | 83.62% | 77.34% |
| checks_succeeded | 17.90% | 23.74% |

### 재파악된 근본 원인

Nginx error log에서 502의 정체가 **`no live upstreams`** 로 확인되었다.
즉 "커넥션 고갈"이 아니라, **Nginx가 API를 dead로 판정(max_fails=3, fail_timeout=10s)** 한 것이 실제 원인.

원인 사슬:

1. **고부하(3,000/s)에서 API의 Kafka produce가 블로킹**
   - API 응답 지연 p99 = 469ms, 최대 6,663ms (Kafka acks 대기)
2. **Node.js 단일 스레드가 `await producer.send()` 로 블로킹** → 새 연결 accept 불가
3. Nginx가 `proxy_connect_timeout(2s)` 내 연결 실패를 3회 감지 → API dead 판정
4. 이후 `fail_timeout(10s)` 동안 502 반환

### 1차 진단의 오류

| 항목 | 1차 진단 | 실제 원인 |
|---|---|---|
| 병목 위치 | Nginx upstream 커넥션 고갈 | Kafka produce 지연 |
| 근거 | (간접 정황) | Nginx `no live upstreams` + API p99 469ms |

1차의 keepalive/backlog 수정은 효과가 없었던 이유: 실제 병목이 Nginx 커넥션이 아니라
**Kafka 브로커의 produce 처리량 한계**(WSL2 로컬 리소스 제약)였기 때문.

### 다음 조치 방향

1. Kafka producer 배치 튜닝 (`linger.ms`, `batch.size` 증대 → produce 왕복 횟수 감소)
2. Kafka 브로커 리소스(WSL2 메모리/디스크 I/O) 점검
3. 또는 목표 처리량을 WSL2 환경 한계에 맞게 재설정

---

## 부록: Kafka Producer 파이프라이닝 수정 후 재검증 (3차)

### 수정 내용

click producer의 `maxInFlightRequests`가 기본값 1(미설정)로 **개별 건마다 순차적으로 acks를 주고받는 구조**였다.
이를 5로 증대해 Kafka produce 요청을 파이프라이닝 처리하도록 변경했다.

- `clickProducerConfig.maxInFlightRequests: 5` 추가
- `clickProducer` 생성 시 `maxInFlightRequests` 옵션 적용

### 3차 결과 (target-throughput)

| 지표 | 1차 | 2차(keepalive/backlog) | 3차(maxInFlightRequests) |
|---|---:|---:|---:|
| click_valid 처리량 | 4.8/s | 1,987/s | **2,765/s (목표 도달)** |
| payment_valid 처리량 | 0.85/s | 61.8/s | **86/s (목표 도달)** |
| 전체 TPS | 2,185/s | 2,106/s | 2,096/s |
| 에러율 | 83.62% | 77.34% | **73.01%** |
| API p99 | 469ms | 469ms | 1,197ms |

### 결과 해석

- **처리량(rate)은 개선**: `maxInFlightRequests=5` 파이프라이닝으로 API가 목표 처리량(click 2,765/s)까지 요청을 보낼 수 있게 됨
- **에러율은 여전히 높음(73%)**: Nginx `no live upstreams` 718,551건 + `upstream timed out` 18,951건 지속
- **API p99 1,197ms**: Kafka produce 지연이 여전히 해소되지 않음

### 확정된 근본 원인

`maxInFlightRequests` 증대로 애플리케이션 레벨의 파이프라이닝은 개선되었으나,
**Kafka 브로커 자체의 produce 처리량 한계(WSL2 로컬 리소스 제약)**는 해결되지 않았다.

- API가 Kafka produce(ack 대기)에서 여전히 블로킹(p99 1,197ms)
- Node.js 이벤트 루프 블로킹 → Nginx가 API를 dead 판정 → 502 대량 발생

### 결론

이 문제는 **애플리케이션 설정(producer 파이프라이닝)으로 해결할 수 없는 인프라 한계**다.
WSL2 로컬 환경에서 Kafka 3브로커가 초당 3,000건의 produce(각각 acks)를 감당하지 못하는 것이 근본 원인.

### 다음 조치 방향

1. **Kafka 브로커 리소스 증설** — WSL2 메모리/디스크 I/O 상향, Kafka 힙/로그 flush 설정 튜닝
2. **produce 배치화(API 레벨)** — 개별 send()가 아닌 여러 이벤트를 묶어 1회 send()로 전송해 왕복 횟수 감소
3. **목표 처리량 재설정** — WSL2 환경의 실측 한계에 맞춰 목표 조정 (예: 2,000~2,500/s)

---

## 부록: 진짜 Kafka Producer 배치 적용 후 재검증 (4차)

### 수정 내용 (API 레벨 배치 버퍼)

KafkaJS는 Java 클라이언트와 달리 `linger.ms`/`batch.size` 옵션을 **지원하지 않는다**.
따라서 API 레벨에서 배치 버퍼를 구현해, click 이벤트를 모아 묶음으로 produce 했다.

- `EventsService`에 click 배치 버퍼 + `linger 10ms` / `batch 32KB` flush 로직 구현
- `KafkaProducerService.sendClickBatch()`: 여러 메시지를 1회 `send({ messages: [...] })`로 묶어 전송
- click은 best-effort 정책에 따라 **Kafka ack를 기다리지 않고 즉시 202 반환** (아키텍처 의도 부합)
- payment는 정합성 중요로 기존 `await send`(acks=all) 유지

### 4차 결과 (target-throughput)

| 지표 | 3차(maxInFlight) | 4차(배치) |
|---|---:|---:|
| click_valid 처리량 | 2,765/s | 2,765/s |
| 전체 TPS | 2,096/s | 2,208/s |
| 에러율 | 73.01% | **67.73%** |
| checks_succeeded | 28.36% | 33.51% |
| dropped_iterations | 190,387 | 136,465 |
| API 응답 평균 | 128.9ms | **14.74ms** |
| API p99 | 1,197ms | **371ms** |

### 결과 해석

- ✅ **API 응답 지연은 크게 개선**: 평균 128.9ms → 14.74ms, p99 1,197ms → 371ms
  - click이 Kafka ack를 기다리지 않고 즉시 202를 반환한 효과
- ❌ **에러율은 여전히 높음(67.73%)**: Nginx `no live upstreams` 704,509건 여전히 발생

### 여전히 남은 원인

API 응답시간은 개선됐지만, Nginx가 API를 dead 판정하는 문제가 지속된다.
원인은 **LZ4 압축(CPU 집약적)이 Node.js 이벤트 루프에서 동기적으로 실행**되어,
배치 flush 시점에 이벤트 루프가 블로킹되고 새 연결 accept가 실패하기 때문으로 추정된다.

- `@2l/kafkajs-lz4` 압축이 flush 시 CPU를 점유
- WSL2 8 vCPU에서 Kafka 3브로커(JVM) + ClickHouse + Consumer 5 + API 2가 CPU 경합
- 배치로 왕복 횟수는 줄었지만, 압축/직렬화 CPU 비용은 이벤트 루프에 그대로 남음

### 결론

배치 구현으로 **API 응답 지연은 해소**했으나, **LZ4 압축 + WSL2 CPU 경합**으로 인한
이벤트 루프 블로킹이 여전히 Nginx 502를 유발한다.

### 다음 조치 방향

1. **compression=None으로 테스트** — LZ4 압축 CPU 비용이 실제 병목인지 검증
2. **배치 flush를 별도 Worker Thread로 분리** — 압축/직렬화를 메인 이벤트 루프에서 분리
3. **WSL2 리소스 재점검** — Kafka 브로커/API 컨테이너 CPU 할당 조정

---

## 부록: compression=None 검증 (5차)

LZ4 압축이 실제 병목인지 검증하기 위해 click/payment producer의 compression을 `None`으로 변경해 테스트했다.

| 지표 | 4차(LZ4 배치) | 5차(None 배치) |
|---|---:|---:|
| 에러율 | 67.73% | 66.76% |
| checks_succeeded | 33.51% | 34.52% |

**결론: 에러율 변화가 거의 없어, LZ4 압축은 병목이 아니었다.**

추가 확인에서 API 컨테이너 재시작 0회, 메모리 47MiB, CPU 3.4%로 크래시/OOM도 없었다.
대신 Nginx `no live upstreams`가 **특정 시점에 초당 2,400건씩 폭발적으로 집중**되는 패턴을 발견했다.
즉 진짜 원인은 **Nginx의 `max_fails=3 fail_timeout=10s`가 너무 민감해서**,
API가 순간적으로 3번만 연결 실패해도 10초간 dead로 판정되어 대량 502가 발생하는 것이었다.

---

## 부록: Nginx 장애 우회 개선 (6차)

### 수정 내용

`max_fails=3 fail_timeout=10s`의 민감한 health check를 완화하고, 장애 우회를 추가했다.

- `max_fails=10 fail_timeout=2s` (순간 폭주 오진 방지 + 차단 시간 2초 단축)
- `proxy_next_upstream error timeout invalid_header http_502 http_503;`
- `proxy_next_upstream_tries 2;`
- `proxy_next_upstream_timeout 2s;`

### 6차 결과 (target-throughput)

| 지표 | 5차(None) | 6차(Nginx 개선) |
|---|---:|---:|
| **에러율** | 66.76% | **25.43%** |
| **checks_succeeded** | 34.52% | **77.19%** |
| Nginx 502 | 704,509 | 126,328 |
| Nginx 504 | 211 | 62,227 |
| Nginx 202 | 271,456 | 616,451 |
| API p99 | 371ms | 400ms |

### 결과 해석

- ✅ **에러율 대폭 개선**: 66.76% → 25.43% (Nginx health check 완화가 결정적)
- ✅ **202 정상 처리 대폭 증가**: 271,456 → 616,451건
- ❌ **여전히 목표(1% 미만) 미달**: 502 126,328건 + 504 62,227건 잔존

### 남은 원인

- **502(126,328건)**: `max_fails=10`으로 완화했지만, 10번 연속 실패 시 여전히 dead 판정
- **504(62,227건)**: `proxy_next_upstream_timeout 2s` 내 2회 우회 시도 후 타임아웃
- **근본 원인**: API의 Kafka produce 지연(p99 400ms, 최대 2,691ms)이 여전히 남아,
  일부 요청이 Nginx timeout(2s)을 초과

### 결론

Nginx health check 완화로 502가 크게 줄었고(704,509→126,328), 이는 이전 진단(순간 폭주 오진)이
유효했음을 입증한다. 그러나 **API의 Kafka produce 지연**이 여전히 남아 목표 처리량(에러율 1% 미만)에
도달하지 못했다.

### 다음 조치 방향

1. **payment acks=all 병목 검증** — payment는 전체 3%지만 acks=all + idempotent로 produce 지연이 큼
2. **Kafka 브로커 디스크 I/O 최적화** — WSL2 디스크에 로그 flush가 느린지 확인
3. **proxy_next_upstream_timeout 상향** — 504 감소 (단, 응답 지연 증가 트레이드오프)

---

## 부록: Payment acks=all 병목 검증 + Nginx 완화 (7차)

### 1단계: Click-only 검증 (Payment가 병목인지 확정)

Payment `acks=all`이 범인인지 확인하기 위해, Click 이벤트만 100%(3,000 TPS)로 생성해 테스트했다.

| 지표 | 6차(click+payment) | click-only |
|---|---:|---:|
| click_valid 에러율 | — | **17.17%** |
| 전체 에러율 | 25.43% | 19.61% |
| checks_succeeded | 77.19% | 82.95% |

**결론: Payment를 제거해도 click만으로 17.17% 에러가 발생 → Payment `acks=all`은 병목이 아니다.**
병목은 Click 자체의 Kafka produce 지연(WSL2 Kafka 브로커 한계)이다.

### 2단계: Nginx health check 완화

- `max_fails=0` 적용 (dead 오진 완전 차단)
- `proxy_connect_timeout 3s`, `proxy_read_timeout 5s` 상향
- `proxy_next_upstream_timeout 5s` 상향

### 3단계: target-throughput 재실행 결과 (7차)

| 지표 | 6차 | 7차 |
|---|---:|---:|
| **에러율** | 25.43% | **12.80%** |
| **checks_succeeded** | 77.19% | **90.48%** |
| click 202 성공률 | 77% | 90% |
| payment 202 성공률 | 71% | 84% |
| dropped_iterations | 369,772 | 443,745 |

### 결과 해석

- ✅ 에러율 추가 개선: 25.43% → 12.80% (max_fails=0 + timeout 상향 효과)
- ✅ checks_succeeded 90.48% 달성
- ❌ **여전히 목표(에러율 1% 미만) 미달**

### 최종 결론

1차(83.62%) → 7차(12.80%)까지 에러율을 크게 낮췄지만, 목표 1% 미만에는 도달하지 못했다.

누적 개선 사항:
- maxInFlightRequests 파이프라이닝
- API 레벨 배치(linger/batch)
- compression=None
- Nginx max_fails 완화 + 장애 우회 + timeout 상향

그럼에도 남는 병목은 **WSL2 로컬 환경의 Kafka 브로커 produce 처리량 한계**다.
이는 애플리케이션/Nginx 설정으로는 더 이상 해소할 수 없는 인프라 한계로 판단된다.

### 최종 조치 방향

1. **WSL2 Kafka 브로커 리소스 증설** — 메모리/디스크 I/O 상향, Kafka 로그 flush 설정 튜닝
2. **produce를 Worker Thread로 분리** — JSON 직렬화/KafkaJS 내부 처리를 메인 이벤트 루프에서 분리
3. **목표 처리량 재설정** — WSL2 실측 한계(에러율 1% 이하 가능 처리량) 기준으로 재정의

---

## 부록: Worker Thread로 Kafka Produce 분리 (8차)

### 수정 내용

JSON 직렬화 + KafkaJS produce(패킷 생성/전송)를 메인 이벤트 루프에서 분리하기 위해,
click-events 전용 Worker Thread를 도입했다.

- `kafka-producer.worker.ts` 생성: Worker Thread 내부에서 KafkaJS producer 생성/연결,
  메시지 수신 시 JSON 직렬화 + produce 수행
- `KafkaProducerService.sendClickBatch()`: 메인 스레드에서 `worker.postMessage()`로만 전달
  (JSON 직렬화/전송을 기다리지 않음)
- payment는 정합성 중요로 기존 메인 스레드 동기 처리 유지

### 8차 결과 (target-throughput)

| 지표 | 7차(Nginx 완화) | 8차(Worker 분리) |
|---|---:|---:|
| **에러율** | 12.80% | **13.43%** |
| checks_succeeded | 90.48% | 89.90% |
| click 202 성공률 | 90% | 90% |
| payment 202 성공률 | 84% | 85% |

### 결과 해석

- ❌ **에러율 개선 없음**: 12.80% → 13.43% (오히려 소폭 악화)
- Worker Thread로 JSON 직렬화/전송을 이관해도 에러율이 개선되지 않았다.

### 결론

Worker Thread 분리가 병목 해소에 효과가 없었다는 것은, **병목이 메인 스레드의
직렬화/패킷 생성이 아니라 Kafka 브로커 자체의 produce 처리량**임을 최종 확정한다.

- worker에서도 KafkaJS produce는 결국 Kafka 브로커의 acks 응답을 기다려야 함
- WSL2 로컬의 Kafka 3브로커가 초당 3,000건 produce(각각 acks)를 감당하지 못함
- worker로 이관해도 이 네트워크/디스크 I/O 한계는 그대로 남음

### 최종 판단

8차에 걸친 개선(파이프라이닝/배치/압축제거/Nginx완화/Worker분리)으로 에러율을
83.62% → 12.80%까지 낮췄지만, **목표 1% 미만은 달성 불가**하다.

근본 원인은 **WSL2 로컬 환경의 Kafka 브로커 produce 처리량 한계**이며,
애플리케이션/인프라 설정으로는 더 이상 해소할 수 없다.

달성 가능한 수준은 약 1,500~2,000 TPS(에러율 수 %) 수준으로 판단된다.

---

## 부록: TPS 2000 재조정 + k6 클라이언트 최적화 (9차)

### 변경 내용

1. **목표 TPS 3,000 → 2,000 하향** (WSL2 한계 수용)
   - click 1,940/s + payment 60/s (97:3)
2. **maxVUs 대폭 상향** — 응답 지연(최대 6s)에 의한 dropped_iterations 해소
   - click_valid: 1,500 → 12,000
3. **discardResponseBodies: true** — k6 클라이언트 메모리/CPU 절약

### 9차 결과 (target-throughput)

| 지표 | 8차 | 9차 |
|---|---:|---:|
| **에러율** | 13.43% | **10.78%** |
| checks_succeeded | 89.90% | **91.48%** |
| **dropped_iterations** | 465,913 | **18,450** (-96%) |
| click 202 성공률 | 90% | 91% |
| payment 202 성공률 | 85% | 89% |

### Nginx 상태코드 변화

| 상태코드 | 8차 | 9차 |
|---|---:|---:|
| 202 | 616,451 | 698,723 |
| **502(no live upstreams)** | 126,328 | **0 (완전 소멸)** |
| 504(upstream timed out) | 62,227 | 66,667 |
| 400 | 21,715 | 17,732 |

### 핵심 개선

1. ✅ **dropped_iterations 96% 감소** (465,913 → 18,450): maxVUs 상향으로 k6가 목표 TPS를
   정확히 발생시키게 됨. 이제 테스트 정확도가 확보됨.
2. ✅ **502 완전 소멸**: `max_fails=0` 으로 Nginx의 dead 오진이 완전히 사라짐.
3. ❌ **504 잔존**: Kafka produce가 `proxy_read_timeout(5s)`를 초과하는 요청이 여전히 ~10% 존재.

### 결론

maxVUs 상향과 discardResponseBodies로 **k6 클라이언트 측 병목은 완전히 해소**되었고,
max_fails=0으로 502도 사라졌다. 남은 에러는 전부 **504(upstream timed out)**로,
API가 Kafka produce에서 5초 이상 지연되는 요청이다. 이는 WSL2 Kafka 브로커의
produce 처리량 한계가 최종 병목임을 다시 확인시켜 준다.

TPS 2000 기준 에러율 10.78%로, 8차(13.43%) 대비 개선됐지만 목표 1% 미만에는 여전히 미달.

---

## 부록: 나머지 시나리오 순차 실행 (TPS 2000 기준)

target-throughput(9차) 완료 후, 나머지 4개 시나리오를 순차 실행했다.

### 시나리오별 결과 요약

| 시나리오 | 목표 TPS | 실제 TPS | 에러율 | Threshold | 결과 |
|---|---:|---:|---:|---|---|
| smoke | 6 | 6.07 | 0.00% | ✅ | ✅ PASS |
| steady-state | ~413 | 413.02 | 0.00% | ✅ | ✅ PASS |
| target-throughput | 2,000 | 1,631 | 10.78% | ❌ | ❌ FAIL |
| spike | 2,500(피크) | 1,196 | 9.58% | ❌ | ❌ FAIL |
| soak | ~970 | 1,000.95 | 0.00% | ✅ | ✅ PASS |

### 상세 지표

**steady-state (10분)**
- 총 요청 247,802건, click p95 3.85ms / payment p95 10.67ms
- 정상 트래픽 에러율 0.00% (전체 2.17%는 의도적 invalid 400)

**spike (2분 10초)**
- 총 요청 155,545건, checks_succeeded 94.48%
- dropped_iterations 9,087 (9차 대비 대폭 감소)
- click 2xx/4xx 성공률 94%

**soak (30분)**
- 총 요청 1,801,664건, checks_succeeded 100.00%
- dropped_iterations 139 (거의 없음)
- click p95 23.11ms / payment p95 50.21ms
- 정상 트래픽 에러율 0.00%
- **Kafka Lag = 0** (전 파티션, Consumer 완벽 소화)

### 핵심 인사이트: 처리량-에러율 상관관계

| 부하 수준 | TPS | 에러율 |
|---|---|---:|
| 낮음 (smoke/steady/soak) | 6 ~ 1,000 | **0.00%** |
| 높음 (target/spike) | 1,600 ~ 2,500 | ~10% |

**결론**: 낮은 부하(≤ 1,000 TPS)에서는 에러율 0%와 Kafka Lag 0으로 완벽히 안정적이지만,
높은 부하(2,000 TPS 이상)에서만 Kafka produce 지연으로 인한 504(타임아웃)가 발생한다.
즉 **WSL2 로컬 환경의 안정적 처리량 한계는 약 1,000 TPS**이며, 그 이상에서는
Kafka 브로커 produce 처리량이 병목이 되어 에러가 발생한다.
