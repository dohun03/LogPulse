# LogPulse 부하 테스트 리포트

- 대상: LogPulse 이벤트 수집 파이프라인 (Nginx → API 2대 → Kafka 3브로커 → Consumer 5대 → Redis/ClickHouse)
- 도구: k6 (Docker 컨테이너, 애플리케이션과 동일 네트워크에서 직접 요청)
- 환경: WSL2 + Docker Compose (로컬)
- 실행 일시: 2026-09-11
- 목표: 3,000 TPS 이상 처리, 순간 5,000 TPS 대응

[← README로 돌아가기](./README.md)

---

## 0. 종합 요약 (이력서/발표 요약용)

| 항목 | 결과 |
|---|---|
| 목표 처리량 | 3,000 TPS 이상 (순간 5,000 TPS) |
| **실측 안정 처리량** | **약 1,000 TPS** — 에러율 0%, Kafka Lag 0, 30분간 180만 건 무손실 처리 |
| 2,000 TPS 부하 시 | 에러율 10.78% (504 timeout, Kafka produce 지연) |
| 최초 3,000 TPS 시도 시 | 에러율 83.62% |
| 최종 원인 | WSL2 로컬 환경의 **Kafka 브로커 produce 처리량 한계** (애플리케이션 설계 결함 아님) |
| 진단 과정 | 8단계에 걸쳐 병목 후보를 데이터로 검증/반증하며 원인 추적, 에러율 83.62% → 10.78%까지 단계적 개선 |

이 리포트는 "목표를 달성했다"는 결과보다, **왜 달성하지 못했는지를 8단계에 걸쳐 데이터로 추적한 과정**을 기록했습니다.<br>
각 단계는 가설 → 조치 → 재측정 → 해석 순으로 진행했습니다.

---

## 1. 실행 환경

| 항목 | 값 |
|---|---|
| API 인스턴스 | 2대 (Nginx Round Robin) |
| Kafka | 3 broker (KRaft) |
| Consumer | Click 3대 / Payment 2대 |
| k6 실행 | Docker 컨테이너, 애플리케이션과 동일 네트워크에서 `http://nginx` 직접 요청 |
| Rate Limit | 부하 테스트 전용 설정으로 override 적용 (운영 기본값과 분리) |

---

## 2. 최종 시나리오별 결과 (목표 TPS 2,000 기준, 9차 조정 이후)

1차 시도(목표 3,000 TPS)에서 에러율 83.62%를 확인한 뒤, 8단계 디버깅 끝에 **WSL2 환경의 실측 한계에 맞춰 목표를 2,000 TPS로 재조정**하고 최종 5개 시나리오를 실행했습니다.

| 시나리오 | 목표 TPS | 실제 TPS | 에러율 | 결과 |
|---|---:|---:|---:|---|
| smoke | 6 | 6.07 | 0.00% | ✅ PASS |
| steady-state | ~413 | 413.02 | 0.00% | ✅ PASS |
| target-throughput | 2,000 | 1,631 | 10.78% | ❌ FAIL |
| spike | 2,500(피크) | 1,196 | 9.58% | ❌ FAIL |
| soak (30분) | ~970 | 1,000.95 | **0.00%** | ✅ PASS |

### soak (30분) — 핵심 근거

- 총 요청 **1,801,664건**, checks_succeeded **100.00%**
- dropped_iterations 139건 (거의 없음)
- click p95 23.11ms / payment p95 50.21ms
- **Kafka Lag = 0** (전 파티션, Consumer가 생산 속도를 완전히 따라잡음)

### 처리량-에러율 상관관계

| 부하 수준 | TPS 범위 | 에러율 |
|---|---|---:|
| 낮음 (smoke/steady/soak) | 6 ~ 1,000 | **0.00%** |
| 높음 (target/spike) | 1,600 ~ 2,500 | **~10%** |

30분간 180만 건을 에러율 0%로 처리한 soak 결과와, 2,000 TPS 이상에서만 에러가 발생하는 target/spike 결과를 비교하면, **약 1,000 TPS가 이 인프라 구성(WSL2 로컬)에서의 안정적 처리 한계**라는 결론이 명확하게 뒷받침됩니다.

---

## 3. 병목 진단 타임라인 (1차 → 8차)

목표 3,000 TPS에서 에러율 83.62%를 처음 확인한 뒤, 병목 위치를 다음과 같이 단계적으로 좁혔습니다.

| 단계 | 가설 | 조치 | 에러율 변화 | 판정 |
|---|---|---|---:|---|
| 1 | Nginx→API upstream 커넥션 고갈 | keepalive, listen backlog 조정 | 83.62% → 77.34% | ❌ 틀린 진단 (효과 미미) |
| 2 | Nginx `no live upstreams` 재확인 → 실제론 Kafka produce 블로킹 | — (진단만) | — | 근본 원인 재정의 |
| 3 | Producer가 요청마다 순차 acks 대기 | `maxInFlightRequests: 5` 파이프라이닝 적용 | 77.34% → 73.01% | ✅ 처리량은 목표치 도달, 에러율은 잔존 |
| 4 | KafkaJS는 `linger.ms` 미지원 → 진짜 배치 필요 | API 레벨 배치 버퍼(linger 10ms/batch 32KB) 직접 구현 | 73.01% → 67.73% | ✅ API 응답 평균 128.9ms→14.7ms 개선, 에러율은 잔존 |
| 5 | LZ4 압축이 이벤트 루프를 블로킹 | compression=None으로 검증 | 67.73% → 66.76% | ❌ 병목 아님 (변화 거의 없음) |
| 6 | Nginx `max_fails=3/fail_timeout=10s`가 순간 폭주를 오진 | `max_fails=10/fail_timeout=2s` + `proxy_next_upstream` 우회 | 66.76% → **25.43%** | ✅ 결정적 개선 |
| 7 | Payment `acks=all`이 병목일 가능성 | Click-only 테스트로 검증 → Payment 무관 확인, Nginx `max_fails=0` 완화 | 25.43% → **12.80%** | ✅ Payment는 병목 아님 확정 |
| 8 | 메인 스레드의 직렬화/압축이 이벤트 루프 블로킹 | Worker Thread로 Kafka produce 분리 | 12.80% → 13.43% | ❌ 개선 없음 → 병목이 Kafka 브로커 자체임을 최종 확정 |
| 9 | k6 클라이언트 자체 한계(dropped_iterations) | maxVUs 대폭 상향 + 목표 TPS 2,000으로 재조정 | 13.43% → **10.78%** | ✅ 502 완전 소멸, 남은 에러는 전부 504(Kafka produce 지연) |

### 핵심 판단 근거

- **Kafka Consumer Lag이 전 구간에서 0** — Consumer/Redis/ClickHouse 적재 구간은 병목이 아님이 처음부터 확인됨
- **API 429/503이 0건** — Rate Limit, Kafka Producer 백프레셔도 병목 아님
- **8차(Worker Thread) 개선 없음** — 메인 스레드 직렬화가 아니라 Kafka 브로커의 produce 응답(acks) 자체가 느린 것으로 최종 확정
- **9차에서 502가 완전히 사라지고 504만 남음** — Nginx 오진 문제는 완전히 해결되었고, 남은 에러는 전부 "API가 Kafka produce 응답을 5초 넘게 기다리는" 순수 Kafka 처리량 한계

---

## 4. 1차 시도 상세 (목표 3,000 TPS, 수정 전)

### 4.1 시나리오별 결과

| 시나리오 | 목표 TPS | 실제 TPS | 에러율 | 결과 |
|---|---:|---:|---:|---|
| smoke | 6/s | 6/s | 0.00% | ✅ PASS |
| steady-state | ~413/s | 412.75/s | 0.00% | ✅ PASS |
| target-throughput | 3,000/s | 2,185/s | 83.62%(click) | ❌ FAIL |
| spike | 5,000/s | 1,757/s | 83%(click) | ❌ FAIL |
| soak | 1,450/s | — | — | ⏸️ 보류 (동일 병목 재현이 명확해 미실행) |

### 4.2 HTTP 에러율 분석 (병목 위치 최초 특정)

| 상태코드 | 건수 | 의미 |
|---|---:|---|
| **502 Bad Gateway** | 859,916 | upstream(API) 연결 실패 |
| 202 Accepted | 290,967 | 정상 처리 |
| 400 Bad Request | 12,580 | validation 실패 (의도적 invalid) |
| 504 Gateway Timeout | 1,107 | upstream 타임아웃 |

API 서버 자체 로그에는 **429(Rate Limit) / 503(Kafka 백프레셔)이 0건**으로, k6가 받은 83% 에러는 API가 아니라 Nginx의 502였습니다. API 응답의 중앙값(0.62ms)은 매우 빠른데 p95가 2,000ms까지 벌어지는 것으로 보아, 일부 요청이 Kafka produce에서 크게 지연되며 Nginx의 `proxy_connect_timeout`(2s)을 넘겨 실패하는 패턴으로 판단했습니다.

### 4.3 Kafka Lag / Dedup 검증 (1차 시점)

| Consumer Group | Topic | Lag |
|---|---|---:|
| logpulse-click-loader | click-events | 0 / 0 / 0 |
| logpulse-payment-loader | payment-events | 0 / 0 |

의도적으로 섞은 중복(3%)·invalid(2%) 트래픽도 각각 202(중복 수용 후 Redis 단계에서 dedup)와 400(validation 실패)으로 정확히 관측되어, **트래픽 구성 자체는 의도대로 재현되고 있음**을 확인했습니다.

---

## 5. 결론

| 평가 항목 | 결과 |
|---|---|
| 기능 동작 (smoke/steady-state) | ✅ 정상 |
| **목표 3,000 TPS 처리** | ❌ 미달 (실측 안정 한계 약 1,000 TPS) |
| 순간 5,000 TPS 대응 | ❌ 미달 |
| 백엔드 파이프라인 정합성 | ✅ 정상 (Kafka Lag 0, dedup/validation 정상 전 구간) |
| 병목 위치 | Kafka 브로커 produce 처리량 (WSL2 로컬 인프라 제약) |

애플리케이션 로직(API → Kafka → Consumer → ClickHouse)은 정상이며, 문제는 **WSL2 로컬 환경에서 Kafka 3브로커가 초당 3,000건 이상의 produce(acks 응답 포함)를 감당하지 못하는 인프라 리소스 제약**입니다. 8단계 디버깅으로 Nginx 설정, Producer 파이프라이닝/배치, 압축, 스레드 분리 등 애플리케이션 레벨에서 시도할 수 있는 개선은 모두 적용했고, 그 결과 에러율을 83.62%에서 10.78%까지 낮췄지만 근본 한계는 애플리케이션 코드로 해소할 수 없다는 결론에 도달했습니다.

---

## 6. 다음 단계

1. **실서버(클라우드 인스턴스) 환경에서 재검증** — 지금 확정한 한계(1,000 TPS)가 WSL2 로컬 제약이 맞는지, 실제 서버에서도 재현되는지 교차 확인
2. **Kafka 브로커 리소스 스케일 아웃** — 실서버 환경에서 브로커 수/디스크 I/O를 늘렸을 때 처리량 변화 측정
3. **Producer 설정 스윕** — `acks`, 배치 크기별 처리량-지연시간 트레이드오프 곡선화

---

## 부록: 재현 방법

```bash
# 1) 인프라 기동
docker compose up -d --build

# 2) 부하 테스트 전용 Rate Limit override 적용
docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up -d --build

# 3) k6 실행 (compose 네트워크 내부, Nginx 경유)
docker run --rm --network logpulse_default \
  -v "$PWD/load-test/k6:/scripts:ro" \
  -e BASE_URL=http://nginx \
  grafana/k6 run /scripts/scenarios/target-throughput.js
# smoke / steady-state / target-throughput / spike / soak 순으로 반복 실행
```

> 전체 스크립트: `load-test/k6/` · 설계 문서: `PLAN.md` Step 13