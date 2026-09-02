# LogPulse — 제품 요구사항 정의서 (PRD)

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 |
| 작성일 | 2026-09-02 |
| 문서 상태 | Draft (개발 착수용) |
| 대상 프로젝트 | 실시간 대용량 로그 수집·분석 파이프라인 "LogPulse" |
| 문서 유형 | 제품 요구사항 정의서 (Product Requirements Document) |

---

## 1. 개요 (Overview)

### 1.1 배경 및 문제 정의

이커머스형 서비스는 사용자의 행동 로그(클릭, 페이지 이동)와 비즈니스 크리티컬 로그(주문, 결제)가 동시에 대량으로 발생한다. 단일 API 서버가 이 로그들을 동기적으로 DB에 직접 적재하는 구조는 다음과 같은 문제를 갖는다.

- **트래픽 폭주 시 장애 전파**: 로그 트래픽이 급증하면 DB 커넥션 풀이 고갈되어 핵심 비즈니스 로직(주문/결제)까지 함께 장애가 난다.
- **처리 우선순위 부재**: 유실되어도 큰 문제가 없는 클릭 로그와, 절대 유실되면 안 되는 결제 로그가 동일한 신뢰도 수준으로 처리된다.
- **중복 처리 위험**: 네트워크 재시도, 컨슈머 재처리 등으로 동일 이벤트가 중복 적재될 수 있다.
- **분석 시스템과의 강결합**: 향후 이상 탐지, 알림 등 부가 기능을 추가하려면 기존 파이프라인을 직접 수정해야 하는 구조다.

**LogPulse**는 위 문제를 Kafka 기반 비동기 완충 계층과 Redis 멱등성 계층, ClickHouse 분석 계층으로 분리하여 해결하는 실시간 로그 파이프라인이다.

### 1.2 프로젝트 목적

- 초당 3,000건 이상의 이벤트가 유입되어도 API 서버와 후단 시스템이 안정적으로 동작하는 **완충(buffering) 구조**를 구현한다.
- 이벤트의 중요도에 따라 **차등화된 신뢰성 정책**(유실 허용 vs. 유실 불가)을 적용한다.
- 분석 워크로드에 최적화된 **ClickHouse** 기반 대용량 적재 체계를 구축한다.
- 향후 AI 기반 이상 탐지·알림 기능이 기존 파이프라인의 변경 없이 붙을 수 있는 **디커플링된 이벤트 기반 아키텍처**를 설계한다.
- 개인 포트폴리오 관점에서, 실무형 대용량 트래픽 처리·메시지 브로커 운영·데이터 정합성 설계 경험을 구체적인 산출물(설계 문서, 코드, 부하 테스트 결과)로 증명한다.

### 1.3 용어 정의 (Glossary)

| 용어 | 정의 |
|---|---|
| Producer | Kafka에 이벤트를 발행하는 주체. 본 프로젝트에서는 NestJS API Server |
| Consumer | Kafka에서 이벤트를 구독해 처리하는 주체. 본 프로젝트에서는 별도의 Consumer/Worker 서비스 |
| 멱등성 (Idempotency) | 동일 이벤트가 여러 번 처리되어도 결과가 한 번 처리된 것과 동일하게 유지되는 성질 |
| 유실 허용 (Best-effort) | 극단적 장애 상황에서 일부 데이터 손실을 감수하더라도 가용성을 우선하는 정책 |
| 유실 불가 (At-least-once + 멱등 적재) | 데이터가 최소 1회 이상 반드시 적재되도록 보장하는 정책 |
| 백프레셔 (Backpressure) | 후단 처리 속도가 유입 속도를 따라가지 못할 때 시스템을 보호하기 위해 부하를 조절하는 메커니즘 |
| DLQ (Dead Letter Queue) | 정상 처리에 실패한 메시지를 별도로 보관해 후속 조치를 취하기 위한 토픽 |

---

## 2. 목표 (Goals)

### 2.1 비즈니스 목표

- 로그 트래픽 폭주가 핵심 서비스(주문/결제) 가용성에 영향을 주지 않는다.
- 결제 관련 데이터의 정합성을 보장하여 정산/매출 분석의 신뢰도를 확보한다.
- 축적된 로그 데이터를 기반으로 향후 사용자 행동 분석, 이상 거래 탐지 등 부가 서비스를 빠르게 확장할 수 있다.

### 2.2 기술 목표

- 초당 3,000건 이상의 이벤트를 안정적으로 수집한다 (피크 트래픽 기준 여유율 포함, 목표 처리량 5,000 events/sec까지 확장 가능한 구조로 설계).
- Kafka를 이용해 API 서버와 적재 로직을 분리하고, 컨슈머 장애 시에도 API 서버는 영향을 받지 않는다.
- Redis 기반 멱등성 체크로 중복 적재율 0%에 근접하도록 한다.
- ClickHouse 적재 지연(수집 시점 대비 조회 가능 시점)을 평상시 기준 수 초~수십 초 이내로 유지한다.
- 이벤트 스키마와 토픽 구조만으로 신규 컨슈머(AI 이상 탐지 등)를 추가할 수 있는 구조를 갖춘다.

### 2.3 성공 지표 (KPI / SLA)

| 지표 | 목표치 | 측정 방법 |
|---|---|---|
| 처리량 (Throughput) | 3,000 events/sec 이상, 순간 피크 5,000 events/sec 대응 | k6/Artillery 부하 테스트 |
| API 응답 시간 (click-events) | p95 100ms 이하, p99 300ms 이하 | 부하 테스트 + APM |
| API 응답 시간 (payment-events) | p95 200ms 이하, p99 500ms 이하 | 부하 테스트 + APM |
| payment-events 유실률 | 0% (acks=all + 재시도 + DLQ로 보장) | Kafka 오프셋 대비 ClickHouse 적재 건수 검증 |
| click-events 유실 허용치 | 장애 상황에서 최대 0.1% 이내 허용 | 모니터링 대시보드 |
| 중복 적재율 | 0.01% 미만 (Redis 멱등성 체크 + ClickHouse 측 이중 방어) | ClickHouse 쿼리 검증 |
| Consumer Lag | 평상시 1,000건 이하 유지 | Kafka Exporter + Grafana |
| 시스템 가용성 | API 서버 99.9% (월간) | 업타임 모니터링 |

---

## 3. 범위 (Scope)

### 3.1 In-Scope (1차 개발 범위)

- NestJS 기반 이벤트 수집 API 서버 (click-events, payment-events)
- Kafka(KRaft 모드) 클러스터 구성 및 토픽 설계
- Redis 기반 멱등성 체크 로직
- Kafka Consumer/Worker 서비스 (Redis 체크 → ClickHouse 배치 적재)
- ClickHouse 스키마 설계 및 적재 파이프라인
- Docker Compose 기반 로컬/개발 환경 구성
- 기본 모니터링(Consumer Lag, 처리량, 에러율) 대시보드
- 부하 테스트 및 성능 검증 리포트

### 3.2 Out-of-Scope (1차 제외)

- AI 기반 이상 탐지 모델 자체의 구현 (인터페이스와 연동 지점만 설계)
- 사용자 인증/인가를 포함한 완전한 이커머스 서비스 기능
- 다중 리전(Multi-region) 배포, 재해 복구(DR) 구성
- Kubernetes 기반 오토스케일링 (Docker Compose 환경까지가 1차 목표이며, 구조상 K8s 이전이 가능하도록만 설계)
- 프론트엔드 대시보드 UI (Grafana 등 기존 도구 활용으로 대체)

### 3.3 향후 확장 (Future Scope)

- 동일 Kafka 토픽을 구독하는 별도 Consumer Group으로 AI 기반 이상 에러 감지 서비스 연동
- 이상 탐지 결과에 대한 Slack/이메일 요약 알림 발송
- ClickHouse Materialized View 기반 실시간 집계 대시보드 고도화
- Kubernetes 환경으로의 이전 및 HPA(Horizontal Pod Autoscaler) 적용

---

## 4. 이해관계자 및 사용자

| 구분 | 설명 |
|---|---|
| API 소비자 (내부) | 이커머스 프론트엔드/모바일 앱, 결제 서비스 등 이벤트를 발행하는 내부 시스템 |
| 데이터 분석가 (가상) | ClickHouse에 적재된 데이터를 조회해 사용자 행동/매출 분석을 수행 |
| 운영자 (개발자 본인) | 파이프라인의 장애 대응, 모니터링, 스케일 조정을 담당 |
| (향후) AI 이상 탐지 서비스 | 동일 이벤트 스트림을 구독해 이상 패턴을 탐지하는 별도 서비스 |

---

## 5. 기능 요구사항 (Functional Requirements)

### 5.1 로그 수집 API

- `POST /events/click` : 클릭/페이지 이동 이벤트를 수신해 `click-events` 토픽으로 발행한다.
- `POST /events/payment` : 주문/결제 완료 이벤트를 수신해 `payment-events` 토픽으로 발행한다.
- 요청 스키마는 `class-validator` 기반으로 서버 단에서 유효성 검사를 수행하며, 스키마 불일치 시 400을 반환한다.
- 모든 요청/발행 이벤트에는 클라이언트가 생성한 고유 `eventId`(UUID)가 포함되어야 하며, 이는 이후 멱등성 체크의 키로 사용된다.
- Kafka 발행 실패(브로커 다운, 버퍼 초과 등) 시 API는 503과 함께 `Retry-After` 헤더를 반환한다.

### 5.2 도메인 이벤트 정의

**click-events**

| 필드 | 타입 | 설명 |
|---|---|---|
| eventId | string (UUID) | 이벤트 고유 식별자, 멱등성 키 |
| userId | string | 사용자 식별자 (비로그인 시 세션 ID) |
| sessionId | string | 세션 식별자 (Kafka 파티션 키) |
| eventType | string | `product_click`, `page_view` 등 |
| productId | string (optional) | 상품 클릭 시 상품 ID |
| pageUrl | string | 발생 페이지 |
| occurredAt | ISO8601 timestamp | 클라이언트 발생 시각 |
| metadata | object | referrer, device, IP 등 부가 정보 |

**payment-events**

| 필드 | 타입 | 설명 |
|---|---|---|
| eventId | string (UUID) | 이벤트 고유 식별자, 멱등성 키 |
| orderId | string | 주문 ID (Kafka 파티션 키) |
| userId | string | 사용자 식별자 |
| amount | number | 결제 금액 |
| currency | string | 통화 |
| paymentMethod | string | 결제 수단 |
| status | string | `completed`, `failed`, `canceled` |
| occurredAt | ISO8601 timestamp | 결제 완료 시각 |

### 5.3 멱등성 처리 (Redis)

- 컨슈머는 이벤트 처리 전 `dedup:{topic}:{eventId}` 키를 `SET ... NX EX`로 선점 시도한다.
- 키 선점에 성공한 경우에만 ClickHouse 적재를 수행하고, 실패(이미 존재)한 경우 해당 이벤트는 중복으로 간주하고 스킵한다.
- click-events와 payment-events는 TTL 정책을 차등 적용한다 (상세는 시스템 아키텍처 문서 3.4절 참조).
- Redis 자체 장애 시 이벤트 중요도에 따라 fail-open/fail-closed 정책을 분리 적용한다.

### 5.4 영구 적재 (ClickHouse)

- 컨슈머는 일정 배치 단위(건수 또는 시간 기준)로 이벤트를 모아 ClickHouse에 배치 INSERT한다.
- click-events, payment-events는 각각 별도 테이블에 적재하며, 분석 편의를 위한 Materialized View를 함께 구성한다.
- payment-events 테이블은 ReplacingMergeTree 계열 엔진을 사용해 Redis 멱등성 체크를 통과하지 못한 극히 일부의 중복도 최종적으로 정리되도록 이중 방어한다.

### 5.5 향후 AI 연동 인터페이스

- AI 이상 탐지 서비스는 별도의 Kafka Consumer Group ID로 `click-events`, `payment-events` 토픽을 동일하게 구독할 수 있다.
- 기존 컨슈머(적재 파이프라인)의 오프셋/처리 로직과 완전히 독립적이므로, AI 서비스의 장애나 성능 저하가 기존 파이프라인에 영향을 주지 않는다.
- 이벤트 스키마(5.2절)는 AI 서비스와의 계약(Contract)으로 취급하며, 변경 시 하위 호환을 유지한다.

---

## 6. 비기능 요구사항 (Non-Functional Requirements)

### 6.1 성능 (Performance / Throughput)

- 시스템 전체는 초당 3,000건 이상의 이벤트를 안정적으로 처리해야 하며, API 서버 자체는 Kafka Producer 발행만 담당하므로 DB I/O에 의한 지연이 발생하지 않아야 한다.
- Kafka 토픽은 파티션 수를 목표 처리량에 맞게 산정하여 컨슈머 병렬 처리가 가능해야 한다.

### 6.2 신뢰성 / 가용성

- payment-events는 `acks=all`, `min.insync.replicas` 설정을 통해 브로커 장애 상황에서도 유실되지 않아야 한다.
- 컨슈머 처리 실패 시 재시도(Retry Topic) 및 DLQ를 통해 데이터가 유실되지 않고 추적 가능해야 한다.
- API 서버는 Kafka 장애와 무관하게 최소한의 헬스체크 응답을 유지해야 한다.

### 6.3 확장성

- Consumer는 파티션 수 범위 내에서 인스턴스를 수평 확장하여 처리량을 늘릴 수 있어야 한다.
- 신규 도메인 이벤트(토픽) 추가 시 기존 컴포넌트의 구조 변경 없이 확장 가능해야 한다.

### 6.4 데이터 정합성

- 동일 eventId를 가진 이벤트는 ClickHouse에 최종적으로 1건만 유효 데이터로 남아야 한다.
- 이벤트 발생 시각(occurredAt)과 시스템 수집 시각(ingestedAt)을 모두 기록하여 지연 유입 데이터도 정확히 분석할 수 있어야 한다.

### 6.5 보안

- API 엔드포인트는 최소한의 인증(API Key 또는 내부망 접근 제한)을 적용한다.
- Redis, ClickHouse, Kafka는 운영 환경 기준 인증(AUTH, SASL, 사용자 권한 분리)을 적용한다.

### 6.6 운영 / 관측성 (Observability)

- Kafka Consumer Lag, 처리량, 에러율, Redis 히트율, ClickHouse 적재 지연을 실시간으로 모니터링할 수 있어야 한다.
- 주요 장애 상황(Consumer Lag 급증, DLQ 적재 발생 등)에 대한 알림 체계를 갖춘다.

---

## 7. 요구사항 요약 표

| ID | 구분 | 요구사항 | 우선순위 |
|---|---|---|---|
| FR-01 | 기능 | click/payment 이벤트 수집 API | P0 |
| FR-02 | 기능 | Kafka 토픽 발행 (토픽별 신뢰성 정책 차등화) | P0 |
| FR-03 | 기능 | Redis 기반 멱등성 체크 | P0 |
| FR-04 | 기능 | ClickHouse 배치 적재 | P0 |
| FR-05 | 기능 | DLQ / 재시도 처리 | P1 |
| FR-06 | 기능 | AI 연동을 위한 Consumer Group 분리 구조 | P1 |
| NFR-01 | 성능 | 3,000 events/sec 이상 처리 | P0 |
| NFR-02 | 신뢰성 | payment-events 유실 0% | P0 |
| NFR-03 | 운영 | Consumer Lag / 에러율 모니터링 | P1 |
| NFR-04 | 보안 | 기본 인증 및 접근 제어 | P1 |
| NFR-05 | 확장성 | 파티션 기반 Consumer 수평 확장 | P1 |

---

## 8. 리스크 및 제약사항

| 리스크 | 영향 | 완화 방안 |
|---|---|---|
| 1인 개발로 인한 개발 기간 제약 | 일정 지연 | 1차 범위를 P0 요구사항으로 한정, 단계적 마일스톤 운영 |
| 로컬/단일 노드 환경에서의 부하 테스트 한계 | 실제 운영 환경과의 성능 차이 발생 가능 | 테스트 결과에 리소스 스펙을 명시하고, 상대적 개선폭 중심으로 검증 |
| Kafka/ClickHouse 운영 경험 부족 | 초기 설정 오류 가능성 | 공식 문서 기반 설정값 사용, 단계적으로 부하를 올리며 검증 |
| Redis 단일 장애점(SPOF) | 멱등성 체크 실패 시 전체 영향 가능 | click은 fail-open, payment는 fail-closed + ClickHouse 이중 방어로 리스크 분산 |

---

## 9. 마일스톤 / 로드맵

| Phase | 목표 | 주요 산출물 |
|---|---|---|
| Phase 1 | 기본 파이프라인 구축 | Docker Compose 환경, API 서버, Kafka 토픽, 기본 Consumer, ClickHouse 스키마 |
| Phase 2 | 신뢰성 강화 | Redis 멱등성 체크, acks=all 설정, DLQ/재시도, ReplacingMergeTree 적용 |
| Phase 3 | 성능 검증 | 부하 테스트(k6), 파티션/Consumer 스케일 튜닝, 목표 처리량 달성 검증 |
| Phase 4 | 운영 고도화 | 모니터링 대시보드, 알림, AI 연동 인터페이스 문서화 |

---

## 10. 수용 기준 (Acceptance Criteria)

- [ ] 초당 3,000건 부하 테스트에서 API 서버 에러율 1% 미만, p99 응답시간 목표치 이내
- [ ] 의도적으로 동일 eventId를 중복 발행했을 때 ClickHouse 최종 적재 건수가 1건으로 유지됨
- [ ] Kafka 브로커 1대를 강제 중단해도 payment-events가 유실되지 않고 정상 적재됨 (재기동 후 확인)
- [ ] Redis를 강제로 중단했을 때 click-events는 서비스가 계속되고, payment-events는 재시도/DLQ로 안전하게 처리됨
- [ ] Consumer를 중단한 상태로 일정 시간 트래픽을 발생시켜도 Kafka에 안전하게 적재되며, Consumer 재기동 시 유실 없이 순차 처리됨
- [ ] AI 연동을 가정한 별도 Consumer Group으로 동일 토픽을 구독했을 때 기존 파이프라인의 처리량/지연에 영향이 없음

---

## 11. 부록: 참고 기술 스택 버전 (2026년 9월 기준)

| 컴포넌트 | 권장 버전 | 비고 |
|---|---|---|
| Apache Kafka | 4.3.x (KRaft 모드) | Zookeeper 없이 Controller 노드로 메타데이터 관리 |
| ClickHouse | 25.8.x LTS 계열 | 장기 지원 버전 권장, 신규 기능 필요 시 26.x stable 검토 |
| Redis | 7.x 계열 | Cluster/Sentinel 구성은 운영 단계에서 검토 |
| NestJS | 최신 안정 버전 | 구현 시점 기준 공식 문서에서 재확인 권장 |
| Node.js | 최신 LTS | 구현 시점 기준 공식 문서에서 재확인 권장 |

> 위 버전은 문서 작성 시점 기준이며, 실제 구현 시점의 최신 안정/LTS 버전을 다시 확인하는 것을 권장한다.