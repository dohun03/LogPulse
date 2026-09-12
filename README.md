# 🛡️ LogPulse

> Kafka 기반 대규모 로그/이벤트 수집 및 장애 격리 파이프라인

---

## 📋 프로젝트 개요

고트래픽 환경에서 로그/이벤트 데이터를 유실 없이 수집·처리하기 위한 파이프라인입니다. Click(행동 로그)과 Payment(결제 이벤트)를 서로 다른 신뢰도 정책으로 분리 처리하며, Kafka batch 단위 최적화로 Redis/ClickHouse I/O 왕복을 최소화했습니다.

```text
Client → Nginx → API 서버 2대 → Kafka(3 브로커) → Click/Payment Consumer → Redis Dedup → ClickHouse
```

* **진행 기간**: 2026.09
* **참여 인원**: 1명 (개인 프로젝트)

---

## 🛠️ 기술 스택

<div align="left">
  <img src="https://img.shields.io/badge/NestJS-E0234E?style=flat-square&logo=nestjs&logoColor=white" />
  <img src="https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white" />
  <img src="https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white" />
  <img src="https://img.shields.io/badge/ClickHouse-FFCC01?style=flat-square&logo=clickhouse&logoColor=black" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white" />
  <img src="https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white" />
  <img src="https://img.shields.io/badge/k6-7D64FF?style=flat-square&logo=k6&logoColor=white" />
</div>

| 기술 | 용도 및 역할 |
| :--- | :--- |
| **NestJS / TypeScript** | API 서버 및 Consumer Worker |
| **Kafka (KRaft, 3 Broker)** | 이벤트 스트리밍, Consumer Group 기반 파티션 분배 |
| **Redis** | Kafka batch 단위 Dedup |
| **ClickHouse** | Click(MergeTree) / Payment(ReplacingMergeTree) 영구 저장 |
| **Nginx** | API 서버 2대 Round Robin, 장애 우회 |
| **Prometheus / Grafana** | Kafka Lag, Fail-Open/Closed, DLQ 등 실시간 모니터링 |
| **k6** | 시나리오 기반 부하 테스트 |

---

## 🧯 장애 복구 검증

> 전체 검증 과정은 **[장애 복구 테스트 리포트](./FAULT_TOLERANCE.md)** 참고

부하 테스트에 앞서, API/Kafka/Redis/ClickHouse를 각각 실제로 종료시켜 8개 장애·복구 시나리오를 재현했습니다.

| 시나리오 | 결과 |
|---|---|
| API 서버 1대 장애 | ✅ Nginx가 나머지 1대로 트래픽 유지, 복귀 후 재분산 확인 |
| Kafka Broker 1대 장애 | ✅ RF/ISR 범위 내 Cluster 정상 동작, Partition 재할당 확인 |
| Redis 장애 — Click | ✅ Fail-Open으로 처리 계속 |
| Redis 장애 — Payment | ✅ Fail-Closed로 안전하게 처리 지연 |
| ClickHouse 장애 — Click | ✅ Batch Flush 재시도 2회 후 폐기 정책대로 동작 |
| ClickHouse 장애 — Payment | ✅ Retry 소진 시 배치 전체 DLQ 이관, Offset 조기 커밋 없음 |
| Consumer Rebalance — Click | ✅ 남은 Consumer에게 Partition 자동 재할당 |
| Consumer Rebalance — Payment | ✅ 남은 Consumer에게 Partition 자동 재할당 |

---

## ✅ 성능 검증 결과 (k6 부하 테스트)

> 자세한 내용은 **[부하 테스트 전체 리포트](./LOAD_TEST.md)** 참고

| 항목 | 값 |
|---|---|
| 목표 처리량 | 3,000 TPS 이상 (순간 5,000 TPS 대응) |
| **실측 안정 처리량** | **약 1,000 TPS** (에러율 0%, Kafka Lag 0, 30분간 180만 건 무손실 처리) |
| 2,000 TPS 부하 시 | 에러율 약 10.78% (WSL2 로컬 환경의 Kafka 브로커 produce 처리량 한계) |
| 3,000 TPS 최초 시도 시 | 에러율 83.62% → 8단계 디버깅으로 10.78%까지 단계적 개선 |
| 병목 원인 | 애플리케이션 로직 결함이 아닌 **WSL2 로컬 인프라 리소스 제약**으로 최종 규명 |

목표치(3,000 TPS)는 미달성했지만, Nginx / API / Kafka 3개 레이어에 걸쳐 8개의 병목 후보 가설을 데이터로 하나씩 검증·반증하며 실측 한계치를 확정했습니다.

---

## ✨ 핵심 기능

- **Click / Payment 이원화 신뢰도 정책** — Click은 Best-effort/Fail-Open, Payment는 Fail-Closed + DLQ
- **Kafka batch 단위 Redis Dedup** — 메시지별 순차 `await` 대신 MGET + Pipeline으로 Network Round Trip 최소화
- **Payment Kafka batch 기반 Bulk Insert** — ClickHouse 저장 성공 후에만 Redis 완료 마킹 (유실 방지 순서 보장)
- **DLQ + Exponential Backoff** — Payment 저장 실패 시 재시도 후 DLQ 이관, 실패 시 Offset 미커밋으로 재처리 보장
- **Prometheus + Grafana 실시간 모니터링** — Consumer Lag, Fail-Open/Closed 횟수, DLQ 발행 현황 추적

---

## ⚡ 핵심 트러블슈팅

<details>
<summary><b>1. Redis 순차 호출 제거 — Kafka batch 단위 Dedup</b></summary>

Kafka batch를 순회하며 메시지마다 Redis를 `await`하는 구조는 N번의 순차 Network Round Trip을 유발함 → batch 내부 중복 제거 후 Redis MGET(Batch Read) + Pipeline SET NX로 전환 → 연산량(O(N))은 유지하되 Network I/O만 배치화.
</details>

<details>
<summary><b>2. Payment 데이터 유실 방지를 위한 처리 순서 설계</b></summary>

"Redis 완료 마킹 → ClickHouse 저장" 순서로 구현하면, 저장 실패 후 재시도 시 Redis가 DUPLICATE로 판정해 데이터가 영구 유실될 위험이 있음 → "ClickHouse 저장 성공 → Redis 완료 마킹 → Offset Commit" 순서로 고정.
</details>

<details>
<summary><b>3. 목표 처리량(3,000 TPS) 미달 원인 8단계 추적</b></summary>

k6 부하 테스트 초기 에러율 83.62%. Nginx 502 로그, API 응답 지연 분포, Kafka Consumer Lag을 교차 분석하며 Rate Limit → Kafka Producer 백프레셔 → Nginx 커넥션 고갈 → Kafka produce 블로킹 → LZ4 압축 → Nginx 헬스체크 민감도 → Payment acks=all → 메인 스레드 블로킹까지 8개 가설을 순서대로 검증/반증. 최종적으로 WSL2 로컬 Kafka 브로커의 produce 처리량 한계로 확정하고, 안정적 처리량(약 1,000 TPS)을 실측으로 확정.
</details>

---

## 🏗️ 서비스 아키텍처

<img width="100%" alt="Image" src="https://github.com/user-attachments/assets/2b284060-d74a-47fc-ae4e-088045ae70ba" />

> 다이어그램 이미지는 추가 예정

---

## 📖 API 명세서

| 엔드포인트 | 메서드 | 핵심 기능 |
| :--- | :---: | :--- |
| `/events/click` | `POST` | Click 이벤트 수집 (Kafka Key: `sessionId`) |
| `/events/payment` | `POST` | Payment 이벤트 수집 (Kafka Key: `orderId`) |
| `/health`, `/ready` | `GET` | 헬스체크 / 레디니스 |
| `/metrics` | `GET` | Prometheus 메트릭 |

두 엔드포인트 모두 Kafka Publish 성공 시에만 `202 Accepted`를 반환하며, API 서버는 ClickHouse/Redis에 직접 쓰지 않는 Stateless 구조입니다.

---

## 🗄️ 데이터 정책 (핵심)

| 구분 | Click | Payment |
| :--- | :--- | :--- |
| Redis 장애 정책 | Fail-Open | Fail-Closed |
| Offset Commit | `autoCommit=true` | `autoCommit=false`, 저장 성공 후에만 |
| 실패 시 처리 | 재시도 2회 후 폐기 (Best-effort) | Retry → DLQ → 실패 시 Offset 미커밋 |
| ClickHouse 테이블 | `click_events` (MergeTree) | `payment_events` (ReplacingMergeTree, `order_id`+`event_id` 기준 최종 중복 해소) |

---

## 💭 회고

- **목표 3,000 TPS를 못 채운 것을 어떻게 받아들였는지**: 처음엔 에러율 83.62%를 보고 애플리케이션 설계 결함이라 의심했지만, Kafka Lag이 항상 0이라는 사실이 "백엔드 처리 능력은 충분한데 뭔가 다른 게 막고 있다"는 단서였습니다. 결과를 그대로 받아들이지 않고 각 레이어(Nginx/API/Kafka)의 로그를 분리해서 원인을 좁혀간 과정이 이 프로젝트에서 가장 남는 경험입니다.
- **틀렸던 진단도 기록으로 남긴 이유**: 1차 진단(Nginx 커넥션 고갈)은 틀렸고, 수정해도 효과가 없었습니다. 이걸 지우지 않고 "왜 틀렸는지"까지 남긴 이유는, 실제 트러블슈팅은 한 번에 정답을 맞히는 게 아니라 가설을 세우고 데이터로 반증하는 과정의 반복이라고 생각했기 때문입니다.
- **다음에 다시 한다면**: WSL2 로컬 환경이 아닌 실제 서버(클라우드 인스턴스) 환경에서 동일 테스트를 재실행해, 지금 찾은 한계(1,000 TPS)가 로컬 리소스 제약이 맞는지 교차 검증하고 싶습니다.

---

## 🚀 실행 방법

```bash
git clone https://github.com/dohun03/LogPulse.git
cd LogPulse
docker compose up -d --build
```