# LogPulse 장애 복구 테스트 리포트

- 대상: LogPulse 이벤트 수집 파이프라인 (API 2대 / Kafka 3브로커 / Consumer 5대 / Redis / ClickHouse)
- 방법: 각 컴포넌트를 실제로 종료시켜 장애를 재현하고, 로그가 아닌 실제 상태(Kafka Offset, Consumer Group, ClickHouse Row, Grafana 지표)로 복구를 확인
- 목표: 8개 장애 시나리오 실제 재현으로 Fail-Open/Fail-Closed 정책 검증

[← README로 돌아가기](./README.md)

---

## 0. 종합 요약

| 시나리오 | 확인 내용 | 결과 |
|---|---|---|
| API 서버 장애 | API #1 종료 시 Nginx가 API #2로 트래픽 유지, 복귀 후 재분산 | ✅ 통과 |
| Kafka Broker 장애 | Broker 1대 종료 시 RF/ISR 범위 내 Cluster 정상, Partition Reassignment | ✅ 통과 |
| Redis 장애 — Click | Fail-Open (Redis 없이도 처리 계속) | ✅ 통과 |
| Redis 장애 — Payment | Fail-Closed (배치 내 Redis 조회 실패 시 재시도 후 처리 지연) | ✅ 통과 |
| ClickHouse 장애 — Click | Batch Flush 재시도 2회 후 폐기 정책대로 동작 | ✅ 통과 |
| ClickHouse 장애 — Payment | Retry → 최종 실패 시 배치 전체 DLQ 이관, Offset 조기 커밋 없음 | ✅ 통과 |
| Consumer Rebalance — Click | Consumer 1대 종료 시 남은 Consumer에게 Partition 자동 재할당 | ✅ 통과 |
| Consumer Rebalance — Payment | 동일하게 Payment Consumer Partition 자동 재할당 | ✅ 통과 |

모든 시나리오는 Grafana 대시보드의 실시간 지표(Kafka Lag, Fail-Open/Closed 횟수, DLQ 발행 수)와 함께 교차 확인했습니다.

---

## 1. API 서버 장애

**시나리오**: API #1을 종료 → 재실행

| 확인 항목 | 결과 |
|---|---|
| Nginx가 계속 요청을 받는가 | ✅ |
| API #2가 계속 처리하는가 | ✅ |
| API #1 재실행 시 Cluster 복귀 | ✅ |
| 재실행 후 요청이 다시 2대로 분산되는가 | ✅ |

---

## 2. Kafka Broker 장애

**시나리오**: Kafka Broker 1대를 종료 → 재실행

| 확인 항목 | 결과 |
|---|---|
| RF/min ISR 범위 내에서 Cluster 정상 동작 | ✅ |
| Producer가 명세대로 동작 (Click `acks=1`, Payment `acks=all`) | ✅ |
| Consumer가 명세대로 동작 | ✅ |
| 필요한 Partition Recovery/Reassignment 발생 | ✅ |
| Broker 재실행 시 Cluster 재참여 | ✅ |
| Replica 상태 회복 | ✅ |

---

## 3. Redis 장애

### Click — Fail-Open

Redis 연결이 끊긴 상태에서도 Click Consumer는 처리를 중단하지 않고 계속 진행하는지 확인했습니다. (중복 저장 허용)
**결과**: ✅ Fail-Open으로 정상 동작. Redis 복구 후 다시 정상 Dedup 경로로 복귀.

### Payment — Fail-Closed

Payment는 배치 내 Redis 조회 자체가 실패하면 안전하게 처리를 지연시켜야 합니다(데이터 유실 방지가 우선).

**결과**: ✅ Redis 장애 시 재시도 후에도 실패하면 처리가 지연되는 것을 확인 (Fail-Closed). Redis 복구 후 정상 처리 재개.

---

## 4. ClickHouse 장애

### Click

ClickHouse를 종료한 상태에서 BatchBuffer의 Flush가 실패했을 때, 설계한 정책(재시도 2회 → 실패 시 batch 폐기 + warn 로그 + 실패 건수 기록)대로 동작하는지 확인했습니다.

**결과**: ✅ 정책대로 동작. ClickHouse 복구 후 이후 batch는 정상 저장.

### Payment

| 확인 항목 | 결과 |
|---|---|
| 배치 Retry 동작 | ✅ |
| Offset 조기 Commit 없음 | ✅ (저장 성공 또는 DLQ 발행 성공 전까지 Offset 미커밋 확인) |
| Retry 최종 실패 시 배치 전체가 DLQ로 이관 | ✅ (poison event 트레이드오프 — 배치 내 한 건이 실패해도 배치 전체가 DLQ 대상이 될 수 있음을 실측으로도 재확인) |
| DLQ 이관까지 실패 시 Offset 미커밋 + 프로세스 재시작 | ✅ |

ClickHouse 복구 후 Consumer가 다시 정상 처리되는 것을 확인했습니다.

---

## 5. Consumer Rebalance

**Click**: Consumer 1대 종료 → Kafka가 남은 Consumer들에게 Partition을 자동 재할당하는지 확인 → ✅
**Payment**: 동일 절차로 확인 → ✅

Consumer를 다시 실행했을 때 정상적으로 Group에 복귀하는지도 확인했으며, 모든 과정에서 Partition을 수동으로 지정하지 않고 Kafka Consumer Group의 자동 할당에 위임했습니다.

---

## 6. 결론

8개 장애 시나리오 모두 설계 의도(Click Fail-Open / Payment Fail-Closed / DLQ / 자동 Rebalance)대로 동작함을 실제 환경에서 검증했습니다.

이 검증은 이후 진행한 [부하 테스트](./LOAD_TEST.md)에서도 유용했습니다. 부하 테스트 중 목표 처리량 미달을 처음 발견했을 때, "이게 장애인가 처리량 한계인가"를 빠르게 구분할 수 있었던 것은 이 단계에서 "정상 상태의 Kafka Lag은 항상 0으로 수렴한다"는 기준을 이미 실측으로 확보해두었기 때문입니다.