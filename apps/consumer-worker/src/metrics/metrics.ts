import {
  collectDefaultMetrics,
  Counter,
  Gauge,
  register,
} from 'prom-client';

/**
 * Node.js 프로세스 기본 지표 수집기 활성화
 * - CPU / Memory / Event Loop 지연 / GC 지표 자동 수집
 */
collectDefaultMetrics();

// ── Click Consumer 지표 (Best-effort / Fail-open 관측) ──
export const clickRedisBatchDedupTotal = new Counter({
  name: 'click_redis_batch_dedup_total',
  help: 'Click Consumer가 Kafka batch 단위 Redis 배치 Dedup으로 처리한 이벤트 수',
});

export const redisDedupFailOpenTotal = new Counter({
  name: 'redis_dedup_fail_open_total',
  help: 'Click Consumer Redis Dedup 실패 시 fail-open 처리 횟수',
});

export const clickBatchDroppedTotal = new Counter({
  name: 'click_batch_dropped_total',
  help: 'Click Consumer BatchBuffer 적재 최종 실패로 폐기된 이벤트 수',
});

// ── Payment Consumer 지표 (Guaranteed / Fail-closed 관측) ──
export const redisDedupFailClosedRetryTotal = new Counter({
  name: 'redis_dedup_fail_closed_retry_total',
  help: 'Payment Consumer Redis Dedup 실패 시 fail-closed 재시도 횟수',
});

export const paymentBatchInsertTotal = new Counter({
  name: 'payment_batch_insert_total',
  help: 'Payment Consumer ClickHouse Bulk Insert 성공 횟수',
});

export const paymentBatchInsertFailuresTotal = new Counter({
  name: 'payment_batch_insert_failures_total',
  help: 'Payment Consumer ClickHouse Bulk Insert 최종 실패 횟수',
});

export const paymentDlqSentTotal = new Counter({
  name: 'payment_dlq_sent_total',
  help: 'Payment Consumer DLQ 발행 성공 횟수',
});

export const paymentDlqSendFailuresTotal = new Counter({
  name: 'payment_dlq_send_failures_total',
  help: 'Payment Consumer DLQ 발행 실패 횟수',
});

// ── Kafka Consumer Lag (실시간 지연 관측) ──
/**
 * - 각 Consumer 인스턴스가 Kafka eachBatch 처리 시 batch.offsetLag()로 현재 파티션의 미처리 메시지 잔량을 기록
 * - 라벨: topic, partition, consumer_group
 */
export const kafkaConsumerLag = new Gauge({
  name: 'kafka_consumer_lag',
  help: 'Kafka Consumer Group 파티션별 처리 지연(Lag)',
  labelNames: ['topic', 'partition', 'consumer_group'],
});

export { register };

