export const kafkaClientId =
  process.env.KAFKA_CLIENT_ID ?? 'logpulse-consumer-worker';

export const kafkaBrokers = (
  process.env.KAFKA_BROKERS ?? 'localhost:9092'
).split(',');

export const clickConsumerConfig = {
  groupId:
    process.env.CLICK_CONSUMER_GROUP_ID ??
    'logpulse-click-loader',
  topic: process.env.KAFKA_CLICK_TOPIC ?? 'click-events',
  autoCommit: true,
  batchMaxSize: Number(
    process.env.CLICK_BATCH_MAX_SIZE ?? 500,
  ),
  batchFlushMs: Number(
    process.env.CLICK_BATCH_FLUSH_MS ?? 1000,
  ),
  dedupTtlSec: Number(
    process.env.REDIS_CLICK_DEDUP_TTL_SEC ?? 600,
  ),
};

export const paymentConsumerConfig = {
  groupId:
    process.env.PAYMENT_CONSUMER_GROUP_ID ??
    'logpulse-payment-loader',
  topic:
    process.env.KAFKA_PAYMENT_TOPIC ?? 'payment-events',
  dlqTopic:
    process.env.KAFKA_PAYMENT_DLQ_TOPIC ??
    'payment-events-dlq',
  autoCommit: false,
  maxRetry: Number(process.env.PAYMENT_MAX_RETRY ?? 3),
  dedupTtlSec: Number(
    process.env.REDIS_PAYMENT_DEDUP_TTL_SEC ?? 86400,
  ),
};
