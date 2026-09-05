import { CompressionCodecs, CompressionTypes } from 'kafkajs';
import LZ4Codec from '@2l/kafkajs-lz4';

// API Producer가 LZ4로 압축해 발행하므로, Consumer에서도 동일 codec을 등록해야 Fetch 시 압축을 해제할 수 있다.
CompressionCodecs[CompressionTypes.LZ4] = new LZ4Codec().codec;

export const kafkaClientId = process.env.KAFKA_CLIENT_ID ?? 'logpulse-consumer-worker';
export const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

// 클릭 컨슈머 설정 값
export const clickConsumerConfig = {
  groupId: process.env.CLICK_CONSUMER_GROUP_ID ?? 'logpulse-click-loader',
  topic: process.env.KAFKA_CLICK_TOPIC ?? 'click-events',
  autoCommit: true,
  batchMaxSize: Number(process.env.CLICK_BATCH_MAX_SIZE ?? 500),
  batchFlushMs: Number(process.env.CLICK_BATCH_FLUSH_MS ?? 1000),
  dedupTtlSec: Number(process.env.REDIS_CLICK_DEDUP_TTL_SEC ?? 600),
};

// 결제 컨슈머 설정 값
export const paymentConsumerConfig = {
  groupId: process.env.PAYMENT_CONSUMER_GROUP_ID ?? 'logpulse-payment-loader',
  topic: process.env.KAFKA_PAYMENT_TOPIC ?? 'payment-events',
  dlqTopic: process.env.KAFKA_PAYMENT_DLQ_TOPIC ?? 'payment-events-dlq',
  autoCommit: false,
  maxRetry: Number(process.env.PAYMENT_MAX_RETRY ?? 3),
  dedupTtlSec: Number(process.env.REDIS_PAYMENT_DEDUP_TTL_SEC ?? 86400),
};
