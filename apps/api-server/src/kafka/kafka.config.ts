import { CompressionTypes, CompressionCodecs } from 'kafkajs';
import LZ4Codec from '@2l/kafkajs-lz4';

CompressionCodecs[CompressionTypes.LZ4] =
  new LZ4Codec().codec;

export const clickProducerConfig = {
  acks: 1,
  compression: CompressionTypes.LZ4,
  maxInFlightRequests: 5,
} as const;

export const paymentProducerConfig = {
  acks: -1,
  idempotent: true,
  maxInFlightRequests: 5,
  compression: CompressionTypes.LZ4,
} as const;