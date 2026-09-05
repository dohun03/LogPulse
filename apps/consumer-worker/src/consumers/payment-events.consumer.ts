import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { EachBatchPayload, Kafka } from 'kafkajs';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { toClickHouseDateTime } from '../common/datetime.util';
import { retryWithBackoff } from '../common/retry.util';
import { kafkaBrokers, kafkaClientId, paymentConsumerConfig } from '../config/consumer.config';
import { DlqProducerService } from '../dlq/dlq-producer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';

// Kafka에서 가져오는 '결제 이벤트' 인터페이스
interface PaymentEventEnvelope {
  eventId: string;
  ingestedAt: string;
  payload: {
    eventId: string;
    orderId: string;
    userId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
    status: 'completed' | 'failed' | 'canceled';
    occurredAt: string;
  };
}

@Injectable()
export class PaymentEventsConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PaymentEventsConsumer.name);

  private readonly kafka = new Kafka({
    clientId: kafkaClientId,
    brokers: kafkaBrokers,
  });

  private readonly consumer = this.kafka.consumer({
    groupId: paymentConsumerConfig.groupId,
  });

  constructor(
    private readonly redisDedup: RedisDedupService,
    private readonly clickhouseWriter: ClickHouseWriterService,
    private readonly dlqProducer: DlqProducerService,
  ) {}

  // NestJS 구동 시 자동 실행: Kafka 연결 및 구독
  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: paymentConsumerConfig.topic,
      fromBeginning: false,
    });

    // Kafka로부터 메시지를 배치 단위로 가져와서 처리 시작
    await this.consumer.run({
      autoCommit: paymentConsumerConfig.autoCommit,
      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
      }: EachBatchPayload) => {
        // 배치의 각 메시지 개별 처리 (processMessage)
        for (const message of batch.messages) {
          await this.processMessage(message, resolveOffset);
        }

        // 배치 단위 처리 후 생존 신고
        await heartbeat();

        // 성공 또는 DLQ 이관된 offset을 명시적으로 커밋한다.
        await commitOffsetsIfNecessary(uncommittedOffsets());
      },
    });

    this.logger.log(`PaymentEventsConsumer subscribed (group=${paymentConsumerConfig.groupId}, topic=${paymentConsumerConfig.topic})`);
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }

  // Payment는 Fail-Closed/정확성 우선 정책을 따른다.
  // 저장 성공 또는 DLQ 이관 성공 후에만 offset을 resolve(→ commit)한다.
  private async processMessage(
    message: {
      value: Buffer | null;
      offset: string;
    },
    resolveOffset: (offset: string) => void,
  ) {
    const envelope = JSON.parse(message.value!.toString()) as PaymentEventEnvelope;

    // Redis 중복 확인
    const isDuplicate = await retryWithBackoff(
      () => this.redisDedup.isDuplicate('payment', envelope.eventId),
      3,
    );

    // 중복이면 오프셋 처리 후 마무리
    if (isDuplicate) {
      resolveOffset(message.offset);
      return;
    }

    const row = this.toPaymentRow(envelope);

    try {
      // 건별로 바로 DB 저장 (클릭 이벤트와 다르게 버퍼에 모으지 않음.)
      await retryWithBackoff(
        () => this.clickhouseWriter.insertPaymentEvents([row]),
        paymentConsumerConfig.maxRetry,
      );

      // 성공 후 Redis 처리 완료 키 기록.
      await retryWithBackoff(
        () => this.redisDedup.markProcessed('payment', envelope.eventId, paymentConsumerConfig.dedupTtlSec),
        3,
      );

      // 성공 시 offset 처리.
      resolveOffset(message.offset);
    } catch (err) {
      // 실패하면 DLQ로 이관한다.

      // 민감한 원본 값(orderId/amount 등)은 로그에 남기지 않는다.
      this.logger.error(
        {
          eventId: envelope.eventId,
          topic: paymentConsumerConfig.topic,
          error: (err as Error).message,
        },
        'ClickHouse 적재 최종 실패, DLQ 이관',
      );

      // DLQ 이관도 실패하면 throw되어 offset을 resolve하지 않으므로 다음 rebalance/재처리 시 다시 시도된다.
      await this.dlqProducer.send(envelope, (err as Error).message);

      resolveOffset(message.offset);
    }
  }

  // DB 컬럼명에 맞게 매핑
  private toPaymentRow(envelope: PaymentEventEnvelope): Record<string, unknown> {
    const p = envelope.payload;

    return {
      event_id: envelope.eventId,
      order_id: p.orderId,
      user_id: p.userId,
      amount: p.amount,
      currency: p.currency,
      payment_method: p.paymentMethod,
      status: p.status,
      occurred_at: toClickHouseDateTime(p.occurredAt),
      ingested_at: toClickHouseDateTime(
        envelope.ingestedAt,
      ),
    };
  }
}
