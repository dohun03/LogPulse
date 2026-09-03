import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { EachBatchPayload, Kafka } from 'kafkajs';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { toClickHouseDateTime } from '../common/datetime.util';
import { retryWithBackoff } from '../common/retry.util';
import {
  kafkaBrokers,
  kafkaClientId,
  paymentConsumerConfig,
} from '../config/consumer.config';
import { DlqProducerService } from '../dlq/dlq-producer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';

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
export class PaymentEventsConsumer
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(
    PaymentEventsConsumer.name,
  );

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

  async onModuleInit() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: paymentConsumerConfig.topic,
      fromBeginning: false,
    });

    await this.consumer.run({
      autoCommit: paymentConsumerConfig.autoCommit,

      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
      }: EachBatchPayload) => {
        for (const message of batch.messages) {
          await this.processMessage(message, resolveOffset);
        }

        await heartbeat();

        // autoCommit=false이므로 해결(resolve)된 offset을 명시적으로 커밋한다.
        // 성공 또는 DLQ 이관이 끝난 offset만 resolve되므로, 커밋은
        // "저장/안전한 DLQ 처리 이후"에만 일어난다.
        await commitOffsetsIfNecessary(
          uncommittedOffsets(),
        );
      },
    });

    this.logger.log(
      `PaymentEventsConsumer subscribed (group=${paymentConsumerConfig.groupId}, topic=${paymentConsumerConfig.topic})`,
    );
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
    const envelope = JSON.parse(
      message.value!.toString(),
    ) as PaymentEventEnvelope;

    // 1) Redis 중복 확인. Redis 장애 시 isDuplicate가 throw → Fail-Closed.
    const isDuplicate = await retryWithBackoff(
      () =>
        this.redisDedup.isDuplicate(
          'payment',
          envelope.eventId,
        ),
      3,
    );

    // 2) DUPLICATE면 offset 처리 후 다음 메시지로.
    if (isDuplicate) {
      resolveOffset(message.offset);
      return;
    }

    const row = this.toPaymentRow(envelope);

    try {
      // 3) ClickHouse 저장(지수 백오프 재시도).
      await retryWithBackoff(
        () =>
          this.clickhouseWriter.insertPaymentEvents([row]),
        paymentConsumerConfig.maxRetry,
      );

      // 4) ClickHouse 저장 성공 후 Redis 처리 완료 키 기록.
      await retryWithBackoff(
        () =>
          this.redisDedup.markProcessed(
            'payment',
            envelope.eventId,
            paymentConsumerConfig.dedupTtlSec,
          ),
        3,
      );

      // 5) 성공 시 offset 처리.
      resolveOffset(message.offset);
    } catch (err) {
      // ClickHouse 재시도가 모두 실패하면 DLQ로 이관한다.
      // 민감한 원본 값(orderId/amount 등)은 로그에 남기지 않는다.
      this.logger.error(
        {
          eventId: envelope.eventId,
          topic: paymentConsumerConfig.topic,
          error: (err as Error).message,
        },
        'ClickHouse 적재 최종 실패, DLQ 이관',
      );

      // DLQ publish가 실패하면 throw되어 offset을 resolve하지 않으므로
      // 다음 rebalance/재처리 시 다시 시도된다.
      await this.dlqProducer.send(
        envelope,
        (err as Error).message,
      );

      resolveOffset(message.offset);
    }
  }

  private toPaymentRow(
    envelope: PaymentEventEnvelope,
  ): Record<string, unknown> {
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
