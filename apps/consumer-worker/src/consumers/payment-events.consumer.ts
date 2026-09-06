import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { EachBatchPayload, Kafka } from 'kafkajs';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { toClickHouseDateTime } from '../common/datetime.util';
import { retryWithBackoff } from '../common/retry.util';
import { kafkaBrokers, kafkaClientId, paymentConsumerConfig } from '../config/consumer.config';
import { DlqProducerService } from '../dlq/dlq-producer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';
import { kafkaConsumerLag, paymentBatchInsertFailuresTotal, paymentBatchInsertTotal, paymentDlqSendFailuresTotal, paymentDlqSentTotal, redisDedupFailClosedRetryTotal } from '../metrics/metrics';

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
        // 현재 파티션의 처리 지연(Lag)을 지표로 기록한다.
        kafkaConsumerLag.set(
          {
            topic: paymentConsumerConfig.topic,
            partition: String(batch.partition),
            consumer_group: paymentConsumerConfig.groupId,
          },
          Number(batch.offsetLag()),
        );

        // Kafka batch 전체를 파싱/처리한다.
        const envelopes = batch.messages.map(
          (message) => JSON.parse(message.value!.toString()) as PaymentEventEnvelope,
        );

        await this.processBatch(envelopes);

        // 배치 단위 처리 후 생존 신고
        await heartbeat();

        // 정상 저장 / DUPLICATE / DLQ 성공으로 안전 종료된 offset을 resolve 후 명시적으로 커밋한다.
        for (const message of batch.messages) {
          resolveOffset(message.offset);
        }
        await commitOffsetsIfNecessary(uncommittedOffsets());
      },
    });

    this.logger.log(`PaymentEventsConsumer subscribed (group=${paymentConsumerConfig.groupId}, topic=${paymentConsumerConfig.topic})`);
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }

  // Payment는 Fail-Closed/정확성 우선 정책을 따른다.
  // 내부 중복 제거 → Redis Batch Read → ClickHouse Bulk Insert 1회 → Redis 완료 마킹
  private async processBatch(envelopes: PaymentEventEnvelope[]): Promise<void> {
    // batch 내부 eventId 중복 제거
    const unique = new Map<string, PaymentEventEnvelope>();
    for (const envelope of envelopes) {
      if (!unique.has(envelope.eventId)) {
        unique.set(envelope.eventId, envelope);
      }
    }
    const uniqueList = [...unique.values()];

    // Redis 배치 단위로 중복되는 키 목록 불러오기
    const existing = await retryWithBackoff(
      () =>
        this.redisDedup.batchGetExisting(
          'payment',
          uniqueList.map((envelope) => envelope.eventId),
        ),
      paymentConsumerConfig.maxRetry,
      200,
      () => redisDedupFailClosedRetryTotal.inc(),
    );

    // 중복 이벤트 제외
    const newEvents = uniqueList.filter(
      (envelope) => !existing.has(envelope.eventId),
    );

    if (newEvents.length === 0) {
      return;
    }

    // DB 컬럼명으로 일괄 매핑
    const rows = newEvents.map((envelope) => this.toPaymentRow(envelope));

    try {
      // 성공: DB 저장 (Bulk Insert)
      await retryWithBackoff(
        () => this.clickhouseWriter.insertPaymentEvents(rows),
        paymentConsumerConfig.maxRetry,
      );
      paymentBatchInsertTotal.inc();
    } catch (err) {
      paymentBatchInsertFailuresTotal.inc();
      // 실패: 유효 이벤트를 DLQ로 이관한다.(배치 내부의 유효하지 않은 DUPLICATE 이벤트는 DLQ로 보내지 않는다.)
      const reason = (err as Error).message;

      // 민감한 원본 값(orderId/amount 등)은 로그에 남기지 않는다.
      this.logger.error(
        {
          eventCount: newEvents.length,
          topic: paymentConsumerConfig.topic,
          error: reason,
        },
        'ClickHouse Bulk Insert 최종 실패, 유효 이벤트 DLQ 이관',
      );

      // DLQ 이관
      await this.sendToDlq(newEvents, reason);
      return;
    }

    // 성공: Redis 완료 마킹.
    // 실패: 예외 전파 → offset 미커밋 → 재시도.
    await retryWithBackoff(
      () =>
        this.redisDedup.batchMarkIfAbsent(
          'payment',
          newEvents.map((envelope) => envelope.eventId),
          paymentConsumerConfig.dedupTtlSec,
        ),
      paymentConsumerConfig.maxRetry,
      200,
      () => redisDedupFailClosedRetryTotal.inc(),
    );
  }

  // 유효 이벤트를 DLQ로 이관한다. Retry 후에도 실패하면 예외를 전파해 offset을 커밋하지 않는다.
  private async sendToDlq(
    events: PaymentEventEnvelope[],
    reason: string,
  ): Promise<void> {
    for (const event of events) {
      try {
        await retryWithBackoff(
          () => this.dlqProducer.send(event, reason),
          paymentConsumerConfig.maxRetry,
        );
        paymentDlqSentTotal.inc();
      } catch (err) {
        paymentDlqSendFailuresTotal.inc();
        throw err;
      }
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
      ingested_at: toClickHouseDateTime(envelope.ingestedAt),
    };
  }
}
