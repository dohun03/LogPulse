import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { EachBatchPayload, Kafka } from 'kafkajs';
import { clickConsumerConfig, kafkaBrokers, kafkaClientId } from '../config/consumer.config';
import { BatchBuffer } from '../clickhouse/batch-buffer';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { toClickHouseDateTime } from '../common/datetime.util';
import { retryWithBackoff } from '../common/retry.util';
import { RedisDedupService } from '../redis/redis-dedup.service';

// Kafka에서 가져오는 '클릭 이벤트' 인터페이스
interface ClickEventEnvelope {
  eventId: string;
  ingestedAt: string;
  payload: {
    userId: string;
    sessionId: string;
    eventType: 'product_click' | 'page_view';
    productId?: string;
    pageUrl: string;
    occurredAt: string;
    metadata?: Record<string, unknown>;
  };
}

@Injectable()
export class ClickEventsConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ClickEventsConsumer.name);

  // 에러 카운터 변수
  private failedBatchCount = 0;
  private failedRowCount = 0;

  private readonly kafka = new Kafka({
    clientId: kafkaClientId,
    brokers: kafkaBrokers,
  });

  private readonly consumer = this.kafka.consumer({
    groupId: clickConsumerConfig.groupId,
  });

  // "나는 'Record<string, unknown>' (객체) 형태의 데이터를 모아서 처리하는 버퍼를 만들거야!"
  private readonly buffer = new BatchBuffer<Record<string, unknown>>(
    clickConsumerConfig.batchMaxSize,    // 설정된 최대 개수가 모이면 방출
    clickConsumerConfig.batchFlushMs,    // 설정된 최대 시간이 지나면 방출
    (rows) => this.flushWithRetry(rows), // 위 두 조건 중 하나가 만족되면 실행할 실제 저장 함수 지정
  );

  constructor(
    private readonly redisDedup: RedisDedupService,
    private readonly clickhouseWriter: ClickHouseWriterService,
  ) {}

  // NestJS 구동 시 자동 실행: Kafka 연결 및 구독
  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: clickConsumerConfig.topic,
      fromBeginning: false, // 지금 시점 이후의 메시지만 구독
    });
    
    // Kafka로부터 메시지를 배치 단위로 가져와서 처리 시작
    await this.consumer.run({
      autoCommit: clickConsumerConfig.autoCommit,
      eachBatch: async ({ batch, resolveOffset, heartbeat }: EachBatchPayload) => {
        // 배치의 각 메시지 개별 처리 (버퍼에 저장)
        for (const message of batch.messages) {
          const envelope = JSON.parse(message.value!.toString()) as ClickEventEnvelope;

          // Redis 중복 체크 및 마킹
          const dedupResult = await this.redisDedup.checkAndMark('click', envelope.eventId, clickConsumerConfig.dedupTtlSec);

          // 신규 이벤트이거나 에러일 경우에만 처리 진행
          if (dedupResult === 'NEW' || dedupResult === 'ERROR') {
            this.buffer.add(this.toClickRow(envelope));
          }

          // 오프셋 커밋
          resolveOffset(message.offset);
        }

        // Kafka 브로커에게 신호 보냄 (끊김 방지)
        await heartbeat();
      },
    });

    this.logger.log(`ClickEventsConsumer subscribed to ${clickConsumerConfig.topic}`);
  }

  // NestJS 종료시 Kafka 연결 해제
  async onModuleDestroy() {
    await this.consumer.disconnect();
  }

  // 버퍼에 데이터가 쌓여서 방출될 때 실행되는 실제 DB 저장 함수
  private async flushWithRetry(rows: Record<string, unknown>[]) {
    try {
      // 첫 실행(+재시도 2회) 후 실패하면 batch 폐기
      await retryWithBackoff(
        () => this.clickhouseWriter.insertClickEvents(rows),
        2,
        0,
      );
    } catch (err) {
      this.failedBatchCount += 1;
      this.failedRowCount += rows.length;

      this.logger.warn(
        {
          droppedRows: rows.length,
          failedBatchCount: this.failedBatchCount,
          failedRowCount: this.failedRowCount,
        },
        'ClickHouse 배치 적재 최종 실패, batch 폐기',
      );
    }
  }

  // DB 컬럼명에 맞게 매핑
  private toClickRow(envelope: ClickEventEnvelope): Record<string, unknown> {
    const p = envelope.payload;

    return {
      event_id: envelope.eventId,
      user_id: p.userId,
      session_id: p.sessionId,
      event_type: p.eventType,
      product_id: p.productId ?? null,
      page_url: p.pageUrl,
      occurred_at: toClickHouseDateTime(p.occurredAt),
      ingested_at: toClickHouseDateTime(envelope.ingestedAt),
      metadata: JSON.stringify(p.metadata ?? {}),
    };
  }
}
