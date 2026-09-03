import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import { EachBatchPayload, Kafka } from 'kafkajs';

import {
  clickConsumerConfig,
  kafkaBrokers,
  kafkaClientId,
} from '../config/consumer.config';
import { BatchBuffer } from '../clickhouse/batch-buffer';
import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { retryWithBackoff } from '../common/retry.util';
import { RedisDedupService } from '../redis/redis-dedup.service';

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
export class ClickEventsConsumer
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(
    ClickEventsConsumer.name,
  );

  private failedBatchCount = 0;
  private failedRowCount = 0;

  private readonly kafka = new Kafka({
    clientId: kafkaClientId,
    brokers: kafkaBrokers,
  });

  private readonly consumer = this.kafka.consumer({
    groupId: clickConsumerConfig.groupId,
  });

  private readonly buffer = new BatchBuffer<
    Record<string, unknown>
  >(
    clickConsumerConfig.batchMaxSize,
    clickConsumerConfig.batchFlushMs,
    (rows) => this.flushWithRetry(rows),
  );

  constructor(
    private readonly redisDedup: RedisDedupService,
    private readonly clickhouseWriter: ClickHouseWriterService,
  ) {}

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: clickConsumerConfig.topic,
      fromBeginning: false,
    });

    await this.consumer.run({
      autoCommit: clickConsumerConfig.autoCommit,

      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
      }: EachBatchPayload) => {
        for (const message of batch.messages) {
          const envelope = JSON.parse(
            message.value!.toString(),
          ) as ClickEventEnvelope;

          const dedupResult =
            await this.redisDedup.checkAndMark(
              'click',
              envelope.eventId,
              clickConsumerConfig.dedupTtlSec,
            );

          if (
            dedupResult === 'NEW' ||
            dedupResult === 'ERROR'
          ) {
            this.buffer.add(
              this.toClickRow(envelope),
            );
          }

          resolveOffset(message.offset);
        }

        await heartbeat();
      },
    });

    this.logger.log(
      `ClickEventsConsumer subscribed to ${clickConsumerConfig.topic}`,
    );
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }

  private async flushWithRetry(
    rows: Record<string, unknown>[],
  ) {
    try {
      // 즉시 재시도 2회(총 3회 시도) 후에도 실패하면 batch 폐기
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

  private toClickRow(
    envelope: ClickEventEnvelope,
  ): Record<string, unknown> {
    const p = envelope.payload;

    return {
      event_id: envelope.eventId,
      user_id: p.userId,
      session_id: p.sessionId,
      event_type: p.eventType,
      product_id: p.productId ?? null,
      page_url: p.pageUrl,
      occurred_at: this.toClickHouseDateTime(
        p.occurredAt,
      ),
      ingested_at: this.toClickHouseDateTime(
        envelope.ingestedAt,
      ),
      metadata: JSON.stringify(p.metadata ?? {}),
    };
  }

  // ClickHouse DateTime64는 'YYYY-MM-DD HH:MM:SS.mmm' 형식을 요구하므로
  // ISO 8601(예: 2026-09-03T06:30:00.000Z)을 UTC 기준으로 변환한다.
  private toClickHouseDateTime(iso: string): string {
    return new Date(iso)
      .toISOString()
      .replace('T', ' ')
      .replace('Z', '');
  }
}
