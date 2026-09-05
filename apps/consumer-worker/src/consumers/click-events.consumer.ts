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
        // 배치 전체를 파싱한다.
        const envelopes = batch.messages.map(
          (message) => JSON.parse(message.value!.toString()) as ClickEventEnvelope,
        );

        // Kafka batch 단위 Redis 배치 Dedup 후 선점 성공 이벤트만 버퍼에 적재한다.
        await this.processClickBatch(envelopes);

        // 전체 메시지 오프셋을 resolve한다 (중복/에러 포함: Click은 Best-effort).
        for (const message of batch.messages) {
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

  // Kafka batch 단위로 Redis 배치 Dedup 후 선점 성공 이벤트만 BatchBuffer에 추가한다.
  private async processClickBatch(envelopes: ClickEventEnvelope[]): Promise<void> {
    const claimed = await this.dedupeAndClaim(envelopes);

    for (const envelope of claimed) {
      this.buffer.add(this.toClickRow(envelope));
    }
  }

  // 내부 중복 제거 → Redis MGET → 신규 후보 Pipeline SET NX 선점 순으로 진행한다.
  private async dedupeAndClaim(envelopes: ClickEventEnvelope[]): Promise<ClickEventEnvelope[]> {
    // 1) 동일 batch 내부 eventId 중복 제거 (첫 등장만 유지)
    const unique = new Map<string, ClickEventEnvelope>();
    for (const envelope of envelopes) {
      if (!unique.has(envelope.eventId)) {
        unique.set(envelope.eventId, envelope);
      }
    }
    const uniqueList = [...unique.values()];
    const eventIds = uniqueList.map((envelope) => envelope.eventId);

    // 2) Redis Batch Read(MGET): 기존 key 조회. 실패 시 fail-open 처리.
    let existing: Set<string>;
    try {
      existing = await retryWithBackoff(
        () => this.redisDedup.batchGetExisting('click', eventIds),
        3,
        0,
      );
    } catch (err) {
      this.logger.warn(`Redis Batch Read 실패(fail-open): ${(err as Error).message}`);
      return uniqueList;
    }

    // 3) 기존 key(DUPLICATE) 제외 → 신규 후보
    const candidates = uniqueList.filter(
      (envelope) => !existing.has(envelope.eventId),
    );

    if (candidates.length === 0) {
      return [];
    }

    // 4) Pipeline SET NX 선점. 실패 시 fail-open 처리.
    let claimed: Set<string>;
    try {
      claimed = await this.redisDedup.batchMarkIfAbsent(
        'click',
        candidates.map((envelope) => envelope.eventId),
        clickConsumerConfig.dedupTtlSec,
      );
    } catch (err) {
      this.logger.warn(`Redis Pipeline 선점 실패(fail-open): ${(err as Error).message}`);
      return candidates;
    }

    // 5) 최종 선점 성공 이벤트만 반환
    return candidates.filter((envelope) => claimed.has(envelope.eventId));
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
