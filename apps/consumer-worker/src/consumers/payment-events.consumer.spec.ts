import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { DlqProducerService } from '../dlq/dlq-producer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';
import { PaymentEventsConsumer } from './payment-events.consumer';

describe('PaymentEventsConsumer', () => {
  let consumer: PaymentEventsConsumer;
  let batchGetExisting: jest.Mock;
  let batchMarkIfAbsent: jest.Mock;
  let insertPaymentEvents: jest.Mock;
  let dlqSend: jest.Mock;

  const envelope = (eventId: string) => ({
    eventId,
    ingestedAt: '2026-09-03T00:00:00.000Z',
    payload: {
      eventId,
      orderId: `order-${eventId}`,
      userId: 'user-1001',
      amount: 49900,
      currency: 'KRW',
      paymentMethod: 'card',
      status: 'completed',
      occurredAt: '2026-09-03T00:00:00.000Z',
    },
  });

  const internals = (c: PaymentEventsConsumer) =>
    c as unknown as {
      processBatch: (envelopes: unknown[]) => Promise<void>;
      sendToDlq: (
        events: unknown[],
        reason: string,
      ) => Promise<void>;
      toPaymentRow: (e: unknown) => Record<string, unknown>;
    };

  beforeEach(() => {
    batchGetExisting = jest
      .fn()
      .mockResolvedValue(new Set());
    batchMarkIfAbsent = jest
      .fn()
      .mockResolvedValue(new Set());
    insertPaymentEvents = jest
      .fn()
      .mockResolvedValue(undefined);
    dlqSend = jest.fn().mockResolvedValue(undefined);

    consumer = new PaymentEventsConsumer(
      {
        batchGetExisting,
        batchMarkIfAbsent,
      } as unknown as RedisDedupService,

      {
        insertPaymentEvents,
      } as unknown as ClickHouseWriterService,

      {
        send: dlqSend,
      } as unknown as DlqProducerService,
    );
  });

  describe('toPaymentRow', () => {
    it('envelope을 ClickHouse row로 매핑하고 DateTime을 변환한다', () => {
      const row = internals(consumer).toPaymentRow(
        envelope('evt-pay-1'),
      );

      expect(row).toEqual({
        event_id: 'evt-pay-1',
        order_id: 'order-evt-pay-1',
        user_id: 'user-1001',
        amount: 49900,
        currency: 'KRW',
        payment_method: 'card',
        status: 'completed',
        occurred_at: '2026-09-03 00:00:00.000',
        ingested_at: '2026-09-03 00:00:00.000',
      });
    });
  });

  describe('processBatch', () => {
    it('신규 이벤트: Bulk Insert 1회 → 완료 마킹 1회', async () => {
      await internals(consumer).processBatch([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(insertPaymentEvents).toHaveBeenCalledTimes(1);
      expect(batchMarkIfAbsent).toHaveBeenCalledTimes(1);
      expect(dlqSend).not.toHaveBeenCalled();
      expect(batchMarkIfAbsent).toHaveBeenCalledWith(
        'payment',
        ['evt-1', 'evt-2'],
        86400,
      );
    });

    it('batch 내부 동일 eventId 중복을 제거한다', async () => {
      await internals(consumer).processBatch([
        envelope('evt-1'),
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      const rows = insertPaymentEvents.mock.calls[0][0];
      expect(rows).toHaveLength(2);
      expect(batchGetExisting).toHaveBeenCalledWith(
        'payment',
        ['evt-1', 'evt-2'],
      );
    });

    it('Redis DUPLICATE를 제외하고 신규만 처리한다', async () => {
      batchGetExisting.mockResolvedValue(
        new Set(['evt-2']),
      );

      await internals(consumer).processBatch([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      const rows = insertPaymentEvents.mock.calls[0][0];
      expect(
        rows.map(
          (r: Record<string, unknown>) => r.event_id,
        ),
      ).toEqual(['evt-1']);
      expect(batchMarkIfAbsent).toHaveBeenCalledWith(
        'payment',
        ['evt-1'],
        86400,
      );
    });

    it('모두 DUPLICATE면 Insert와 완료 마킹을 수행하지 않는다', async () => {
      batchGetExisting.mockResolvedValue(
        new Set(['evt-1', 'evt-2']),
      );

      await internals(consumer).processBatch([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(insertPaymentEvents).not.toHaveBeenCalled();
      expect(batchMarkIfAbsent).not.toHaveBeenCalled();
    });

    it('Redis Batch Read 실패 시 Fail-Closed로 throw한다', async () => {
      batchGetExisting.mockRejectedValue(
        new Error('redis down'),
      );

      await expect(
        internals(consumer).processBatch([
          envelope('evt-1'),
        ]),
      ).rejects.toThrow('redis down');

      expect(insertPaymentEvents).not.toHaveBeenCalled();
    });

    it('ClickHouse Bulk Insert 실패 시 Retry 후 DLQ 이관, 완료 마킹은 하지 않는다', async () => {
      insertPaymentEvents.mockRejectedValue(
        new Error('clickhouse down'),
      );

      await internals(consumer).processBatch([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      // 최초 1회 + 재시도 maxRetry(3) = 총 4회
      expect(insertPaymentEvents).toHaveBeenCalledTimes(4);
      expect(dlqSend).toHaveBeenCalledTimes(2);
      expect(batchMarkIfAbsent).not.toHaveBeenCalled();
    });

    it('DLQ 최종 실패 시 예외를 전파한다', async () => {
      insertPaymentEvents.mockRejectedValue(
        new Error('clickhouse down'),
      );
      dlqSend.mockRejectedValue(new Error('dlq down'));

      await expect(
        internals(consumer).processBatch([
          envelope('evt-1'),
        ]),
      ).rejects.toThrow('dlq down');
    });

    it('ClickHouse 성공 후 Redis 완료 마킹 실패 시 Fail-Closed로 throw한다', async () => {
      batchMarkIfAbsent.mockRejectedValue(
        new Error('redis down'),
      );

      await expect(
        internals(consumer).processBatch([
          envelope('evt-1'),
        ]),
      ).rejects.toThrow('redis down');

      expect(insertPaymentEvents).toHaveBeenCalledTimes(1);
      expect(dlqSend).not.toHaveBeenCalled();
    });
  });

  describe('sendToDlq', () => {
    it('각 이벤트를 DLQ로 전송한다', async () => {
      await internals(consumer).sendToDlq(
        [envelope('evt-1'), envelope('evt-2')],
        'reason',
      );

      expect(dlqSend).toHaveBeenCalledTimes(2);
    });
  });
});
