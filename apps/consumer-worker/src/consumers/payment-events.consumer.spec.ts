import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { DlqProducerService } from '../dlq/dlq-producer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';
import { PaymentEventsConsumer } from './payment-events.consumer';

describe('PaymentEventsConsumer', () => {
  let consumer: PaymentEventsConsumer;
  let isDuplicate: jest.Mock;
  let markProcessed: jest.Mock;
  let insertPaymentEvents: jest.Mock;
  let dlqSend: jest.Mock;

  const envelope = () => ({
    eventId: 'evt-pay-1',
    ingestedAt: '2026-09-03T00:00:00.000Z',
    payload: {
      eventId: 'evt-pay-1',
      orderId: 'order-55231',
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
      processMessage: (
        message: { value: Buffer | null; offset: string },
        resolveOffset: (offset: string) => void,
      ) => Promise<void>;
      toPaymentRow: (e: unknown) => Record<string, unknown>;
    };

  const message = () => ({
    value: Buffer.from(JSON.stringify(envelope())),
    offset: '10',
  });

  beforeEach(() => {
    isDuplicate = jest.fn().mockResolvedValue(false);
    markProcessed = jest.fn().mockResolvedValue(undefined);
    insertPaymentEvents = jest
      .fn()
      .mockResolvedValue(undefined);
    dlqSend = jest.fn().mockResolvedValue(undefined);

    consumer = new PaymentEventsConsumer(
      {
        isDuplicate,
        markProcessed,
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
      const row = internals(consumer).toPaymentRow(envelope());

      expect(row).toEqual({
        event_id: 'evt-pay-1',
        order_id: 'order-55231',
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

  describe('processMessage', () => {
    it('신규 이벤트: 저장 → 완료표시 → offset resolve', async () => {
      const resolveOffset = jest.fn();

      await internals(consumer).processMessage(
        message(),
        resolveOffset,
      );

      expect(isDuplicate).toHaveBeenCalledWith(
        'payment',
        'evt-pay-1',
      );
      expect(insertPaymentEvents).toHaveBeenCalledTimes(1);
      expect(markProcessed).toHaveBeenCalledTimes(1);
      expect(resolveOffset).toHaveBeenCalledWith('10');
      expect(dlqSend).not.toHaveBeenCalled();
    });

    it('중복 이벤트: 저장하지 않고 offset만 resolve', async () => {
      isDuplicate.mockResolvedValue(true);
      const resolveOffset = jest.fn();

      await internals(consumer).processMessage(
        message(),
        resolveOffset,
      );

      expect(insertPaymentEvents).not.toHaveBeenCalled();
      expect(markProcessed).not.toHaveBeenCalled();
      expect(resolveOffset).toHaveBeenCalledWith('10');
      expect(dlqSend).not.toHaveBeenCalled();
    });

    it('ClickHouse 저장이 최종 실패하면 DLQ로 이관 후 offset resolve', async () => {
      insertPaymentEvents.mockRejectedValue(
        new Error('clickhouse down'),
      );
      const resolveOffset = jest.fn();

      await internals(consumer).processMessage(
        message(),
        resolveOffset,
      );

      expect(dlqSend).toHaveBeenCalledTimes(1);
      expect(markProcessed).not.toHaveBeenCalled();
      expect(resolveOffset).toHaveBeenCalledWith('10');
    });

    it('Redis isDuplicate가 계속 실패하면(Fail-Closed) throw한다', async () => {
      isDuplicate.mockRejectedValue(new Error('redis down'));
      const resolveOffset = jest.fn();

      await expect(
        internals(consumer).processMessage(
          message(),
          resolveOffset,
        ),
      ).rejects.toThrow('redis down');

      expect(resolveOffset).not.toHaveBeenCalled();
    });
  });
});
