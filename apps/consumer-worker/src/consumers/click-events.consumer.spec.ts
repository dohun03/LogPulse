import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';
import { ClickEventsConsumer } from './click-events.consumer';

describe('ClickEventsConsumer', () => {
  let consumer: ClickEventsConsumer;
  let insertClickEvents: jest.Mock;

  const internals = (c: ClickEventsConsumer) =>
    c as unknown as {
      toClickRow: (
        envelope: unknown,
      ) => Record<string, unknown>;
      flushWithRetry: (
        rows: Record<string, unknown>[],
      ) => Promise<void>;
      failedBatchCount: number;
      failedRowCount: number;
    };

  beforeEach(() => {
    insertClickEvents = jest
      .fn()
      .mockResolvedValue(undefined);

    consumer = new ClickEventsConsumer(
      {
        checkAndMark: jest.fn(),
      } as unknown as RedisDedupService,

      {
        insertClickEvents,
      } as unknown as ClickHouseWriterService,
    );
  });

  describe('toClickRow', () => {
    it('envelope을 ClickHouse row로 매핑한다', () => {
      const envelope = {
        eventId: 'evt-1',
        ingestedAt: '2026-09-03T00:00:00.000Z',
        payload: {
          userId: 'user-1',
          sessionId: 'sess-1',
          eventType: 'product_click',
          productId: 'prod-1',
          pageUrl: '/products/1',
          occurredAt: '2026-09-03T00:00:00.000Z',
          metadata: { referrer: 'https://x.com' },
        },
      };

      const row = internals(consumer).toClickRow(envelope);

      expect(row).toEqual({
        event_id: 'evt-1',
        user_id: 'user-1',
        session_id: 'sess-1',
        event_type: 'product_click',
        product_id: 'prod-1',
        page_url: '/products/1',
        occurred_at: '2026-09-03 00:00:00.000',
        ingested_at: '2026-09-03 00:00:00.000',
        metadata: '{"referrer":"https://x.com"}',
      });
    });

    it('productId/metadata가 없으면 null과 {}를 사용한다', () => {
      const envelope = {
        eventId: 'evt-2',
        ingestedAt: '2026-09-03T00:00:00.000Z',
        payload: {
          userId: 'user-1',
          sessionId: 'sess-1',
          eventType: 'page_view',
          pageUrl: '/',
          occurredAt: '2026-09-03T00:00:00.000Z',
        },
      };

      const row = internals(consumer).toClickRow(envelope);

      expect(row.product_id).toBeNull();
      expect(row.metadata).toBe('{}');
    });
  });

  describe('flushWithRetry', () => {
    it('첫 시도에 성공하면 1회만 호출한다', async () => {
      await internals(consumer).flushWithRetry([
        { event_id: 'evt-1' },
      ]);

      expect(insertClickEvents).toHaveBeenCalledTimes(1);
      expect(internals(consumer).failedBatchCount).toBe(0);
    });

    it('재시도 후 성공하면 실패 횟수만큼 재호출한다', async () => {
      insertClickEvents
        .mockRejectedValueOnce(new Error('fail-1'))
        .mockResolvedValue(undefined);

      await internals(consumer).flushWithRetry([
        { event_id: 'evt-1' },
      ]);

      expect(insertClickEvents).toHaveBeenCalledTimes(2);
      expect(internals(consumer).failedBatchCount).toBe(0);
    });

    it('재시도 2회 후에도 실패하면 batch를 폐기하고 실패 건수를 증가시킨다', async () => {
      insertClickEvents.mockRejectedValue(
        new Error('always-fail'),
      );

      await internals(consumer).flushWithRetry([
        { event_id: 'evt-1' },
        { event_id: 'evt-2' },
        { event_id: 'evt-3' },
      ]);

      // 최초 1회 + 즉시 재시도 2회 = 총 3회 호출
      expect(insertClickEvents).toHaveBeenCalledTimes(3);
      expect(internals(consumer).failedBatchCount).toBe(1);
      expect(internals(consumer).failedRowCount).toBe(3);
    });
  });
});
