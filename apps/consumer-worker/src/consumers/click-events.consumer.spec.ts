import { ClickHouseWriterService } from '../clickhouse/clickhouse-writer.service';
import { RedisDedupService } from '../redis/redis-dedup.service';
import { ClickEventsConsumer } from './click-events.consumer';

describe('ClickEventsConsumer', () => {
  let consumer: ClickEventsConsumer;
  let insertClickEvents: jest.Mock;
  let batchGetExisting: jest.Mock;
  let batchMarkIfAbsent: jest.Mock;

  const envelope = (eventId: string) => ({
    eventId,
    ingestedAt: '2026-09-03T00:00:00.000Z',
    payload: {
      userId: 'user-1',
      sessionId: 'sess-1',
      eventType: 'product_click',
      productId: 'prod-1',
      pageUrl: '/products/1',
      occurredAt: '2026-09-03T00:00:00.000Z',
      metadata: {},
    },
  });

  const internals = (c: ClickEventsConsumer) =>
    c as unknown as {
      toClickRow: (
        envelope: unknown,
      ) => Record<string, unknown>;
      flushWithRetry: (
        rows: Record<string, unknown>[],
      ) => Promise<void>;
      dedupeAndClaim: (
        envelopes: unknown[],
      ) => Promise<unknown[]>;
      processClickBatch: (
        envelopes: unknown[],
      ) => Promise<void>;
      failedBatchCount: number;
      failedRowCount: number;
      buffer: { add: jest.Mock };
    };

  beforeEach(() => {
    insertClickEvents = jest
      .fn()
      .mockResolvedValue(undefined);
    batchGetExisting = jest
      .fn()
      .mockResolvedValue(new Set());
    batchMarkIfAbsent = jest
      .fn()
      .mockResolvedValue(new Set());

    consumer = new ClickEventsConsumer(
      {
        batchGetExisting,
        batchMarkIfAbsent,
      } as unknown as RedisDedupService,

      {
        insertClickEvents,
      } as unknown as ClickHouseWriterService,
    );
  });

  describe('toClickRow', () => {
    it('envelope을 ClickHouse row로 매핑한다', () => {
      const row = internals(consumer).toClickRow(envelope('evt-1'));

      expect(row).toEqual({
        event_id: 'evt-1',
        user_id: 'user-1',
        session_id: 'sess-1',
        event_type: 'product_click',
        product_id: 'prod-1',
        page_url: '/products/1',
        occurred_at: '2026-09-03 00:00:00.000',
        ingested_at: '2026-09-03 00:00:00.000',
        metadata: '{}',
      });
    });

    it('productId/metadata가 없으면 null과 {}를 사용한다', () => {
      const env = {
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

      const row = internals(consumer).toClickRow(env);

      expect(row.product_id).toBeNull();
      expect(row.metadata).toBe('{}');
    });
  });

  describe('dedupeAndClaim', () => {
    it('동일 batch 내부 eventId 중복을 제거한다', async () => {
      batchMarkIfAbsent.mockResolvedValue(
        new Set(['evt-1', 'evt-2']),
      );

      const result = await internals(consumer).dedupeAndClaim([
        envelope('evt-1'),
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(
        (result as { eventId: string }[]).map((e) => e.eventId),
      ).toEqual(['evt-1', 'evt-2']);
      expect(batchGetExisting).toHaveBeenCalledWith(
        'click',
        ['evt-1', 'evt-2'],
      );
    });

    it('Redis Batch Read 결과에 따라 DUPLICATE를 필터링한다', async () => {
      batchGetExisting.mockResolvedValue(
        new Set(['evt-2']),
      );
      batchMarkIfAbsent.mockResolvedValue(
        new Set(['evt-1']),
      );

      const result = await internals(consumer).dedupeAndClaim([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(
        (result as { eventId: string }[]).map((e) => e.eventId),
      ).toEqual(['evt-1']);
      expect(batchMarkIfAbsent).toHaveBeenCalledWith(
        'click',
        ['evt-1'],
        600,
      );
    });

    it('모두 DUPLICATE면 빈 배열을 반환하고 선점을 수행하지 않는다', async () => {
      batchGetExisting.mockResolvedValue(
        new Set(['evt-1', 'evt-2']),
      );

      const result = await internals(consumer).dedupeAndClaim([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(result).toEqual([]);
      expect(batchMarkIfAbsent).not.toHaveBeenCalled();
    });

    it('Pipeline SET NX 결과에 따라 최종 선점을 필터링한다', async () => {
      batchMarkIfAbsent.mockResolvedValue(
        new Set(['evt-1']),
      );

      const result = await internals(consumer).dedupeAndClaim([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(
        (result as { eventId: string }[]).map((e) => e.eventId),
      ).toEqual(['evt-1']);
    });

    it('Redis Batch Read 실패 시 fail-open으로 전체를 반환한다', async () => {
      batchGetExisting.mockRejectedValue(
        new Error('down'),
      );

      const result = await internals(consumer).dedupeAndClaim([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(
        (result as { eventId: string }[]).map((e) => e.eventId),
      ).toEqual(['evt-1', 'evt-2']);
      expect(batchMarkIfAbsent).not.toHaveBeenCalled();
    });

    it('Pipeline SET NX 실패 시 fail-open으로 신규 후보 전체를 반환한다', async () => {
      batchMarkIfAbsent.mockRejectedValue(
        new Error('down'),
      );

      const result = await internals(consumer).dedupeAndClaim([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(
        (result as { eventId: string }[]).map((e) => e.eventId),
      ).toEqual(['evt-1', 'evt-2']);
    });
  });

  describe('processClickBatch', () => {
    it('선점 성공한 이벤트만 BatchBuffer에 전달한다', async () => {
      const addSpy = jest.spyOn(
        internals(consumer).buffer,
        'add',
      );
      batchMarkIfAbsent.mockResolvedValue(
        new Set(['evt-1']),
      );
      batchGetExisting.mockResolvedValue(
        new Set(['evt-2']),
      );

      await internals(consumer).processClickBatch([
        envelope('evt-1'),
        envelope('evt-2'),
      ]);

      expect(addSpy).toHaveBeenCalledTimes(1);
      expect(addSpy.mock.calls[0][0]).toMatchObject({
        event_id: 'evt-1',
      });
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
