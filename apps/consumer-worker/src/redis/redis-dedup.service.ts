import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

export type DedupResult =
  | 'NEW'
  | 'DUPLICATE'
  | 'ERROR';

@Injectable()
export class RedisDedupService implements OnModuleDestroy {
  private readonly logger = new Logger(RedisDedupService.name);

  private readonly redis = new Redis(
    process.env.REDIS_URL ?? 'redis://localhost:6379',
    {
      maxRetriesPerRequest: 1,                               // 재연결 횟수
      retryStrategy: (times) => Math.min(times * 200, 1000), // 연결 재연결 간격(백오프). 최대 1초
    },
  );

  constructor() {
    // ioredis 'error' 이벤트를 반드시 구독한다, 핸들러가 없으면 Node.js 프로세스가 종료될 수 있다.
    this.redis.on('error', (err) => {
      this.logger.warn(`Redis 연결 오류: ${err.message}`);
    });
  }

  async onModuleDestroy() {
    this.redis.disconnect();
  }

  // Redis 저장, 상태 체크, 마킹 로직
  async checkAndMark(topic: string, eventId: string, ttlSec: number): Promise<DedupResult> {
    try {
      const result = await this.redis.set(`dedup:${topic}:${eventId}`, '1', 'EX', ttlSec, 'NX');
      return result === 'OK' ? 'NEW' : 'DUPLICATE';
    } catch (err) {
      this.logger.warn(`Redis dedup 체크 실패(fail-open): ${eventId}`);
      return 'ERROR';
    }
  }

  // Redis 중복 체크 로직
  async isDuplicate(topic: string, eventId: string): Promise<boolean> {
    const result = await this.redis.exists(`dedup:${topic}:${eventId}`);
    return result === 1;
  }

  // Redis 저장 로직
  async markProcessed(topic: string, eventId: string, ttlSec: number): Promise<void> {
    await this.redis.set(`dedup:${topic}:${eventId}`, '1', 'EX', ttlSec, 'NX');
  }

  // Kafka batch 단위 Redis Batch Read(MGET): 이미 존재하는 eventId 집합을 반환한다.
  async batchGetExisting(topic: string, eventIds: string[]): Promise<Set<string>> {
    if (eventIds.length === 0) {
      return new Set();
    }

    const keys = eventIds.map((eventId) => `dedup:${topic}:${eventId}`);
    const values = await this.redis.mget(...keys);

    const existing = new Set<string>();
    values.forEach((value, index) => {
      if (value !== null) {
        existing.add(eventIds[index]);
      }
    });

    return existing;
  }

  // Kafka batch 단위 Redis 선점(Pipeline SET NX): 신규 선점에 성공한 eventId 집합을 반환한다.
  async batchMarkIfAbsent(topic: string, eventIds: string[], ttlSec: number): Promise<Set<string>> {
    if (eventIds.length === 0) {
      return new Set();
    }

    const pipeline = this.redis.pipeline();
    for (const eventId of eventIds) {
      pipeline.set(`dedup:${topic}:${eventId}`, '1', 'EX', ttlSec, 'NX');
    }

    const results = await pipeline.exec();

    const claimed = new Set<string>();
    (results ?? []).forEach(([error, result], index) => {
      if (!error && result === 'OK') {
        claimed.add(eventIds[index]);
      }
    });

    return claimed;
  }
}
