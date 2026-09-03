import {
  Injectable,
  Logger,
  OnModuleDestroy,
} from '@nestjs/common';

import Redis from 'ioredis';

export type DedupResult =
  | 'NEW'
  | 'DUPLICATE'
  | 'ERROR';

@Injectable()
export class RedisDedupService
  implements OnModuleDestroy
{
  private readonly logger = new Logger(
    RedisDedupService.name,
  );

  private readonly redis = new Redis(
    process.env.REDIS_URL ?? 'redis://localhost:6379',
  );

  async onModuleDestroy() {
    this.redis.disconnect();
  }

  async checkAndMark(
    topic: string,
    eventId: string,
    ttlSec: number,
  ): Promise<DedupResult> {
    try {
      const result = await this.redis.set(
        `dedup:${topic}:${eventId}`,
        '1',
        'EX',
        ttlSec,
        'NX',
      );

      return result === 'OK'
        ? 'NEW'
        : 'DUPLICATE';
    } catch (err) {
      this.logger.warn(
        `Redis dedup 체크 실패(fail-open): ${eventId}`,
      );

      return 'ERROR';
    }
  }

  async isDuplicate(
    topic: string,
    eventId: string,
  ): Promise<boolean> {
    const result = await this.redis.exists(
      `dedup:${topic}:${eventId}`,
    );

    return result === 1;
  }

  async markProcessed(
    topic: string,
    eventId: string,
    ttlSec: number,
  ): Promise<void> {
    await this.redis.set(
      `dedup:${topic}:${eventId}`,
      '1',
      'EX',
      ttlSec,
      'NX',
    );
  }
}
