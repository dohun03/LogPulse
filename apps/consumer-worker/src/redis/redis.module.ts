import { Module } from '@nestjs/common';

import { RedisDedupService } from './redis-dedup.service';

@Module({
  providers: [RedisDedupService],
  exports: [RedisDedupService],
})
export class RedisModule {}
