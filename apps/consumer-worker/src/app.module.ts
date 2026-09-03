import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { LoggerModule } from 'nestjs-pino';

import { RedisModule } from './redis/redis.module';
import { ClickhouseModule } from './clickhouse/clickhouse.module';
import { DlqProducerService } from './dlq/dlq-producer.service';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    LoggerModule.forRoot({
      pinoHttp: {
        level: process.env.LOG_LEVEL ?? 'info',
        mixin: () => ({ service: 'consumer-worker' }),
      },
    }),
    RedisModule,
    ClickhouseModule,
  ],
  providers: [DlqProducerService],
})
export class AppModule {}
