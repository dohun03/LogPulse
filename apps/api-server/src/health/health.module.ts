import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';

import { HealthController } from './health.controller';
import { KafkaHealthIndicator } from './kafka.health-indicator';
import { KafkaModule } from '../kafka/kafka.module';

@Module({
  imports: [TerminusModule, KafkaModule],
  controllers: [HealthController],
  providers: [KafkaHealthIndicator],
})
export class HealthModule {}