import { Controller, Get, HttpCode } from '@nestjs/common';

import {
  HealthCheck,
  HealthCheckService,
} from '@nestjs/terminus';

import { KafkaHealthIndicator } from './kafka.health-indicator';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly kafkaIndicator: KafkaHealthIndicator,
  ) {}

  @Get('liveness')
  @HttpCode(200)
  liveness() {
    return { status: 'ok' };
  }

  @Get('readiness')
  @HealthCheck()
  readiness() {
    return this.health.check([
      () => this.kafkaIndicator.isHealthy('kafka'),
    ]);
  }
}