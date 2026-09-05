import { Controller, Get, HttpCode } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { KafkaHealthIndicator } from './kafka.health-indicator';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly kafkaIndicator: KafkaHealthIndicator,
  ) {}

  // 단순 API 서버 생존 여부 체크용, 기능X
  @Get('liveness')
  @HttpCode(200)
  liveness() {
    return { status: 'ok' };
  }

  // Kafka 연결 상태 체크용
  @Get('readiness')
  @HealthCheck()
  readiness() {
    return this.health.check([
      () => this.kafkaIndicator.isHealthy('kafka'),
    ]);
  }
}