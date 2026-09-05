import { Injectable } from '@nestjs/common';
import { HealthIndicator, HealthIndicatorResult, HealthCheckError } from '@nestjs/terminus';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Injectable()
export class KafkaHealthIndicator extends HealthIndicator {
  constructor(
    private readonly kafkaProducer: KafkaProducerService,
  ) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const isConnected = this.kafkaProducer.isConnected(); // Kafka 프로듀서의 isConnected()로 연결 여부 체크
    const result = this.getStatus(key, isConnected);

    if (isConnected) {
      return result;
    }

    throw new HealthCheckError('Kafka connection failed', result);
  }
}