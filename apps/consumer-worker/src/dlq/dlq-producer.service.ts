import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import {
  Kafka,
  Producer,
} from 'kafkajs';

@Injectable()
export class DlqProducerService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(
    DlqProducerService.name,
  );

  private readonly kafka = new Kafka({
    clientId: 'logpulse-dlq-producer',

    brokers: (
      process.env.KAFKA_BROKERS ?? 'localhost:9092'
    ).split(','),
  });

  private readonly producer: Producer =
    this.kafka.producer({
      idempotent: true,
      allowAutoTopicCreation: false,
    });

  async onModuleInit() {
    await this.producer.connect();
    this.logger.log('DLQ producer connected');
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }

  async send(
    originalEnvelope: unknown,
    failureReason: string,
  ) {
    await this.producer.send({
      topic:
        process.env.KAFKA_PAYMENT_DLQ_TOPIC ??
        'payment-events-dlq',

      messages: [
        {
          value: JSON.stringify({
            originalEnvelope,
            failureReason,
            failedAt: new Date().toISOString(),
          }),
        },
      ],
    });
  }
}
