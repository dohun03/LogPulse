import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  ServiceUnavailableException,
} from '@nestjs/common';

import { Kafka, Producer } from 'kafkajs';

import {
  clickProducerConfig,
  paymentProducerConfig,
} from './kafka.config';

interface SendArgs<T> {
  key: string;
  value: T;
}

@Injectable()
export class KafkaProducerService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'logpulse-api-server',
    brokers: (
      process.env.KAFKA_BROKERS ?? 'localhost:9092'
    ).split(','),
  });

  private readonly clickProducer: Producer =
    this.kafka.producer({
      allowAutoTopicCreation: false,
    });

  private readonly paymentProducer: Producer =
    this.kafka.producer({
      idempotent: paymentProducerConfig.idempotent,
      maxInFlightRequests:
        paymentProducerConfig.maxInFlightRequests,
      allowAutoTopicCreation: false,
    });

  private clickConnected = false;
  private paymentConnected = false;

  async onModuleInit() {
    await Promise.all([
      this.connectClickProducer(),
      this.connectPaymentProducer(),
    ]);
  }

  async onModuleDestroy() {
    await Promise.allSettled([
      this.clickProducer.disconnect(),
      this.paymentProducer.disconnect(),
    ]);

    this.clickConnected = false;
    this.paymentConnected = false;
  }

  private async connectClickProducer() {
    try {
      await this.clickProducer.connect();
      this.clickConnected = true;
    } catch (err) {
      this.clickConnected = false;
      throw err;
    }
  }

  private async connectPaymentProducer() {
    try {
      await this.paymentProducer.connect();
      this.paymentConnected = true;
    } catch (err) {
      this.paymentConnected = false;
      throw err;
    }
  }

  isConnected(): boolean {
    return this.clickConnected && this.paymentConnected;
  }

  async sendClickEvent<T>({ key, value }: SendArgs<T>) {
    try {
      await this.clickProducer.send({
        topic: process.env.KAFKA_CLICK_TOPIC ?? 'click-events',
        acks: clickProducerConfig.acks,
        compression: clickProducerConfig.compression,
        messages: [
          {
            key,
            value: JSON.stringify(value),
          },
        ],
      });
    } catch (err) {
      this.clickConnected = false;
      throw new ServiceUnavailableException({
        errorCode: 'BROKER_UNAVAILABLE',
        message: 'click-events 발행에 실패했습니다.',
      });
    }
  }

  async sendPaymentEvent<T>({ key, value }: SendArgs<T>) {
    try {
      await this.paymentProducer.send({
        topic:
          process.env.KAFKA_PAYMENT_TOPIC ?? 'payment-events',
        acks: paymentProducerConfig.acks,
        compression: paymentProducerConfig.compression,
        messages: [
          {
            key,
            value: JSON.stringify(value),
          },
        ],
      });
    } catch (err) {
      this.paymentConnected = false;
      throw new ServiceUnavailableException({
        errorCode: 'BROKER_UNAVAILABLE',
        message: 'payment-events 발행에 실패했습니다.',
      });
    }
  }
}