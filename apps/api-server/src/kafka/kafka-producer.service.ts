import { Injectable, OnModuleDestroy, OnModuleInit, ServiceUnavailableException } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { clickProducerConfig, paymentProducerConfig } from './kafka.config';

interface SendArgs<T> {
  key: string;
  value: T;
}

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'logpulse-api-server',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  });

  private readonly clickProducer: Producer = this.kafka.producer({
    allowAutoTopicCreation: false, // 토픽 자동 생성 X
  });

  private readonly paymentProducer: Producer = this.kafka.producer({
    idempotent: paymentProducerConfig.idempotent,                   // 중복 방지
    maxInFlightRequests: paymentProducerConfig.maxInFlightRequests, // 동시 요청 수 제어
    allowAutoTopicCreation: false,                                  // 토픽 자동 생성 X
  });

  private clickConnected = false;
  private paymentConnected = false;

  // NestJS 서버 부팅&종료시 각 프로듀서 연결/해제를 병렬로 수행
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

  // 각 프로듀서 실제 연결 및 상태 값 변경 (onModuleInit에서 참조)
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

  // 실제 클릭 이벤트 전송
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

  // 실제 결제 이벤트 전송
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