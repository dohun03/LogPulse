import { Injectable } from '@nestjs/common';

import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { CreateClickEventDto } from './dto/create-click-event.dto';
import { CreatePaymentEventDto } from './dto/create-payment-event.dto';

@Injectable()
export class EventsService {
  constructor(
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async publishClickEvent(dto: CreateClickEventDto) {
    const ingestedAt = new Date().toISOString();

    await this.kafkaProducer.sendClickEvent({
      key: dto.sessionId,
      value: {
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      },
    });

    return {
      success: true,
      eventId: dto.eventId,
      topic: process.env.KAFKA_CLICK_TOPIC ?? 'click-events',
      acceptedAt: ingestedAt,
    };
  }

  async publishPaymentEvent(dto: CreatePaymentEventDto) {
    const ingestedAt = new Date().toISOString();

    await this.kafkaProducer.sendPaymentEvent({
      key: dto.orderId,
      value: {
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      },
    });

    return {
      success: true,
      eventId: dto.eventId,
      topic: process.env.KAFKA_PAYMENT_TOPIC ?? 'payment-events',
      acceptedAt: ingestedAt,
    };
  }
}