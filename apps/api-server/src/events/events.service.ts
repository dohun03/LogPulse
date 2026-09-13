import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { CreateClickEventDto } from './dto/create-click-event.dto';
import { CreatePaymentEventDto } from './dto/create-payment-event.dto';

@Injectable()
export class EventsService implements OnModuleDestroy {
  // click 배치 버퍼 (linger / batch size 기반으로 묶어 1회 produce)
  private clickBatch: { key: string; value: unknown }[] = [];
  private clickBatchBytes = 0;
  private clickFlushTimer: NodeJS.Timeout | null = null;

  private readonly CLICK_LINGER_MS = 10;          // 배치 지연 시간 (linger.ms)
  private readonly CLICK_BATCH_MAX_BYTES = 32768; // 배치 최대 크기 (batch.size, 32KB)

  constructor(
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  publishClickEvent(dto: CreateClickEventDto) {
    const ingestedAt = new Date().toISOString();
    const message = {
      key: dto.sessionId,
      value: {
        eventId: dto.eventId,
        ingestedAt,
        payload: dto,
      },
    };

    // 배치 버퍼에 추가하고 누적 크기를 추적
    this.clickBatch.push(message);
    this.clickBatchBytes += Buffer.byteLength(JSON.stringify(message.value), 'utf8');

    // batch.size(32KB) 도달 시 즉시 flush, 아니면 linger(10ms) 타이머 예약
    if (this.clickBatchBytes >= this.CLICK_BATCH_MAX_BYTES) {
      void this.flushClickBatch();
    } else if (!this.clickFlushTimer) {
      this.clickFlushTimer = setTimeout(
        () => void this.flushClickBatch(),
        this.CLICK_LINGER_MS,
      );
    }

    // click은 best-effort: Kafka ack를 기다리지 않고 즉시 "수신 성공(202)" 반환
    return {
      success: true,
      eventId: dto.eventId,
      topic: process.env.KAFKA_CLICK_TOPIC ?? 'click-events',
      acceptedAt: ingestedAt,
    };
  }

  async publishPaymentEvent(dto: CreatePaymentEventDto) {
    const ingestedAt = new Date().toISOString();

    // payment는 정합성 중요: Kafka acks=all 확인 후 응답 (기존 유지)
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

  private async flushClickBatch() {
    if (this.clickFlushTimer) {
      clearTimeout(this.clickFlushTimer);
      this.clickFlushTimer = null;
    }
    if (this.clickBatch.length === 0) return;

    const batch = this.clickBatch;
    this.clickBatch = [];
    this.clickBatchBytes = 0;

    await this.kafkaProducer.sendClickBatch(batch);
  }

  onModuleDestroy() {
    if (this.clickFlushTimer) {
      clearTimeout(this.clickFlushTimer);
      this.clickFlushTimer = null;
    }
  }
}