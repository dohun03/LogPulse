import { ServiceUnavailableException } from '@nestjs/common';

import { KafkaProducerService } from './kafka-producer.service';

const mockSend = jest.fn();
const mockConnect = jest.fn();
const mockDisconnect = jest.fn();

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn().mockImplementation(() => ({
      connect: mockConnect,
      disconnect: mockDisconnect,
      send: mockSend,
    })),
  })),
  CompressionTypes: { LZ4: 1 },
  CompressionCodecs: {},
}));

describe('KafkaProducerService', () => {
  let service: KafkaProducerService;

  beforeEach(() => {
    jest.clearAllMocks();
    process.env.KAFKA_BROKERS = 'localhost:9092';
    process.env.KAFKA_CLICK_TOPIC = 'click-events';
    process.env.KAFKA_PAYMENT_TOPIC = 'payment-events';
    service = new KafkaProducerService();
  });

  it('click 이벤트는 sessionId 키, acks=1로 발행한다', async () => {
    mockSend.mockResolvedValue(undefined);

    await service.sendClickEvent({
      key: 'sess-1',
      value: { eventId: 'evt-1' },
    });

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: 'click-events',
        acks: 1,
        messages: [
          { key: 'sess-1', value: JSON.stringify({ eventId: 'evt-1' }) },
        ],
      }),
    );
  });

  it('payment 이벤트는 orderId 키, acks=-1로 발행한다', async () => {
    mockSend.mockResolvedValue(undefined);

    await service.sendPaymentEvent({
      key: 'order-1',
      value: { eventId: 'evt-2' },
    });

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: 'payment-events',
        acks: -1,
        messages: [
          { key: 'order-1', value: JSON.stringify({ eventId: 'evt-2' }) },
        ],
      }),
    );
  });

  it('click 발행 실패 시 ServiceUnavailableException을 던진다', async () => {
    mockSend.mockRejectedValue(new Error('broker down'));

    await expect(
      service.sendClickEvent({ key: 'sess-1', value: {} }),
    ).rejects.toThrow(ServiceUnavailableException);
  });
});