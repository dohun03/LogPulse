import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';

import { CreatePaymentEventDto } from './create-payment-event.dto';

describe('CreatePaymentEventDto', () => {
  const valid = {
    eventId: '9c858901-8a57-4791-81fe-4c455b099bc9',
    orderId: 'order-55231',
    userId: 'user-1001',
    amount: 49900,
    currency: 'KRW',
    paymentMethod: 'card',
    status: 'completed',
    occurredAt: '2026-09-02T05:13:10.500Z',
  };

  it('정상 payload는 통과한다', async () => {
    const dto = plainToInstance(CreatePaymentEventDto, valid);
    const errors = await validate(dto);
    expect(errors).toHaveLength(0);
  });

  it('orderId 누락 시 거절된다', async () => {
    const { orderId, ...rest } = valid;
    const dto = plainToInstance(CreatePaymentEventDto, rest);
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === 'orderId')).toBe(true);
  });

  it('음수 amount는 거절된다', async () => {
    const dto = plainToInstance(CreatePaymentEventDto, {
      ...valid,
      amount: -1,
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === 'amount')).toBe(true);
  });

  it('잘못된 status는 거절된다', async () => {
    const dto = plainToInstance(CreatePaymentEventDto, {
      ...valid,
      status: 'pending',
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === 'status')).toBe(true);
  });
});