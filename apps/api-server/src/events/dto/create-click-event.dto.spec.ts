import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';

import { CreateClickEventDto } from './create-click-event.dto';

describe('CreateClickEventDto', () => {
  const valid = {
    eventId: '3fa85f64-5717-4562-b3fc-2c963f66afa6',
    userId: 'user-1001',
    sessionId: 'sess-88af2',
    eventType: 'product_click',
    productId: 'prod-2024',
    pageUrl: '/products/2024',
    occurredAt: '2026-09-02T05:11:59.900Z',
    metadata: { referrer: 'https://google.com', device: 'mobile' },
  };

  it('정상 payload는 통과한다', async () => {
    const dto = plainToInstance(CreateClickEventDto, valid);
    const errors = await validate(dto);
    expect(errors).toHaveLength(0);
  });

  it('sessionId 누락 시 거절된다', async () => {
    const { sessionId, ...rest } = valid;
    const dto = plainToInstance(CreateClickEventDto, rest);
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === 'sessionId')).toBe(true);
  });

  it('잘못된 eventType은 거절된다', async () => {
    const dto = plainToInstance(CreateClickEventDto, {
      ...valid,
      eventType: 'invalid_type',
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === 'eventType')).toBe(true);
  });

  it('UUID가 아닌 eventId는 거절된다', async () => {
    const dto = plainToInstance(CreateClickEventDto, {
      ...valid,
      eventId: 'not-a-uuid',
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === 'eventId')).toBe(true);
  });
});