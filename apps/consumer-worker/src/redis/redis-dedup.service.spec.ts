import { RedisDedupService } from './redis-dedup.service';

jest.mock('ioredis', () => {
  return jest.fn().mockImplementation(() => ({
    set: jest.fn(),
    exists: jest.fn(),
    disconnect: jest.fn(),
  }));
});

describe('RedisDedupService', () => {
  let service: RedisDedupService;

  const redisMock = () =>
    (service as unknown as {
      redis: {
        set: jest.Mock;
        exists: jest.Mock;
        disconnect: jest.Mock;
      };
    }).redis;

  beforeEach(() => {
    service = new RedisDedupService();
  });

  it('checkAndMark: 신규 이벤트면 NEW를 반환한다', async () => {
    redisMock().set.mockResolvedValue('OK');

    const result = await service.checkAndMark(
      'click',
      'evt-1',
      600,
    );

    expect(result).toBe('NEW');
    expect(redisMock().set).toHaveBeenCalledWith(
      'dedup:click:evt-1',
      '1',
      'EX',
      600,
      'NX',
    );
  });

  it('checkAndMark: 중복 이벤트면 DUPLICATE를 반환한다', async () => {
    redisMock().set.mockResolvedValue(null);

    const result = await service.checkAndMark(
      'click',
      'evt-1',
      600,
    );

    expect(result).toBe('DUPLICATE');
  });

  it('checkAndMark: Redis 오류 시 ERROR를 반환한다 (fail-open)', async () => {
    redisMock().set.mockRejectedValue(new Error('down'));

    const result = await service.checkAndMark(
      'click',
      'evt-1',
      600,
    );

    expect(result).toBe('ERROR');
  });

  it('isDuplicate: 키가 존재하면 true를 반환한다', async () => {
    redisMock().exists.mockResolvedValue(1);

    const result = await service.isDuplicate(
      'payment',
      'evt-2',
    );

    expect(result).toBe(true);
    expect(redisMock().exists).toHaveBeenCalledWith(
      'dedup:payment:evt-2',
    );
  });

  it('isDuplicate: 키가 없으면 false를 반환한다', async () => {
    redisMock().exists.mockResolvedValue(0);

    const result = await service.isDuplicate(
      'payment',
      'evt-2',
    );

    expect(result).toBe(false);
  });
});
