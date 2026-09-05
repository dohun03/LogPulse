import { RedisDedupService } from './redis-dedup.service';

jest.mock('ioredis', () => {
  return jest.fn().mockImplementation(() => ({
    set: jest.fn(),
    exists: jest.fn(),
    mget: jest.fn(),
    pipeline: jest.fn(),
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
        mget: jest.Mock;
        pipeline: jest.Mock;
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

  it('batchGetExisting: 존재하는 eventId만 집합으로 반환한다', async () => {
    redisMock().mget.mockResolvedValue(['1', null, '1']);

    const result = await service.batchGetExisting(
      'click',
      ['evt-1', 'evt-2', 'evt-3'],
    );

    expect(result).toEqual(new Set(['evt-1', 'evt-3']));
    expect(redisMock().mget).toHaveBeenCalledWith(
      'dedup:click:evt-1',
      'dedup:click:evt-2',
      'dedup:click:evt-3',
    );
  });

  it('batchGetExisting: 빈 입력이면 빈 집합을 반환한다', async () => {
    const result = await service.batchGetExisting('click', []);

    expect(result).toEqual(new Set());
    expect(redisMock().mget).not.toHaveBeenCalled();
  });

  it('batchMarkIfAbsent: SET NX 성공(OK)인 key만 집합으로 반환한다', async () => {
    const set = jest.fn();
    const exec = jest.fn().mockResolvedValue([
      [null, 'OK'],
      [null, null],
      [null, 'OK'],
    ]);
    redisMock().pipeline.mockReturnValue({ set, exec });

    const result = await service.batchMarkIfAbsent(
      'click',
      ['evt-1', 'evt-2', 'evt-3'],
      600,
    );

    expect(result).toEqual(new Set(['evt-1', 'evt-3']));
    expect(set).toHaveBeenCalledTimes(3);
    expect(set).toHaveBeenNthCalledWith(
      1,
      'dedup:click:evt-1',
      '1',
      'EX',
      600,
      'NX',
    );
  });

  it('batchMarkIfAbsent: 빈 입력이면 빈 집합을 반환한다', async () => {
    const result = await service.batchMarkIfAbsent('click', [], 600);

    expect(result).toEqual(new Set());
    expect(redisMock().pipeline).not.toHaveBeenCalled();
  });
});
