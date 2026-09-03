import { retryWithBackoff } from './retry.util';

describe('retryWithBackoff', () => {
  it('첫 시도에 성공하면 즉시 반환한다', async () => {
    const fn = jest.fn().mockResolvedValue('ok');

    const result = await retryWithBackoff(fn, 3);

    expect(result).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('재시도 후 성공하면 결과를 반환한다', async () => {
    const fn = jest
      .fn()
      .mockRejectedValueOnce(new Error('fail-1'))
      .mockRejectedValueOnce(new Error('fail-2'))
      .mockResolvedValue('recovered');

    const result = await retryWithBackoff(fn, 3, 1);

    expect(result).toBe('recovered');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('maxRetries 초과 시 마지막 에러를 던진다', async () => {
    const error = new Error('always-fails');
    const fn = jest.fn().mockRejectedValue(error);

    await expect(
      retryWithBackoff(fn, 2, 1),
    ).rejects.toThrow('always-fails');

    // 1회(최초) + 2회(재시도) = 총 3회 호출
    expect(fn).toHaveBeenCalledTimes(3);
  });
});
