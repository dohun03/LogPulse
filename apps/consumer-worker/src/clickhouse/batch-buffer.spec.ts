import { BatchBuffer } from './batch-buffer';

describe('BatchBuffer', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it('버퍼가 maxSize에 도달하면 즉시 flush한다', () => {
    const onFlush = jest
      .fn()
      .mockResolvedValue(undefined);

    const buffer = new BatchBuffer<number>(
      3,
      1000,
      onFlush,
    );

    buffer.add(1);
    buffer.add(2);
    expect(onFlush).not.toHaveBeenCalled();

    buffer.add(3);

    expect(onFlush).toHaveBeenCalledWith([1, 2, 3]);
  });

  it('flushIntervalMs 경과 시 타이머로 flush한다', async () => {
    jest.useFakeTimers();

    const onFlush = jest
      .fn()
      .mockResolvedValue(undefined);

    const buffer = new BatchBuffer<number>(
      100,
      1000,
      onFlush,
    );

    buffer.add(1);
    expect(onFlush).not.toHaveBeenCalled();

    await jest.advanceTimersByTimeAsync(1000);

    expect(onFlush).toHaveBeenCalledWith([1]);
  });

  it('flush 진행 중에는 동시에 flush하지 않는다', async () => {
    let resolveFlush: () => void = () => {};

    const onFlush = jest.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveFlush = resolve;
        }),
    );

    const buffer = new BatchBuffer<number>(
      2,
      1000,
      onFlush,
    );

    buffer.add(1);
    buffer.add(2); // flush 시작 (onFlush pending)

    buffer.add(3);
    buffer.add(4); // maxSize 도달했지만 flushing 중

    expect(onFlush).toHaveBeenCalledTimes(1);

    resolveFlush();
    await Promise.resolve();
  });
});
