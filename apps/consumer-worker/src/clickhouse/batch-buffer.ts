export class BatchBuffer<T> {
  private rows: T[] = [];
  private timer: NodeJS.Timeout | null = null;
  private flushing = false;

  constructor(
    private readonly maxSize: number,
    private readonly flushIntervalMs: number,
    private readonly onFlush: (
      rows: T[],
    ) => Promise<void>,
  ) {}

  add(row: T) {
    this.rows.push(row);

    if (this.rows.length >= this.maxSize) {
      void this.flush();
      return;
    }

    if (!this.timer) {
      this.timer = setTimeout(
        () => void this.flush(),
        this.flushIntervalMs,
      );
    }
  }

  private async flush() {
    if (this.flushing) {
      return;
    }

    this.flushing = true;

    try {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }

      if (this.rows.length === 0) {
        return;
      }

      const batch = this.rows;
      this.rows = [];

      await this.onFlush(batch);
    } finally {
      this.flushing = false;

      if (this.rows.length > 0 && !this.timer) {
        this.timer = setTimeout(
          () => void this.flush(),
          this.flushIntervalMs,
        );
      }
    }
  }
}
