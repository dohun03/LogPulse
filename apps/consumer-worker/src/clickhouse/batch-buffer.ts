export class BatchBuffer<T> {
  private rows: T[] = [];                      // 임시로 데이터를 쌓아둘 메모리 배열
  private timer: NodeJS.Timeout | null = null; // 지정된 시간 뒤에 강제로 방출하기 위한 타이머 변수
  private flushing = false;                    // 현재 DB에 저장 중인지 상태를 나타내는 플래그 (중복 실행 방지)

  constructor(
    private readonly maxSize: number,          // 최대 몇 개까지 모을 것인가?
    private readonly flushIntervalMs: number,  // 최대 몇 ms까지 대기할 것인가?
    private readonly onFlush: (rows: T[]) => Promise<void>, // 조건이 충족되면 데이터를 받아 처리할 콜백 함수
  ) {}

  // 데이터 한 건을 버퍼에 추가하는 함수
  add(row: T) {
    this.rows.push(row);

    // 조건 1: 쌓인 데이터 개수가 maxSize에 도달하면 즉시 방출
    if (this.rows.length >= this.maxSize) {
      void this.flush();
      return;
    }

    // 조건 2: 개수는 안 채워졌지만, 데이터가 처음 들어왔다면 타이머 가동. 시간(flushIntervalMs)이 지나면 방출
    if (!this.timer) {
      this.timer = setTimeout(
        () => void this.flush(),
        this.flushIntervalMs,
      );
    }
  }

  // 버퍼 방출 실행
  private async flush() {
    if (this.flushing) {
      return;
    }

    this.flushing = true;

    try {
      // 기존 타이머 초기화
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
