export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number,
  baseDelayMs = 200,
): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;

      if (attempt === maxRetries) {
        break;
      }

      const delay = baseDelayMs * 2 ** attempt;

      await new Promise((resolve) =>
        setTimeout(resolve, delay),
      );
    }
  }

  throw lastError;
}
