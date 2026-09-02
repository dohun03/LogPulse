export function buildDedupKey(
  topic: 'click' | 'payment',
  eventId: string,
): string {
  return `dedup:${topic}:${eventId}`;
}
