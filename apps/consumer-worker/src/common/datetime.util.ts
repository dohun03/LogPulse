// ClickHouse DateTime64는 'YYYY-MM-DD HH:MM:SS.mmm' 형식을 요구하므로
// ISO 8601(예: 2026-09-03T06:30:00.000Z)을 UTC 기준으로 변환한다.
export function toClickHouseDateTime(iso: string): string {
  return new Date(iso)
    .toISOString()
    .replace('T', ' ')
    .replace('Z', '');
}
