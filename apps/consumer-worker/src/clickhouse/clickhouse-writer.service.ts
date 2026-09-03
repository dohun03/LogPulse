import {
  Injectable,
  OnModuleDestroy,
} from '@nestjs/common';

import {
  createClient,
  ClickHouseClient,
} from '@clickhouse/client';

@Injectable()
export class ClickHouseWriterService
  implements OnModuleDestroy
{
  private readonly client: ClickHouseClient =
    createClient({
      url:
        process.env.CLICKHOUSE_URL ??
        'http://localhost:8123',

      database:
        process.env.CLICKHOUSE_DATABASE ?? 'logpulse',

      username:
        process.env.CLICKHOUSE_USERNAME ??
        'logpulse_writer',

      password: process.env.CLICKHOUSE_PASSWORD,
    });

  async onModuleDestroy() {
    await this.client.close();
  }

  async insertClickEvents(
    rows: Record<string, unknown>[],
  ) {
    if (rows.length === 0) {
      return;
    }

    await this.client.insert({
      table: 'click_events',
      values: rows,
      format: 'JSONEachRow',
    });
  }

  async insertPaymentEvents(
    rows: Record<string, unknown>[],
  ) {
    if (rows.length === 0) {
      return;
    }

    await this.client.insert({
      table: 'payment_events',
      values: rows,
      format: 'JSONEachRow',
    });
  }
}
