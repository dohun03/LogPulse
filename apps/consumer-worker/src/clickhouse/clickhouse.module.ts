import { Module } from '@nestjs/common';

import { ClickHouseWriterService } from './clickhouse-writer.service';

@Module({
  providers: [ClickHouseWriterService],
  exports: [ClickHouseWriterService],
})
export class ClickhouseModule {}
