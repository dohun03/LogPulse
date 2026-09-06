import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
    { bufferLogs: true },
  );

  app.useLogger(app.get(Logger));
  app.enableShutdownHooks();

  const port = Number(process.env.METRICS_PORT ?? 9100);
  await app.listen({ port, host: '0.0.0.0' });

  const logger = app.get(Logger);
  logger.log(`consumer-worker metrics server listening on :${port}`);
}

bootstrap();

