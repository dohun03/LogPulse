import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { ValidationPipe } from '@nestjs/common';
import fastifyHelmet from '@fastify/helmet';
import fastifyCors from '@fastify/cors';
import fastifyRateLimit from '@fastify/rate-limit';
import { Logger } from 'nestjs-pino';

import { AppModule } from './app.module';
import { GlobalExceptionFilter } from './common/filters/global-exception.filter';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({
      trustProxy: true,
    }),
    { bufferLogs: true },
  );

  app.useLogger(app.get(Logger));

  await app.register(fastifyHelmet);

  await app.register(fastifyCors, {
    origin: true,
  });

  const aggregateRateLimit =
    Number(process.env.RATE_LIMIT_MAX ?? 5000);

  const apiInstanceCount = Math.max(
    1,
    Number(process.env.API_INSTANCE_COUNT ?? 2),
  );

  await app.register(fastifyRateLimit, {
    max: Math.ceil(aggregateRateLimit / apiInstanceCount),
    timeWindow: '1 minute',
  });

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  app.useGlobalFilters(new GlobalExceptionFilter());

  const port = Number(process.env.PORT ?? 3000);

  await app.listen({
    port,
    host: '0.0.0.0',
  });
}

bootstrap();
