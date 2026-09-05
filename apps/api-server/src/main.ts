import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { ValidationPipe } from '@nestjs/common';
import fastifyHelmet from '@fastify/helmet';
import fastifyCors from '@fastify/cors';
import fastifyRateLimit from '@fastify/rate-limit';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { GlobalExceptionFilter } from './common/filters/global-exception.filter';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>( // Fastify 엔진 사용
    AppModule,
    new FastifyAdapter({ trustProxy: true }), // Nginx가 보내는 IP를 인식
    { bufferLogs: true },
  );

  app.useLogger(app.get(Logger));                    // 부팅 할때 로그 출력
  await app.register(fastifyHelmet);                 // HTTP 응답 헤더에 보안 관련 헤더 자동 설정
  await app.register(fastifyCors, { origin: true }); // CORS 허용

  // 각 서버 당 TPS 2500 설정 x2
  const aggregateRateLimit = Number(process.env.RATE_LIMIT_MAX ?? 5000);             // TPS: 5000 설정
  const apiInstanceCount = Math.max(1, Number(process.env.API_INSTANCE_COUNT ?? 2)); // API 서버 인스턴스 개수: 2
  await app.register(fastifyRateLimit, {
    max: Math.ceil(aggregateRateLimit / apiInstanceCount),
    timeWindow: '1 second',
  });

  // 컨트롤러 자동 DTO 검증
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  // 글로벌 필터
  app.useGlobalFilters(new GlobalExceptionFilter());

  const port = Number(process.env.PORT ?? 3000);
  await app.listen({
    port,
    host: '0.0.0.0',
  });
}

bootstrap();
