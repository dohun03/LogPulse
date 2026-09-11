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

  // 각 서버 당 RATE_LIMIT_MAX 설정(클라이언트 단일 IP 기준)
  const maxPerIp = Number(process.env.RATE_LIMIT_MAX ?? 100);
  await app.register(fastifyRateLimit, {
    max: maxPerIp,
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
  // 동시 연결 대비 listen backlog 증설 (Node.js 기본 511 → 1024)
  const backlog = Number(process.env.LISTEN_BACKLOG ?? 1024);
  await app.listen({
    port,
    host: '0.0.0.0',
    backlog,
  });
}

bootstrap();
