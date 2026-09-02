import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { FastifyRequest } from 'fastify';
import { ErrorCode } from '../constants/error-code.enum';

@Injectable()
export class ApiKeyGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const req = context.switchToHttp().getRequest<FastifyRequest>();
    const apiKey = req.headers['x-api-key'];

    if (apiKey !== process.env.API_KEY) {
      throw new UnauthorizedException({
        errorCode: ErrorCode.UNAUTHORIZED,
        message: 'API Key가 유효하지 않습니다.',
      });
    }

    return true;
  }
}
