import { UnauthorizedException } from '@nestjs/common';

import { ApiKeyGuard } from './api-key.guard';

describe('ApiKeyGuard', () => {
  const guard = new ApiKeyGuard();
  const KEY = 'test-api-key';

  function makeContext(apiKey?: string) {
    return {
      switchToHttp: () => ({
        getRequest: () => ({ headers: { 'x-api-key': apiKey } }),
      }),
    } as never;
  }

  beforeEach(() => {
    process.env.API_KEY = KEY;
  });

  it('일치하는 API Key는 통과한다', () => {
    expect(guard.canActivate(makeContext(KEY))).toBe(true);
  });

  it('API Key 누락 시 UnauthorizedException', () => {
    expect(() => guard.canActivate(makeContext())).toThrow(
      UnauthorizedException,
    );
  });

  it('API Key 불일치 시 UnauthorizedException', () => {
    expect(() => guard.canActivate(makeContext('wrong-key'))).toThrow(
      UnauthorizedException,
    );
  });
});