// load-test/k6/lib/config.js
// K6 실행 시 공통으로 사용하는 환경설정.
// 값은 실행 시 환경변수(K6__ENV 또는 -e)로 오버라이드할 수 있다.

// BASE_URL: k6 컨테이너가 docker-compose 네트워크 내에서 Nginx를 바라보도록 기본값을 'http://nginx'로 둔다.
// - 호스트에서 직접 실행할 때만 http://localhost 를 넘겨 사용한다.
export const BASE_URL = __ENV.BASE_URL || 'http://nginx';

// API_KEY: Nginx -> API로 전달되는 인증 헤더값.
// infra/docker-compose.yml 의 API_KEY 값(dev-api-key)과 일치해야 한다.
export const API_KEY = __ENV.API_KEY || 'dev-api-key';

// 공통 요청 헤더
export const COMMON_HEADERS = {
  'Content-Type': 'application/json',
  'x-api-key': API_KEY,
};