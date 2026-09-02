# Cline Rules — LogPulse

- `DEVELOPMENT_SPEC.md`를 Source of Truth로 사용한다.
- `plan.md`의 현재 단계만 작업하고, 검증 전 다음 단계로 넘어가지 않는다.
- 기존 코드를 먼저 확인하고 최소 범위로 수정한다.
- API/이벤트 스키마/Kafka 토픽·파티션·키/Consumer Group ID를 임의 변경하지 않는다.
- 각 단계 완료 후 반드시 build/typecheck/test 및 필요한 실제 실행 검증을 수행한다.
- 검증 통과 후에만 `plan.md` 체크박스를 완료한다. 실패하면 다음 단계로 넘어가지 않는다.
- Consumer가 Partition을 코드로 하드코딩하지 않고 Kafka Consumer Group assignment를 사용한다.
- API는 Stateless이며 ClickHouse/Redis에 직접 접근하지 않는다.
- Click key=`sessionId`, Payment key=`orderId`를 유지한다.
- 민감정보/결제정보를 로그에 원문으로 남기지 않는다.
- 불필요한 추상화·패키지·파일을 추가하지 않는다.

구현 전에 반드시 확인:
- API 2대에서 Rate Limit이 인스턴스별 제한이 되는 문제
- Payment Redis dedup 선기록 후 ClickHouse 실패 시 유실 가능성
- Click BatchBuffer의 동시 flush/flush 실패 처리
- Click/Payment Consumer를 역할별 인스턴스로 실행하는 방법

문제 발견 시 숨기거나 우회하지 말고 원인을 수정한 뒤 재검증한다.
