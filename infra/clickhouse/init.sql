CREATE DATABASE IF NOT EXISTS logpulse;

USE logpulse;

-- click_events 테이블
CREATE TABLE IF NOT EXISTS click_events
(
    event_id      String,
    user_id       String,
    session_id    String,
    event_type    LowCardinality(String),
    product_id    Nullable(String),
    page_url      String,
    occurred_at   DateTime64(3),
    ingested_at   DateTime64(3) DEFAULT now64(3),
    metadata      String
)
ENGINE = MergeTree                            -- 기본적인 고성능 데이터 저장 엔진
PARTITION BY toYYYYMMDD(occurred_at)          -- 날짜(일) 단위로 디스크 파티션 분리 (조회 성능 증대 및 오래된 데이터 TTL 삭제 효율화)
ORDER BY (session_id, occurred_at)            -- Primary Key 역할 (세션별, 시간순으로 데이터 정렬 디스크 저장)
TTL toDateTime(occurred_at) + INTERVAL 90 DAY -- 90일이 지난 데이터는 자동 삭제
SETTINGS index_granularity = 8192;

-- payment_events 테이블 (중복 제거)
CREATE TABLE IF NOT EXISTS payment_events
(
    event_id       String,
    order_id       String,
    user_id        String,
    amount         Decimal64(2),
    currency       LowCardinality(String),
    payment_method LowCardinality(String),
    status         LowCardinality(String),
    occurred_at    DateTime64(3),
    ingested_at    DateTime64(3) DEFAULT now64(3),
    version        UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree(version) -- Primary Key가 중복될 경우, version 값이 가장 높은(최신) 데이터로 덮어쓰는 엔진
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (order_id, event_id);       -- order_id와 event_id 조합을 기준으로 중복 판단

-- payment_events_hourly_mv 테이블 (실시간 집계 뷰)
CREATE MATERIALIZED VIEW IF NOT EXISTS payment_events_hourly_mv
ENGINE = SummingMergeTree()          -- payment_events 테이블에 데이터가 들어올 때마다 1시간 단위로 지정된 컬럼을 자동으로 합산해 두는 엔진
PARTITION BY toYYYYMMDD(hour_ts)
ORDER BY (hour_ts, status)           -- 시간대별, 결제 상태별로 정렬
AS
SELECT
    toStartOfHour(occurred_at) AS hour_ts,
    status,
    count() AS event_count,
    sum(amount) AS total_amount
FROM payment_events
GROUP BY hour_ts, status;

-- 워커용 계정: 쓰기/읽기 전용
CREATE USER IF NOT EXISTS logpulse_writer IDENTIFIED WITH plaintext_password BY '{{WRITER_PASSWORD}}';
GRANT INSERT, SELECT ON logpulse.* TO logpulse_writer;

-- 대시보드용 계정: 읽기 전용
CREATE USER IF NOT EXISTS logpulse_reader IDENTIFIED WITH plaintext_password BY '{{READER_PASSWORD}}';
GRANT SELECT ON logpulse.* TO logpulse_reader;