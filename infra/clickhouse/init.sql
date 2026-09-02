CREATE DATABASE IF NOT EXISTS logpulse;

USE logpulse;

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
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (session_id, occurred_at)
TTL toDateTime(occurred_at) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

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
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (order_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS payment_events_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour_ts)
ORDER BY (hour_ts, status)
AS
SELECT
    toStartOfHour(occurred_at) AS hour_ts,
    status,
    count() AS event_count,
    sum(amount) AS total_amount
FROM payment_events
GROUP BY hour_ts, status;

CREATE USER IF NOT EXISTS logpulse_writer
IDENTIFIED WITH plaintext_password BY '{{WRITER_PASSWORD}}';

GRANT INSERT, SELECT
ON logpulse.*
TO logpulse_writer;

CREATE USER IF NOT EXISTS logpulse_reader
IDENTIFIED WITH plaintext_password BY '{{READER_PASSWORD}}';

GRANT SELECT
ON logpulse.*
TO logpulse_reader;