#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka-1:9092}"
KAFKA_BIN="/opt/kafka/bin"

# click-events: 클릭 토픽 (3일 보관)
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic click-events \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

# payment-events: 결제 토픽 (14일 보관)
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic payment-events \
  --partitions 2 \
  --replication-factor 3 \
  --config retention.ms=1209600000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

# payment-events-dlq: 결제 DLQ 토픽 (30일 보관)
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic payment-events-dlq \
  --partitions 2 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

# click retry: 클릭 이벤트 처리 실패 시 임시로 우회 저장하는 큐 (3일 보관)
"$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic click-events-retry \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2

echo "LogPulse Kafka topics created."