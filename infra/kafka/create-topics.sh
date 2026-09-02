#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka-1:9092}"
KAFKA_BIN="/opt/kafka/bin"

# click-events
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

# payment-events
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

# payment DLQ
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

# click retry
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