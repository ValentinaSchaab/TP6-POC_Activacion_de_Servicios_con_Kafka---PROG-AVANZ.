#!/bin/bash
# Crea los 4 topics de negocio + sus DLQ: 3 particiones, retención de 7 días.
set -e

BOOTSTRAP="kafka:19092"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
RETENTION_MS=604800000 # 7 días

TOPICS=(
  "activation.requested"
  "billing.events"
  "provisioning.events"
  "activation.events"
)

create_topic() {
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$1" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS"
}

for topic in "${TOPICS[@]}"; do
  create_topic "$topic"
  create_topic "$topic.dlq"
done

echo "Topics creados:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
