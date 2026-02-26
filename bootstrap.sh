#!/usr/bin/env bash
# ------------------------------------------------------------
# bootstrap.sh
# - Starts Kafka using docker compose
# - Waits until Kafka broker is ready
# - Creates tenant topics (idempotent: won't fail if already exists)
# ------------------------------------------------------------

set -euo pipefail

# Name of the Kafka container (matches container_name in docker-compose.yml)
BROKER_CONTAINER="broker"

# Kafka bootstrap address from the host perspective
BOOTSTRAP="localhost:9092"

# Tenant topics (one per tenant)
TOPIC_A="tenantA.bronze.raw"
TOPIC_B="tenantB.bronze.raw"

# Partitions to allow parallelism later (e.g., Flink parallel consumers)
PARTITIONS=6

# Replication factor for single-node dev (must be 1)
REPL=1

echo "==> Starting Kafka (docker compose up -d)..."
docker compose up -d

echo "==> Waiting for Kafka to be ready on ${BOOTSTRAP} ..."
# We wait until the broker responds to an API call.
# This is more reliable than just waiting a fixed sleep time.
for i in {1..60}; do
  if docker exec -i "${BROKER_CONTAINER}" bash -lc \
    "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${BOOTSTRAP} >/dev/null 2>&1"; then
    echo "✅ Kafka is ready"
    break
  fi

  echo "  ...still waiting (${i}/60)"
  sleep 2

  # If we reach the final attempt, show logs for debugging and fail.
  if [[ "$i" == "60" ]]; then
    echo "❌ Kafka did not become ready in time. Showing last logs:"
    docker logs --tail 120 "${BROKER_CONTAINER}"
    exit 1
  fi
done

# Helper function to create a topic only if it doesn't exist
create_topic() {
  local topic="$1"

  # Check if topic exists
  if docker exec -i "${BROKER_CONTAINER}" bash -lc \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --list | grep -x '${topic}' >/dev/null 2>&1"; then
    echo "==> Topic already exists: ${topic}"
  else
    echo "==> Creating topic: ${topic} (partitions=${PARTITIONS}, repl=${REPL})"
    docker exec -i "${BROKER_CONTAINER}" bash -lc \
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --create --topic '${topic}' --partitions ${PARTITIONS} --replication-factor ${REPL}"
  fi
}

# Create tenant topics
create_topic "${TOPIC_A}"
create_topic "${TOPIC_B}"

# Print topic descriptions (useful to verify partitions)
echo "==> Topic descriptions:"
docker exec -i "${BROKER_CONTAINER}" bash -lc \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --describe --topic ${TOPIC_A}"
docker exec -i "${BROKER_CONTAINER}" bash -lc \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --describe --topic ${TOPIC_B}"

echo "✅ Bootstrap complete."
