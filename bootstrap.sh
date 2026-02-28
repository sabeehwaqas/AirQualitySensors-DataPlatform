#!/usr/bin/env bash
# ------------------------------------------------------------
# bootstrap.sh (from-scratch safe)
# - docker compose up -d
# - detect compose network AFTER stack is up
# - wait kafka ready
# - create kafka topics (idempotent)
# - wait cassandra port open + cql ready (via temp containers)
# - create cassandra keyspaces/tables (via temp cqlsh container)
# ------------------------------------------------------------

set -euo pipefail

BROKER_CONTAINER="broker"
CASSANDRA_SERVICE="cassandra"
CASSANDRA_CONTAINER="cassandra"

BOOTSTRAP="localhost:9092"

TOPIC_A="tenantA.bronze.raw"
TOPIC_B="tenantB.bronze.raw"
PARTITIONS=6
REPL=1

echo "==> Starting Kafka + Cassandra (docker compose up -d)..."
docker compose up -d

# ---- detect docker compose network (retry a bit) ----
echo "==> Detecting docker compose network..."
COMPOSE_NETWORK=""
for i in {1..30}; do
  CID="$(docker compose ps -q 2>/dev/null | head -n 1 || true)"
  if [[ -n "${CID}" ]]; then
    COMPOSE_NETWORK="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "${CID}" | head -n 1 || true)"
  fi

  if [[ -n "${COMPOSE_NETWORK}" ]]; then
    break
  fi

  echo "  ...still waiting for network (${i}/30)"
  sleep 1
done

if [[ -z "${COMPOSE_NETWORK}" ]]; then
  echo "❌ Could not detect docker compose network. Debug info:"
  docker compose ps || true
  exit 1
fi

echo "✅ Using compose network: ${COMPOSE_NETWORK}"

# ---- helper: run cqlsh from a temporary container on compose network ----
cql() {
  local stmt="$1"
  docker run --rm --network "${COMPOSE_NETWORK}" cassandra:4.1 \
    cqlsh "${CASSANDRA_SERVICE}" 9042 -e "$stmt"
}

# ----------------------------
# Wait for Kafka
# ----------------------------
echo "==> Waiting for Kafka to be ready on ${BOOTSTRAP} ..."
for i in {1..60}; do
  if docker exec -i "${BROKER_CONTAINER}" bash -lc \
    "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${BOOTSTRAP} >/dev/null 2>&1"; then
    echo "✅ Kafka is ready"
    break
  fi
  echo "  ...still waiting (${i}/60)"
  sleep 2
  if [[ "$i" == "60" ]]; then
    echo "❌ Kafka did not become ready in time. Last logs:"
    docker logs --tail 120 "${BROKER_CONTAINER}" || true
    exit 1
  fi
done

create_topic() {
  local topic="$1"
  if docker exec -i "${BROKER_CONTAINER}" bash -lc \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --list | grep -x '${topic}' >/dev/null 2>&1"; then
    echo "==> Topic already exists: ${topic}"
  else
    echo "==> Creating topic: ${topic} (partitions=${PARTITIONS}, repl=${REPL})"
    docker exec -i "${BROKER_CONTAINER}" bash -lc \
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --create --topic '${topic}' --partitions ${PARTITIONS} --replication-factor ${REPL}"
  fi
}

create_topic "${TOPIC_A}"
create_topic "${TOPIC_B}"

# ----------------------------
# Wait for Cassandra port open
# ----------------------------
echo "==> Waiting for Cassandra port 9042 to open..."
for i in {1..300}; do
  if docker run --rm --network "${COMPOSE_NETWORK}" bash:5.2 \
      bash -lc "cat < /dev/null > /dev/tcp/${CASSANDRA_SERVICE}/9042" >/dev/null 2>&1; then
    echo "✅ Cassandra port is open"
    break
  fi
  echo "  ...still waiting (${i}/300)"
  sleep 2
  if [[ "$i" == "300" ]]; then
    echo "❌ Cassandra port did not open in time. Last logs:"
    docker logs --tail 200 "${CASSANDRA_CONTAINER}" || true
    exit 1
  fi
done

# ----------------------------
# Wait for Cassandra CQL readiness
# ----------------------------
echo "==> Waiting for Cassandra to be CQL-ready (cqlsh works)..."
for i in {1..300}; do
  if cql "SELECT now() FROM system.local;" >/dev/null 2>&1; then
    echo "✅ Cassandra is CQL-ready"
    break
  fi
  echo "  ...still waiting (${i}/300)"
  sleep 2
  if [[ "$i" == "300" ]]; then
    echo "❌ Cassandra did not become CQL-ready in time. Last logs:"
    docker logs --tail 200 "${CASSANDRA_CONTAINER}" || true
    exit 1
  fi
done

# ----------------------------
# Create schema
# ----------------------------
echo "==> Creating keyspaces/tables (idempotent)..."

cql "CREATE KEYSPACE IF NOT EXISTS tenantA_bronze WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS tenantA_bronze.records (
        sensor_id text,
        ingest_ts timestamp,
        event_ts text,
        event_id bigint,
        topic text,
        payload text,
        PRIMARY KEY ((sensor_id), ingest_ts)
      ) WITH CLUSTERING ORDER BY (ingest_ts DESC);"

cql "CREATE KEYSPACE IF NOT EXISTS tenantB_bronze WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS tenantB_bronze.records (
        location_id text,
        ingest_ts timestamp,
        event_ts text,
        event_id bigint,
        topic text,
        payload text,
        PRIMARY KEY ((location_id), ingest_ts)
      ) WITH CLUSTERING ORDER BY (ingest_ts DESC);"

cql "CREATE KEYSPACE IF NOT EXISTS platform_logs WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cql "CREATE TABLE IF NOT EXISTS platform_logs.streaming_metrics (
        tenant_id text,
        worker_id text,
        ts timestamp,
        window_sec int,
        avg_ingest_ms double,
        records bigint,
        bytes bigint,
        errors bigint,
        PRIMARY KEY ((tenant_id), ts)
      ) WITH CLUSTERING ORDER BY (ts DESC);"

echo "==> Keyspaces:"
cql "DESCRIBE KEYSPACES;"

echo ""
echo "✅ Bootstrap complete."
echo "Next:"
echo "  ./run_producers.sh"
echo "______________________ "
echo " To run manual workers:"
echo "  python3 streamingestworker.py --tenant tenantA --cassandra 127.0.0.1"
echo "  python3 streamingestworker.py --tenant tenantB --cassandra 127.0.0.1"