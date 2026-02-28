import argparse
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from confluent_kafka import Consumer
from cassandra.cluster import Cluster


def now_utc():
    return datetime.now(timezone.utc)


def parse_tenantA(payload_str: str) -> Dict[str, Any]:
    """
    TenantA message schema (flat):
    - sensor_id at top-level
    - timestamp at top-level
    - event_id at top-level
    """
    out = {"pk": "unknown", "event_ts": None, "event_id": 0}
    obj = json.loads(payload_str)
    out["pk"] = str(obj.get("sensor_id") or "unknown")
    out["event_ts"] = obj.get("timestamp")
    out["event_id"] = int(obj.get("event_id") or 0)
    return out


def parse_tenantB(payload_str: str) -> Dict[str, Any]:
    """
    TenantB message schema (nested):
    - geo.location_id
    - meta.timestamp_str
    - meta.event_id
    """
    out = {"pk": "unknown", "event_ts": None, "event_id": 0}
    obj = json.loads(payload_str)
    geo = obj.get("geo") or {}
    meta = obj.get("meta") or {}
    out["pk"] = str(geo.get("location_id") or "unknown")
    out["event_ts"] = meta.get("timestamp_str")
    out["event_id"] = int(meta.get("event_id") or 0)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True, choices=["tenantA", "tenantB"])
    parser.add_argument("--kafka", default="localhost:9092")
    parser.add_argument("--cassandra", default="127.0.0.1")
    parser.add_argument("--group", default=None)
    args = parser.parse_args()

    tenant = args.tenant

    # Tenant-specific routing
    if tenant == "tenantA":
        topic = "tenantA.bronze.raw"
        keyspace = "tenantA_bronze"
        table = "records"
        pk_col = "sensor_id"
        parse_fn = parse_tenantA
    else:
        topic = "tenantB.bronze.raw"
        keyspace = "tenantB_bronze"
        table = "records"
        pk_col = "location_id"
        parse_fn = parse_tenantB

    group_id = args.group or f"{tenant}-streamingestworker"

    # Cassandra connection
    cluster = Cluster([args.cassandra])
    session = cluster.connect()

    # Use a prepared statement (the Cassandra driver expects this when using ? placeholders)
    insert_cql = f"""
    INSERT INTO {keyspace}.{table}
    ({pk_col}, ingest_ts, event_ts, event_id, topic, payload)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_cql)

    # Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": args.kafka,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])

    print(f"==> streamingestworker started tenant={tenant} topic={topic} kafka={args.kafka} cassandra={args.cassandra}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                continue

            t0 = time.time()

            payload_bytes = msg.value()
            payload_str = payload_bytes.decode("utf-8", errors="replace")

            pk = "unknown"
            event_ts: Optional[str] = None
            event_id = 0

            # Best-effort parsing (still store raw even if parsing fails)
            try:
                parsed = parse_fn(payload_str)
                pk = parsed["pk"]
                event_ts = parsed["event_ts"]
                event_id = parsed["event_id"]
            except Exception:
                pass

            ingest_ts = now_utc()

            # Insert into Cassandra
            session.execute(
                prepared,
                (pk, ingest_ts, event_ts, event_id, topic, payload_str)
            )

            elapsed_ms = (time.time() - t0) * 1000.0
            print(f"{tenant} -> wrote {pk_col}={pk} event_ts={event_ts} bytes={len(payload_bytes)} ingest_ms={elapsed_ms:.2f}")

    except KeyboardInterrupt:
        print("\n==> Stopping worker...")

    finally:
        try:
            consumer.close()
        finally:
            cluster.shutdown()
        print("✅ Worker stopped.")


if __name__ == "__main__":
    main()