import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from confluent_kafka import Consumer
from cassandra.cluster import Cluster


def now_utc():
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    # e.g., "2026-03-01T12:00:10.123456+00:00"
    return dt.isoformat()


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


def send_report(monitor_url: str, report: Dict[str, Any]) -> None:
    """
    Best-effort reporting to mysimbdp-streamingestmonitor.
    Failures must NOT crash ingestion (observability is non-critical path).
    """
    try:
        r = requests.post(f"{monitor_url.rstrip('/')}/report", json=report, timeout=2.0)
        if r.status_code >= 300:
            print(f"⚠️ monitor rejected report: status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        print(f"⚠️ monitor report failed: {type(e).__name__}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True, choices=["tenantA", "tenantB"])
    parser.add_argument("--kafka", default="broker:9092")
    parser.add_argument("--cassandra", default="cassandra")
    parser.add_argument("--group", default=None)

    # Reporting config (also supports env vars)
    parser.add_argument("--monitor-url", default=os.getenv("MONITOR_URL", ""))
    parser.add_argument("--report-window-sec", type=int, default=int(os.getenv("REPORT_WINDOW_SEC", "10")))

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

    monitor_url = (args.monitor_url or "").strip()
    window_sec = max(1, int(args.report_window_sec))

    # Cassandra connection
    cluster = Cluster([args.cassandra])
    session = cluster.connect()

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
    if monitor_url:
        print(f"==> reporting enabled monitor_url={monitor_url} window_sec={window_sec}")
    else:
        print("==> reporting disabled (no MONITOR_URL / --monitor-url provided)")
    print("Press Ctrl+C to stop.\n")

    # Window metrics
    win_start = time.time()
    win_records = 0
    win_bytes = 0
    win_errors = 0
    win_sum_ingest_ms = 0.0

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is None:
                # even if idle, flush report if window elapsed
                if monitor_url and (now - win_start) >= window_sec:
                    avg_ms = (win_sum_ingest_ms / win_records) if win_records > 0 else 0.0
                    report = {
                        "tenant_id": tenant,
                        "worker_id": group_id,
                        "ts": iso_utc(now_utc()),
                        "window_sec": window_sec,
                        "avg_ingest_ms": avg_ms,
                        "records": win_records,
                        "bytes": win_bytes,
                        "errors": win_errors,
                    }
                    send_report(monitor_url, report)

                    # reset window
                    win_start = now
                    win_records = 0
                    win_bytes = 0
                    win_errors = 0
                    win_sum_ingest_ms = 0.0
                continue

            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                win_errors += 1
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
                win_errors += 1  # parse failure
                # continue; still ingest raw payload

            ingest_ts = now_utc()

            # Insert into Cassandra
            try:
                session.execute(
                    prepared,
                    (pk, ingest_ts, event_ts, event_id, topic, payload_str)
                )
            except Exception as e:
                win_errors += 1
                print(f"❌ Cassandra insert failed: {type(e).__name__}: {e}")
                # do not crash; continue consuming
                continue

            elapsed_ms = (time.time() - t0) * 1000.0

            # update window metrics only for successful ingestion
            win_records += 1
            win_bytes += len(payload_bytes)
            win_sum_ingest_ms += elapsed_ms

            print(f"{tenant} -> wrote {pk_col}={pk} event_ts={event_ts} bytes={len(payload_bytes)} ingest_ms={elapsed_ms:.2f}")

            # Flush report if window elapsed
            now2 = time.time()
            if monitor_url and (now2 - win_start) >= window_sec:
                avg_ms = (win_sum_ingest_ms / win_records) if win_records > 0 else 0.0
                report = {
                    "tenant_id": tenant,
                    "worker_id": group_id,
                    "ts": iso_utc(now_utc()),
                    "window_sec": window_sec,
                    "avg_ingest_ms": avg_ms,
                    "records": win_records,
                    "bytes": win_bytes,
                    "errors": win_errors,
                }
                send_report(monitor_url, report)

                # reset window
                win_start = now2
                win_records = 0
                win_bytes = 0
                win_errors = 0
                win_sum_ingest_ms = 0.0

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