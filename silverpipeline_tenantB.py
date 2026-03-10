#!/usr/bin/env python3
"""
silverpipeline_tenantB.py
mysimbdp Silver Pipeline - TenantB

Contract with batchmanager (blackbox model):
  - Called as: python silverpipeline_tenantB.py --run-id <id> [--max-records N]
  - Exit 0  = success
  - Exit 1  = failure
  - Writes JSON log to stdout (captured by batchmanager)
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from cassandra.cluster import Cluster


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log_event(stage: str, status: str, detail: dict = None):
    print(json.dumps({
        "ts": now_iso(),
        "pipeline": "silverpipeline_tenantB",
        "stage": stage,
        "status": status,
        **(detail or {}),
    }), flush=True)


# ── transformations ──────────────────────────────────────────────────────────

def safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def has_pm_data(measurements: dict) -> bool:
    return bool(measurements.get("P1") or measurements.get("P2"))

def transform_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bronze → Silver transformation for TenantB.
    - Flatten nested geo/meta/measurements
    - Cast coordinates to float
    - Add has_pm_data derived field
    """
    measurements = raw.get("measurements") or {}

    return {
        "location_id":  raw.get("location_id", "unknown"),
        "event_ts":     raw.get("event_ts"),
        "event_id":     raw.get("event_id", 0),
        "ingest_ts":    raw.get("ingest_ts"),
        "lat":          safe_float(raw.get("lat")),
        "lon":          safe_float(raw.get("lon")),
        "alt":          safe_float(raw.get("alt")),
        "country":      raw.get("country"),
        "sensor_id":    raw.get("sensor_id"),
        "pm10":         safe_float(measurements.get("P1")),
        "pm2_5":        safe_float(measurements.get("P2")),
        "has_pm_data":  has_pm_data(measurements),
        "silver_ts":    now_iso(),
    }


# ── extract ──────────────────────────────────────────────────────────────────

def get_watermark(session) -> Optional[str]:
    try:
        rows = session.execute(
            "SELECT last_processed_ts FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantB'"
        )
        row = rows.one()
        return str(row.last_processed_ts) if row else None
    except Exception:
        return None

def set_watermark(session, ts: str):
    session.execute(
        "INSERT INTO platform_logs.silver_watermarks (tenant_id, last_processed_ts) VALUES ('tenantB', %s)",
        (ts,)
    )

def extract_to_cache(
    session,
    pending_dir: Path,
    run_id: str,
    max_records: int,
    watermark_ts: Optional[str],
) -> List[Path]:
    pending_dir.mkdir(parents=True, exist_ok=True)

    if watermark_ts:
        rows = session.execute(
            """SELECT location_id, ingest_ts, event_ts, event_id, payload
               FROM tenantB_bronze.records
               WHERE ingest_ts > %s
               LIMIT %s
               ALLOW FILTERING""",
            (watermark_ts, max_records),
        )
    else:
        rows = session.execute(
            f"SELECT location_id, ingest_ts, event_ts, event_id, payload FROM tenantB_bronze.records LIMIT {max_records}"
        )

    records = list(rows)
    if not records:
        return []

    cache_file = pending_dir / f"tenantB_{run_id}.jsonl"
    with open(cache_file, "w") as f:
        for row in records:
            try:
                payload_obj = json.loads(row.payload)
            except Exception:
                payload_obj = {}

            geo  = payload_obj.get("geo") or {}
            meta = payload_obj.get("meta") or {}
            dev  = payload_obj.get("device") or {}

            record = {
                "location_id": row.location_id,
                "ingest_ts":   str(row.ingest_ts),
                "event_ts":    row.event_ts,
                "event_id":    row.event_id,
                "lat":         geo.get("lat"),
                "lon":         geo.get("lon"),
                "alt":         geo.get("alt"),
                "country":     geo.get("country"),
                "sensor_id":   dev.get("sensor_id"),
                "measurements": payload_obj.get("measurements") or {},
            }
            f.write(json.dumps(record) + "\n")

    return [cache_file]


# ── transform + load ─────────────────────────────────────────────────────────

def transform_and_load(session, cache_files: List[Path], processed_dir: Path) -> dict:
    processed_dir.mkdir(parents=True, exist_ok=True)

    prepared = session.prepare(
        """INSERT INTO tenantB_silver.geo_measurements
           (location_id, silver_ts, event_ts, event_id, ingest_ts,
            lat, lon, alt, country, sensor_id, pm10, pm2_5, has_pm_data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    total_records = 0
    total_errors = 0
    max_ingest_ts = None

    for cache_file in cache_files:
        with open(cache_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                    silver = transform_record(raw)

                    session.execute(prepared, (
                        silver["location_id"],
                        silver["silver_ts"],
                        silver["event_ts"],
                        int(silver["event_id"] or 0),
                        silver["ingest_ts"],
                        silver["lat"],
                        silver["lon"],
                        silver["alt"],
                        silver["country"],
                        str(silver["sensor_id"]) if silver["sensor_id"] else None,
                        silver["pm10"],
                        silver["pm2_5"],
                        silver["has_pm_data"],
                    ))
                    total_records += 1

                    its = raw.get("ingest_ts")
                    if its and (max_ingest_ts is None or its > max_ingest_ts):
                        max_ingest_ts = its

                except Exception as e:
                    total_errors += 1
                    log_event("transform_load", "record_error", {"error": str(e), "line": line[:120]})

        dest = processed_dir / cache_file.name
        cache_file.rename(dest)

    return {
        "records_loaded": total_records,
        "errors": total_errors,
        "max_ingest_ts": max_ingest_ts,
    }


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=str(uuid.uuid4())[:8])
    parser.add_argument("--max-records", type=int,
                        default=int(os.getenv("MAX_RECORDS", "3000")))
    parser.add_argument("--cassandra", default=os.getenv("CASSANDRA_HOST", "cassandra"))
    parser.add_argument("--pending-dir",
                        default=os.getenv("CACHE_PENDING_DIR", "/app/cache/tenantB/pending"))
    parser.add_argument("--processed-dir",
                        default=os.getenv("CACHE_PROCESSED_DIR", "/app/cache/tenantB/processed"))
    args = parser.parse_args()

    run_id = args.run_id
    pending_dir = Path(args.pending_dir)
    processed_dir = Path(args.processed_dir)

    t_start = time.time()
    log_event("start", "ok", {"run_id": run_id, "max_records": args.max_records})

    try:
        cluster = Cluster([args.cassandra])
        session = cluster.connect()
        log_event("cassandra_connect", "ok")
    except Exception as e:
        log_event("cassandra_connect", "error", {"error": str(e)})
        sys.exit(1)

    try:
        watermark = get_watermark(session)
        log_event("extract", "start", {"watermark": watermark})
        cache_files = extract_to_cache(session, pending_dir, run_id, args.max_records, watermark)
        log_event("extract", "ok", {"files_written": len(cache_files),
                                     "paths": [str(p) for p in cache_files]})
    except Exception as e:
        log_event("extract", "error", {"error": str(e)})
        cluster.shutdown()
        sys.exit(1)

    if not cache_files:
        log_event("extract", "no_new_data", {"watermark": watermark})
        cluster.shutdown()
        sys.exit(0)

    try:
        log_event("transform_load", "start", {"files": len(cache_files)})
        result = transform_and_load(session, cache_files, processed_dir)
        log_event("transform_load", "ok", result)

        if result["max_ingest_ts"]:
            set_watermark(session, result["max_ingest_ts"])
            log_event("watermark", "updated", {"new_watermark": result["max_ingest_ts"]})

    except Exception as e:
        log_event("transform_load", "error", {"error": str(e)})
        cluster.shutdown()
        sys.exit(1)

    elapsed = round(time.time() - t_start, 3)
    log_event("complete", "ok", {
        "run_id": run_id,
        "elapsed_sec": elapsed,
        **result,
    })

    cluster.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    main()