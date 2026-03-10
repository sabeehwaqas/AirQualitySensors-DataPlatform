#!/usr/bin/env python3
"""
silverpipeline_tenantB.py  —  mysimbdp Silver Pipeline (TenantB)

Blackbox contract with batchmanager:
  - Invoked: python silverpipeline_tenantB.py --run-id <id> [options]
  - Exit 0 = success  |  Exit 1 = failure
  - Every status update is a single JSON line printed to stdout.
  - The "complete" line carries all metrics batchmanager persists to Cassandra:
    run_id, elapsed_sec, extract_sec, transform_sec, data_size_bytes,
    cache_mode, records_loaded, errors

TenantB specifics: nested geo/meta/device schema, location_id PK, has_pm_data flag.

P2.4/P2.5:
  - Dual-mode cache: always writes LOCAL .jsonl; also uploads to GCS when GCS_BUCKET is set.
  - Measures: extract_sec, transform_sec, data_size_bytes, cache_mode ("local"|"local+gcs").
  - Cassandra connect uses retry loop (fixes Connection refused on fresh boot).
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cassandra.cluster import Cluster

# ── GCS (optional) ────────────────────────────────────────────────────────────
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()   # bucket name only, no gs://
GCS_PREFIX = os.getenv("GCS_PREFIX", "tenantB")    # folder prefix inside bucket
USE_GCS    = bool(GCS_BUCKET)

if USE_GCS:
    try:
        from google.cloud import storage as _gcs
        _gcs_client = _gcs.Client()
    except Exception as _e:
        print(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "pipeline": "silverpipeline_tenantB", "stage": "init",
            "status": "warning",
            "detail": f"GCS unavailable ({_e}); falling back to local-only",
        }), flush=True)
        USE_GCS = False


# ── logging helper ─────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log_event(stage: str, status: str, detail: dict = None):
    print(json.dumps({
        "ts":       now_iso(),
        "pipeline": "silverpipeline_tenantB",
        "stage":    stage,
        "status":   status,
        **(detail or {}),
    }), flush=True)


# ── GCS helpers ────────────────────────────────────────────────────────────────

def gcs_upload_file(local_path: Path, subfolder: str) -> str:
    """Upload local file to GCS. Returns gs:// URI."""
    bucket    = _gcs_client.bucket(GCS_BUCKET)
    blob_name = f"{GCS_PREFIX}/{subfolder}/{local_path.name}"
    bucket.blob(blob_name).upload_from_filename(str(local_path))
    return f"gs://{GCS_BUCKET}/{blob_name}"

def gcs_delete_blob(blob_name: str):
    bucket = _gcs_client.bucket(GCS_BUCKET)
    b = bucket.blob(blob_name)
    if b.exists():
        b.delete()


# ── transformations ────────────────────────────────────────────────────────────

def safe_float(x) -> Optional[float]:
    try:    return float(x)
    except: return None

def has_pm_data(measurements: dict) -> bool:
    return bool(measurements.get("P1") or measurements.get("P2"))

def transform_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Bronze -> Silver: flatten nested structure, cast coords, add has_pm_data flag."""
    measurements = raw.get("measurements") or {}
    return {
        "location_id": raw.get("location_id", "unknown"),
        "event_ts":    raw.get("event_ts"),
        "event_id":    raw.get("event_id", 0),
        "ingest_ts":   raw.get("ingest_ts"),
        "lat":         safe_float(raw.get("lat")),
        "lon":         safe_float(raw.get("lon")),
        "alt":         safe_float(raw.get("alt")),
        "country":     raw.get("country"),
        "sensor_id":   raw.get("sensor_id"),
        "pm10":        safe_float(measurements.get("P1")),
        "pm2_5":       safe_float(measurements.get("P2")),
        "has_pm_data": has_pm_data(measurements),
        "silver_ts":   now_iso(),
    }


# ── watermark ──────────────────────────────────────────────────────────────────

def get_watermark(session) -> Optional[str]:
    try:
        row = session.execute(
            "SELECT last_processed_ts FROM platform_logs.silver_watermarks "
            "WHERE tenant_id = 'tenantB'"
        ).one()
        return str(row.last_processed_ts) if row else None
    except Exception:
        return None

def set_watermark(session, ts: str):
    session.execute(
        "INSERT INTO platform_logs.silver_watermarks "
        "(tenant_id, last_processed_ts) VALUES ('tenantB', %s)", (ts,)
    )


# ── STAGE 1 -- Extract: bronze Cassandra -> local .jsonl (+ optional GCS) ─────

def extract_to_cache(
    session,
    pending_dir: Path,
    run_id: str,
    max_records: int,
    watermark_ts: Optional[str],
) -> Tuple[List[Path], int, str]:
    """
    Returns: (list_of_local_files, total_bytes_written, cache_mode)
    cache_mode = "local" or "local+gcs"
    """
    pending_dir.mkdir(parents=True, exist_ok=True)

    if watermark_ts:
        rows = session.execute(
            """SELECT location_id, ingest_ts, event_ts, event_id, payload
               FROM tenantB_bronze.records
               WHERE ingest_ts > %s LIMIT %s ALLOW FILTERING""",
            (watermark_ts, max_records),
        )
    else:
        rows = session.execute(
            f"SELECT location_id, ingest_ts, event_ts, event_id, payload "
            f"FROM tenantB_bronze.records LIMIT {max_records}"
        )

    records = list(rows)
    if not records:
        return [], 0, "local"

    cache_file  = pending_dir / f"tenantB_{run_id}.jsonl"
    total_bytes = 0
    with open(cache_file, "w") as fh:
        for row in records:
            try:
                payload_obj = json.loads(row.payload)
            except Exception:
                payload_obj = {}
            geo = payload_obj.get("geo") or {}
            dev = payload_obj.get("device") or {}
            rec = {
                "location_id":  row.location_id,
                "ingest_ts":    str(row.ingest_ts),
                "event_ts":     row.event_ts,
                "event_id":     row.event_id,
                "lat":          geo.get("lat"),
                "lon":          geo.get("lon"),
                "alt":          geo.get("alt"),
                "country":      geo.get("country"),
                "sensor_id":    dev.get("sensor_id"),
                "measurements": payload_obj.get("measurements") or {},
            }
            line = json.dumps(rec) + "\n"
            fh.write(line)
            total_bytes += len(line.encode("utf-8"))

    cache_mode = "local"

    # Also upload to GCS -- failure does NOT abort the pipeline
    if USE_GCS:
        try:
            t0      = time.time()
            gcs_uri = gcs_upload_file(cache_file, "pending")
            log_event("extract", "gcs_upload_ok", {
                "gcs_uri":        gcs_uri,
                "bytes":          total_bytes,
                "gcs_upload_sec": round(time.time() - t0, 3),
            })
            cache_mode = "local+gcs"
        except Exception as e:
            log_event("extract", "gcs_upload_failed", {"error": str(e)})

    return [cache_file], total_bytes, cache_mode


# ── STAGE 2 -- Transform + Load: .jsonl -> silver Cassandra ───────────────────

def transform_and_load(
    session,
    cache_files: List[Path],
    processed_dir: Path,
) -> Dict[str, Any]:
    """
    Returns: {records_loaded, errors, transform_sec, max_ingest_ts}
    Moves each file: local pending/ -> processed/ and GCS pending/ -> processed/.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    prepared = session.prepare(
        """INSERT INTO tenantB_silver.geo_measurements
           (location_id, silver_ts, event_ts, event_id, ingest_ts,
            lat, lon, alt, country, sensor_id, pm10, pm2_5, has_pm_data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    total_records = 0
    total_errors  = 0
    max_ingest_ts = None
    t_start       = time.time()

    for cache_file in cache_files:
        with open(cache_file) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw    = json.loads(line)
                    silver = transform_record(raw)
                    session.execute(prepared, (
                        silver["location_id"], silver["silver_ts"],
                        silver["event_ts"],    int(silver["event_id"] or 0),
                        silver["ingest_ts"],   silver["lat"],
                        silver["lon"],         silver["alt"],
                        silver["country"],
                        str(silver["sensor_id"]) if silver["sensor_id"] else None,
                        silver["pm10"],        silver["pm2_5"],
                        silver["has_pm_data"],
                    ))
                    total_records += 1
                    its = raw.get("ingest_ts")
                    if its and (max_ingest_ts is None or its > max_ingest_ts):
                        max_ingest_ts = its
                except Exception as e:
                    total_errors += 1
                    log_event("transform_load", "record_error",
                              {"error": str(e), "line": line[:120]})

        # Move local file: pending -> processed
        dest = processed_dir / cache_file.name
        cache_file.rename(dest)

        # Move GCS blob: pending/ -> processed/
        if USE_GCS:
            try:
                gcs_upload_file(dest, "processed")
                gcs_delete_blob(f"{GCS_PREFIX}/pending/{cache_file.name}")
                log_event("transform_load", "gcs_move_ok", {"file": cache_file.name})
            except Exception as e:
                log_event("transform_load", "gcs_move_failed", {"error": str(e)})

    return {
        "records_loaded": total_records,
        "errors":         total_errors,
        "transform_sec":  round(time.time() - t_start, 3),
        "max_ingest_ts":  max_ingest_ts,
    }


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id",        default=str(uuid.uuid4())[:8])
    parser.add_argument("--max-records",   type=int,
                        default=int(os.getenv("MAX_RECORDS", "3000")))
    parser.add_argument("--cassandra",     default=os.getenv("CASSANDRA_HOST", "cassandra"))
    parser.add_argument("--pending-dir",
                        default=os.getenv("CACHE_PENDING_DIR",   "/app/cache/tenantB/pending"))
    parser.add_argument("--processed-dir",
                        default=os.getenv("CACHE_PROCESSED_DIR", "/app/cache/tenantB/processed"))
    args = parser.parse_args()

    run_id        = args.run_id
    pending_dir   = Path(args.pending_dir)
    processed_dir = Path(args.processed_dir)
    t_total       = time.time()

    log_event("start", "ok", {
        "run_id":      run_id,
        "max_records": args.max_records,
        "gcs_enabled": USE_GCS,
        "gcs_bucket":  GCS_BUCKET or None,
    })

    # ── Cassandra connect with retry ───────────────────────────────────────────
    # Retry up to 10 times -- pipeline subprocess may start before Cassandra
    # accepts new connections even after the Docker healthcheck passes.
    cluster = None
    session = None
    for _attempt in range(1, 11):
        try:
            cluster = Cluster([args.cassandra])
            session = cluster.connect()
            log_event("cassandra_connect", "ok", {"attempt": _attempt})
            break
        except Exception as e:
            log_event("cassandra_connect", "retry", {"attempt": _attempt, "error": str(e)})
            if _attempt == 10:
                log_event("cassandra_connect", "error", {"error": str(e)})
                sys.exit(1)
            time.sleep(3 * _attempt)  # 3s, 6s, 9s ... 27s

    # ── Extract ────────────────────────────────────────────────────────────────
    try:
        watermark = get_watermark(session)
        log_event("extract", "start", {"watermark": watermark})
        t_ex = time.time()
        cache_files, data_size_bytes, cache_mode = extract_to_cache(
            session, pending_dir, run_id, args.max_records, watermark
        )
        extract_sec = round(time.time() - t_ex, 3)
        log_event("extract", "ok", {
            "files_written":   len(cache_files),
            "data_size_bytes": data_size_bytes,
            "extract_sec":     extract_sec,
            "cache_mode":      cache_mode,
        })
    except Exception as e:
        log_event("extract", "error", {"error": str(e)})
        cluster.shutdown()
        sys.exit(1)

    if not cache_files:
        log_event("extract", "no_new_data", {"watermark": watermark})
        cluster.shutdown()
        sys.exit(0)

    # ── Transform + Load ───────────────────────────────────────────────────────
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

    # ── "complete" line -- batchmanager reads ALL these fields ────────────────
    elapsed_sec = round(time.time() - t_total, 3)
    log_event("complete", "ok", {
        "run_id":          run_id,
        "elapsed_sec":     elapsed_sec,
        "extract_sec":     extract_sec,
        "transform_sec":   result["transform_sec"],
        "data_size_bytes": data_size_bytes,
        "cache_mode":      cache_mode,
        "records_loaded":  result["records_loaded"],
        "errors":          result["errors"],
    })

    cluster.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    main()