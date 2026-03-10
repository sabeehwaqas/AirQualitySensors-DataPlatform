#!/usr/bin/env python3
"""
batchmanager.py  —  mysimbdp-batchmanager

- Reads tenant service agreements from /app/tenant_configs/*.yaml
- Schedules silverpipeline runs every N minutes per tenant (APScheduler)
- Enforces constraints BEFORE launching pipeline
- Launches silverpipeline as a subprocess (blackbox model)
- Captures the structured JSON "complete" line from pipeline stdout
- Persists full run metrics to Cassandra (including extract_sec, transform_sec,
  data_size_bytes, cache_mode  — new in P2.4/P2.5)
- Fixed /stats endpoint: aggregates in Python (Cassandra GROUP BY limitation)
"""

import glob
import json
import os
import subprocess
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import FastAPI, HTTPException

# ── config ────────────────────────────────────────────────────────────────────

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
TENANT_CONFIGS = os.getenv("TENANT_CONFIGS_DIR", "/app/tenant_configs")
APP_PORT       = int(os.getenv("BATCH_MANAGER_PORT", "8003"))
CACHE_BASE     = os.getenv("CACHE_BASE_DIR", "/app/cache")

# ── globals ───────────────────────────────────────────────────────────────────

_session       = None
_prepared_log  = None
_cassandra_ok  = False


# ── Cassandra ─────────────────────────────────────────────────────────────────

def _connect_cassandra():
    global _session, _prepared_log, _cassandra_ok
    for attempt in range(1, 31):
        try:
            cluster   = Cluster([CASSANDRA_HOST])
            _session  = cluster.connect()
            # Prepare insert — matches the updated table schema (P2.4/P2.5 columns)
            _prepared_log = _session.prepare("""
                INSERT INTO platform_logs.silver_pipeline_logs
                (tenant_id, run_id, started_at, finished_at, status,
                 records_loaded, errors, elapsed_sec,
                 extract_sec, transform_sec, data_size_bytes, cache_mode,
                 pipeline_script, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            _cassandra_ok = True
            print("✅ batchmanager connected to Cassandra")
            return
        except NoHostAvailable:
            print(f"⏳ Cassandra host not reachable (attempt {attempt}/30). Waiting 5s...")
            time.sleep(5)
        except Exception as e:
            # This usually means the table schema is missing a column.
            # Run the ALTER TABLE commands in fix_schema.sh to resolve.
            print(f"⏳ Cassandra prepare failed (attempt {attempt}/30): {type(e).__name__}: {e}")
            time.sleep(5)
    print("❌ Could not connect to Cassandra after 30 attempts.")
    print("   Most likely cause: silver_pipeline_logs table is missing columns.")
    print("   Fix: run fix_schema.sh, then restart batchmanager.")


def persist_log(
    tenant_id:       str,
    run_id:          str,
    started_at:      str,
    finished_at:     str,
    status:          str,
    records_loaded:  int,
    errors:          int,
    elapsed_sec:     float,
    extract_sec:     float,
    transform_sec:   float,
    data_size_bytes: int,
    cache_mode:      str,
    pipeline_script: str,
    detail:          str,
):
    if not _cassandra_ok or _prepared_log is None:
        print(f"⚠️ Cassandra not ready — skipping log for run {run_id}")
        return
    try:
        _session.execute(_prepared_log, (
            tenant_id, run_id, started_at, finished_at, status,
            records_loaded, errors, elapsed_sec,
            extract_sec, transform_sec, data_size_bytes, cache_mode,
            pipeline_script, detail[:4000],
        ))
    except Exception as e:
        print(f"⚠️ Failed to persist log: {e}")


# ── config loader ──────────────────────────────────────────────────────────────

def load_tenant_configs() -> Dict[str, dict]:
    configs = {}
    for path in glob.glob(os.path.join(TENANT_CONFIGS, "*.yaml")):
        with open(path) as f:
            cfg = yaml.safe_load(f)
        tid = cfg.get("tenant_id")
        if tid:
            configs[tid] = cfg
            print(f"✅ Loaded config for tenant: {tid}")
    return configs


# ── constraint checker ─────────────────────────────────────────────────────────

def check_constraints(tenant_id: str, cfg: dict) -> Optional[str]:
    """Returns None on pass, or a violation string that blocks the run."""
    constraints  = cfg.get("constraints", {})
    data_limits  = cfg.get("data_limits", {})
    silver_table = cfg.get("silver_table", {})
    caching      = cfg.get("caching", {})

    if constraints.get("reject_if_silver_table_missing"):
        ks = silver_table.get("keyspace", "")
        tb = silver_table.get("table", "")
        if ks and _cassandra_ok:
            try:
                _session.execute(f"SELECT * FROM {ks}.{tb} LIMIT 1")
            except Exception:
                return f"CONSTRAINT_VIOLATION: Silver table {ks}.{tb} does not exist"

    if constraints.get("reject_if_pending_overflow"):
        pending_dir  = Path(caching.get("pending_dir", f"/app/cache/{tenant_id}/pending"))
        max_pending  = int(data_limits.get("max_cache_files_pending", 20))
        if pending_dir.exists():
            count = len(list(pending_dir.glob("*.jsonl")))
            if count >= max_pending:
                return (f"CONSTRAINT_VIOLATION: {count} pending files >= max {max_pending}. "
                        "Clear pending dir before next run.")

    if constraints.get("require_non_empty_bronze"):
        ks_bronze = f"{tenant_id}_bronze"
        try:
            cnt = _session.execute(f"SELECT COUNT(*) FROM {ks_bronze}.records").one()[0]
            if cnt == 0:
                return f"CONSTRAINT_VIOLATION: Bronze table {ks_bronze}.records is empty"
        except Exception:
            pass  # table might not exist yet — don't block

    # Check max_file_size_mb: if any existing pending file already exceeds the
    # limit, block the run — the pipeline would produce more of the same.
    if data_limits.get("max_file_size_mb"):
        max_bytes = int(data_limits["max_file_size_mb"]) * 1024 * 1024
        pending_dir = Path(caching.get("pending_dir", f"/app/cache/{tenant_id}/pending"))
        if pending_dir.exists():
            for f in pending_dir.glob("*.jsonl"):
                size = f.stat().st_size
                if size > max_bytes:
                    return (f"CONSTRAINT_VIOLATION: Pending file {f.name} is "
                            f"{size // (1024*1024)}MB which exceeds max "
                            f"{data_limits['max_file_size_mb']}MB")

    # require_geo_fields (tenantB): check that bronze records have geo data
    # by sampling a few rows and verifying lat/lon exist in the payload
    if constraints.get("require_geo_fields") and _cassandra_ok:
        ks_bronze = f"{tenant_id}_bronze"
        try:
            rows = list(_session.execute(
                f"SELECT payload FROM {ks_bronze}.records LIMIT 5"
            ))
            import json as _json
            missing = 0
            for row in rows:
                try:
                    obj = _json.loads(row.payload)
                    geo = obj.get("geo") or {}
                    if not geo.get("lat") or not geo.get("lon"):
                        missing += 1
                except Exception:
                    missing += 1
            if rows and missing == len(rows):
                return (f"CONSTRAINT_VIOLATION: require_geo_fields=true but sampled "
                        f"{missing}/{len(rows)} records have no geo.lat/geo.lon")
        except Exception:
            pass  # don't block if we can't check

    return None


# ── pipeline runner ────────────────────────────────────────────────────────────

def run_pipeline(tenant_id: str, cfg: dict):
    """
    Invokes the silverpipeline for tenant_id as a subprocess (blackbox).
    Reads the JSON 'complete' line from stdout to get metrics.
    Persists the full run record to Cassandra.
    """
    run_id     = str(uuid.uuid4())[:8]
    t_start    = time.time()
    started_at = datetime.now(timezone.utc).isoformat()

    pipeline_script  = cfg.get("pipeline_script", f"silverpipeline_{tenant_id}.py")
    data_limits      = cfg.get("data_limits", {})
    caching          = cfg.get("caching", {})
    scheduling       = cfg.get("scheduling", {})

    max_records   = int(data_limits.get("max_records_per_run", 5000))
    max_runtime   = int(scheduling.get("max_runtime_seconds", 120))
    pending_dir   = caching.get("pending_dir",   f"/app/cache/{tenant_id}/pending")
    processed_dir = caching.get("processed_dir", f"/app/cache/{tenant_id}/processed")

    print(f"\n==> [{tenant_id}] Run {run_id} starting — script={pipeline_script}")

    # ── Constraint check ───────────────────────────────────────────────────────
    violation = check_constraints(tenant_id, cfg)
    if violation:
        elapsed = round(time.time() - t_start, 3)
        print(f"🚫 [{tenant_id}] Run {run_id} BLOCKED: {violation}")
        persist_log(
            tenant_id, run_id, started_at, datetime.now(timezone.utc).isoformat(),
            "constraint_violation", 0, 0, elapsed,
            0.0, 0.0, 0, "none",
            pipeline_script, violation,
        )
        return

    # ── Launch subprocess ──────────────────────────────────────────────────────
    cmd = [
        "python", pipeline_script,
        "--run-id",        run_id,
        "--max-records",   str(max_records),
        "--cassandra",     CASSANDRA_HOST,
        "--pending-dir",   pending_dir,
        "--processed-dir", processed_dir,
    ]
    env = {
        **os.environ,
        "CASSANDRA_HOST":      CASSANDRA_HOST,
        "CACHE_PENDING_DIR":   pending_dir,
        "CACHE_PROCESSED_DIR": processed_dir,
        "MAX_RECORDS":         str(max_records),
        # GCS_BUCKET is inherited from os.environ automatically if set
    }

    stdout_lines: List[str] = []
    status          = "unknown"
    records_loaded  = 0
    errors          = 0
    extract_sec     = 0.0
    transform_sec   = 0.0
    data_size_bytes = 0
    cache_mode      = "local"

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, cwd="/app",
        )
        try:
            stdout, _ = proc.communicate(timeout=max_runtime)
            stdout_lines = stdout.splitlines()
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            msg = f"Pipeline timed out after {max_runtime}s"
            print(f"⏱ [{tenant_id}] Run {run_id}: {msg}")
            persist_log(
                tenant_id, run_id, started_at, datetime.now(timezone.utc).isoformat(),
                "timeout", 0, 0, round(time.time() - t_start, 3),
                0.0, 0.0, 0, "local",
                pipeline_script, msg,
            )
            return

        exit_code = proc.returncode

        # Parse JSON lines from pipeline stdout — look for the 'complete' summary
        for line in stdout_lines:
            print(f"  [{tenant_id}] {line}")
            try:
                obj = json.loads(line)
                if obj.get("stage") == "complete" and obj.get("status") == "ok":
                    records_loaded  = obj.get("records_loaded",  0)
                    errors          = obj.get("errors",          0)
                    extract_sec     = float(obj.get("extract_sec",     0.0))
                    transform_sec   = float(obj.get("transform_sec",   0.0))
                    data_size_bytes = int(obj.get("data_size_bytes",   0))
                    cache_mode      = obj.get("cache_mode", "local")
                elif obj.get("status") == "error":
                    errors += 1
            except Exception:
                pass

        status = "success" if exit_code == 0 else "failed"

    except FileNotFoundError:
        status       = "failed"
        stdout_lines = [f"Pipeline script not found: {pipeline_script}"]
        print(f"❌ [{tenant_id}] {stdout_lines[0]}")
    except Exception as e:
        status       = "failed"
        stdout_lines = [f"Unexpected error launching pipeline: {e}"]
        print(f"❌ [{tenant_id}] Run {run_id}: {e}")

    elapsed     = round(time.time() - t_start, 3)
    finished_at = datetime.now(timezone.utc).isoformat()
    detail      = "\n".join(stdout_lines[-50:])

    print(f"✅ [{tenant_id}] Run {run_id}: status={status} "
          f"records={records_loaded} errors={errors} "
          f"extract={extract_sec}s transform={transform_sec}s "
          f"size={data_size_bytes}B cache={cache_mode} total={elapsed}s")

    persist_log(
        tenant_id, run_id, started_at, finished_at,
        status, records_loaded, errors, elapsed,
        extract_sec, transform_sec, data_size_bytes, cache_mode,
        pipeline_script, detail,
    )


# ── scheduler ─────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()

def setup_schedules(configs: Dict[str, dict]):
    import datetime as _dt
    # Delay first run by 30s so Cassandra schema is fully ready before the
    # very first automatic pipeline fires (manual /run calls are unaffected).
    first_run = datetime.now(timezone.utc) + _dt.timedelta(seconds=30)
    for tenant_id, cfg in configs.items():
        interval = int(cfg.get("scheduling", {}).get("interval_minutes", 5))
        scheduler.add_job(
            run_pipeline, "interval",
            minutes=interval,
            start_date=first_run,
            args=[tenant_id, cfg],
            id=f"silver_{tenant_id}",
            replace_existing=True,
        )
        print(f"📅 Scheduled {tenant_id} silverpipeline every {interval} min (first run in 30s)")


# ── FastAPI ────────────────────────────────────────────────────────────────────

TENANT_CONFIGS_MAP: Dict[str, dict] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global TENANT_CONFIGS_MAP
    _connect_cassandra()
    TENANT_CONFIGS_MAP = load_tenant_configs()
    setup_schedules(TENANT_CONFIGS_MAP)
    scheduler.start()
    print("✅ batchmanager scheduler started")
    yield
    scheduler.shutdown()

app = FastAPI(title="mysimbdp-batchmanager", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "ok":              True,
        "cassandra_ready": _cassandra_ok,
        "tenants":         list(TENANT_CONFIGS_MAP.keys()),
    }

@app.get("/tenants")
def list_tenants():
    return {
        tid: {
            "pipeline_script":  cfg.get("pipeline_script"),
            "interval_minutes": cfg.get("scheduling", {}).get("interval_minutes"),
            "silver_table":     cfg.get("silver_table"),
        }
        for tid, cfg in TENANT_CONFIGS_MAP.items()
    }

@app.post("/tenants/{tenant_id}/run")
def trigger_run(tenant_id: str):
    """Manually trigger a pipeline run (fires in a background thread)."""
    if tenant_id not in TENANT_CONFIGS_MAP:
        raise HTTPException(404, detail=f"Unknown tenant: {tenant_id}")
    import threading
    t = threading.Thread(
        target=run_pipeline,
        args=[tenant_id, TENANT_CONFIGS_MAP[tenant_id]],
        daemon=True,
    )
    t.start()
    return {"ok": True, "tenant": tenant_id, "message": "Pipeline triggered"}

@app.get("/tenants/{tenant_id}/logs")
def get_logs(tenant_id: str, limit: int = 20):
    """Fetch recent pipeline run records for a tenant."""
    if not _cassandra_ok:
        raise HTTPException(503, detail="Cassandra not ready")
    try:
        rows = _session.execute(
            f"""SELECT run_id, started_at, finished_at, status,
                       records_loaded, errors, elapsed_sec,
                       extract_sec, transform_sec, data_size_bytes, cache_mode,
                       pipeline_script
                FROM platform_logs.silver_pipeline_logs
                WHERE tenant_id = '{tenant_id}'
                ORDER BY started_at DESC
                LIMIT {limit}"""
        )
        return {"tenant": tenant_id, "logs": [dict(r._asdict()) for r in rows]}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@app.get("/stats")
def platform_stats():
    """
    Per-tenant and platform-wide statistics extracted from silver_pipeline_logs.
    Aggregated in Python because Cassandra GROUP BY only works on partition keys.
    """
    if not _cassandra_ok:
        raise HTTPException(503, detail="Cassandra not ready")

    platform_totals = {
        "total_runs": 0, "success": 0, "failed": 0, "constraint_violation": 0,
        "total_records": 0, "total_errors": 0, "total_bytes": 0,
        "avg_elapsed_sec": 0.0, "avg_extract_sec": 0.0, "avg_transform_sec": 0.0,
        "cache_modes": defaultdict(int),
    }
    result = {}

    for tenant_id in TENANT_CONFIGS_MAP:
        try:
            rows = list(_session.execute(
                f"""SELECT status, records_loaded, errors, elapsed_sec,
                           extract_sec, transform_sec, data_size_bytes, cache_mode
                    FROM platform_logs.silver_pipeline_logs
                    WHERE tenant_id = '{tenant_id}'
                    LIMIT 500 ALLOW FILTERING"""
            ))
        except Exception as e:
            result[tenant_id] = {"error": str(e)}
            continue

        # Aggregate per tenant in Python
        buckets = defaultdict(lambda: {
            "count": 0, "total_records": 0, "total_errors": 0,
            "total_bytes": 0, "sum_elapsed": 0.0,
            "sum_extract": 0.0, "sum_transform": 0.0,
        })
        cache_counts = defaultdict(int)

        for r in rows:
            b = buckets[r.status or "unknown"]
            b["count"]          += 1
            b["total_records"]  += (r.records_loaded or 0)
            b["total_errors"]   += (r.errors or 0)
            b["total_bytes"]    += (r.data_size_bytes or 0)
            b["sum_elapsed"]    += (r.elapsed_sec or 0.0)
            b["sum_extract"]    += (r.extract_sec or 0.0)
            b["sum_transform"]  += (r.transform_sec or 0.0)
            cache_counts[r.cache_mode or "local"] += 1

            # Roll into platform totals
            platform_totals["total_runs"]    += 1
            platform_totals["total_records"] += (r.records_loaded or 0)
            platform_totals["total_errors"]  += (r.errors or 0)
            platform_totals["total_bytes"]   += (r.data_size_bytes or 0)
            platform_totals["avg_elapsed_sec"]   += (r.elapsed_sec or 0.0)
            platform_totals["avg_extract_sec"]   += (r.extract_sec or 0.0)
            platform_totals["avg_transform_sec"] += (r.transform_sec or 0.0)
            platform_totals[r.status or "unknown"] = \
                platform_totals.get(r.status or "unknown", 0) + 1
            platform_totals["cache_modes"][r.cache_mode or "local"] += 1

        # Compute averages per status bucket
        tenant_summary = {}
        for status, b in buckets.items():
            n = b["count"]
            tenant_summary[status] = {
                "runs":              n,
                "total_records":     b["total_records"],
                "total_errors":      b["total_errors"],
                "total_bytes":       b["total_bytes"],
                "avg_elapsed_sec":   round(b["sum_elapsed"]   / n, 3) if n else 0,
                "avg_extract_sec":   round(b["sum_extract"]   / n, 3) if n else 0,
                "avg_transform_sec": round(b["sum_transform"] / n, 3) if n else 0,
            }

        result[tenant_id] = {
            "by_status":   tenant_summary,
            "cache_modes": dict(cache_counts),
        }

    # Finalize platform averages
    n = platform_totals["total_runs"]
    if n:
        platform_totals["avg_elapsed_sec"]   = round(platform_totals["avg_elapsed_sec"]   / n, 3)
        platform_totals["avg_extract_sec"]   = round(platform_totals["avg_extract_sec"]   / n, 3)
        platform_totals["avg_transform_sec"] = round(platform_totals["avg_transform_sec"] / n, 3)
    platform_totals["cache_modes"] = dict(platform_totals["cache_modes"])

    return {"per_tenant": result, "platform": platform_totals}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)