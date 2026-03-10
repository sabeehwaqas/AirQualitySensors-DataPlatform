#!/usr/bin/env python3
"""
batchmanager.py
mysimbdp-batchmanager

- Reads tenant service agreements from /app/tenant_configs/*.yaml
- Schedules silverpipeline runs every N minutes per tenant (APScheduler)
- Enforces constraints BEFORE launching pipeline (constraint violations block the run)
- Launches silverpipeline as a subprocess (blackbox model)
- Captures stdout logs from pipeline and persists to Cassandra
- Exposes FastAPI endpoints for manual trigger, history, and stats
"""

import glob
import json
import os
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import FastAPI, HTTPException

# ── config ────────────────────────────────────────────────────────────────────

CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST", "cassandra")
TENANT_CONFIGS  = os.getenv("TENANT_CONFIGS_DIR", "/app/tenant_configs")
APP_PORT        = int(os.getenv("BATCH_MANAGER_PORT", "8003"))
CACHE_BASE      = os.getenv("CACHE_BASE_DIR", "/app/cache")

# ── globals ───────────────────────────────────────────────────────────────────

_session = None
_prepared_log = None
_cassandra_ready = False


# ── Cassandra helpers ─────────────────────────────────────────────────────────

def _connect_cassandra():
    global _session, _prepared_log, _cassandra_ready
    for attempt in range(1, 31):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            _session = cluster.connect()
            _prepared_log = _session.prepare("""
                INSERT INTO platform_logs.silver_pipeline_logs
                (tenant_id, run_id, started_at, finished_at, status,
                 records_loaded, errors, elapsed_sec, pipeline_script, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            _cassandra_ready = True
            print(f"✅ batchmanager connected to Cassandra")
            return
        except NoHostAvailable:
            print(f"⏳ Cassandra not ready (attempt {attempt}/30). Waiting...")
            time.sleep(5)
        except Exception as e:
            print(f"⏳ Cassandra error: {e}. Retrying...")
            time.sleep(5)
    print("❌ Could not connect to Cassandra after 30 attempts. Logging will be disabled.")


def persist_log(
    tenant_id: str,
    run_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    records_loaded: int,
    errors: int,
    elapsed_sec: float,
    pipeline_script: str,
    detail: str,
):
    if not _cassandra_ready or _prepared_log is None:
        print(f"⚠️ Cassandra not ready — skipping log persist for run {run_id}")
        return
    try:
        _session.execute(_prepared_log, (
            tenant_id, run_id, started_at, finished_at,
            status, records_loaded, errors, elapsed_sec,
            pipeline_script, detail[:4000],
        ))
    except Exception as e:
        print(f"⚠️ Failed to persist log: {e}")


# ── config loader ─────────────────────────────────────────────────────────────

def load_tenant_configs() -> Dict[str, dict]:
    configs = {}
    pattern = os.path.join(TENANT_CONFIGS, "*.yaml")
    for path in glob.glob(pattern):
        with open(path) as f:
            cfg = yaml.safe_load(f)
        tid = cfg.get("tenant_id")
        if tid:
            configs[tid] = cfg
            print(f"✅ Loaded config for tenant: {tid}")
    return configs


# ── constraint checker ────────────────────────────────────────────────────────

def check_constraints(tenant_id: str, cfg: dict) -> Optional[str]:
    """
    Returns None if all constraints pass, or a violation message string.
    This is called by batchmanager BEFORE launching the pipeline.
    """
    constraints = cfg.get("constraints", {})
    data_limits = cfg.get("data_limits", {})
    silver_table = cfg.get("silver_table", {})
    caching = cfg.get("caching", {})

    # 1. Check if silver table keyspace exists
    if constraints.get("reject_if_silver_table_missing"):
        keyspace = silver_table.get("keyspace", "")
        table    = silver_table.get("table", "")
        if keyspace and _cassandra_ready:
            try:
                _session.execute(f"SELECT * FROM {keyspace}.{table} LIMIT 1")
            except Exception:
                return f"CONSTRAINT_VIOLATION: Silver table {keyspace}.{table} does not exist"

    # 2. Check pending dir overflow
    if constraints.get("reject_if_pending_overflow"):
        pending_dir = Path(caching.get("pending_dir", f"/app/cache/{tenant_id}/pending"))
        max_pending = int(data_limits.get("max_cache_files_pending", 20))
        if pending_dir.exists():
            pending_count = len(list(pending_dir.glob("*.jsonl")))
            if pending_count >= max_pending:
                return (f"CONSTRAINT_VIOLATION: pending dir has {pending_count} files "
                        f"(max={max_pending}). Clear processed files before next run.")

    # 3. Check bronze has data (best-effort)
    if constraints.get("require_non_empty_bronze"):
        keyspace_bronze = f"{tenant_id}_bronze"
        try:
            rows = _session.execute(f"SELECT COUNT(*) FROM {keyspace_bronze}.records")
            count = rows.one()[0]
            if count == 0:
                return f"CONSTRAINT_VIOLATION: Bronze table {keyspace_bronze}.records is empty"
        except Exception as e:
            # Table might just not exist yet — don't block run
            pass

    return None  # all constraints passed


# ── pipeline runner ───────────────────────────────────────────────────────────

def run_pipeline(tenant_id: str, cfg: dict):
    """
    Invokes the silverpipeline for a tenant as a subprocess (blackbox).
    Captures stdout, parses JSON log lines, persists summary to Cassandra.
    """
    run_id  = str(uuid.uuid4())[:8]
    t_start = time.time()
    started_at = datetime.now(timezone.utc).isoformat()

    pipeline_script = cfg.get("pipeline_script", f"silverpipeline_{tenant_id}.py")
    data_limits     = cfg.get("data_limits", {})
    caching         = cfg.get("caching", {})
    silver_table    = cfg.get("silver_table", {})
    scheduling      = cfg.get("scheduling", {})

    max_records   = int(data_limits.get("max_records_per_run", 5000))
    max_runtime   = int(scheduling.get("max_runtime_seconds", 120))
    pending_dir   = caching.get("pending_dir",   f"/app/cache/{tenant_id}/pending")
    processed_dir = caching.get("processed_dir", f"/app/cache/{tenant_id}/processed")

    print(f"\n==> [{tenant_id}] Starting pipeline run {run_id} script={pipeline_script}")

    # ── Constraint check ──────────────────────────────────────────────────────
    violation = check_constraints(tenant_id, cfg)
    if violation:
        elapsed = round(time.time() - t_start, 3)
        finished_at = datetime.now(timezone.utc).isoformat()
        print(f"🚫 [{tenant_id}] Run {run_id} BLOCKED: {violation}")
        persist_log(
            tenant_id, run_id, started_at, finished_at,
            "constraint_violation", 0, 0, elapsed,
            pipeline_script, violation,
        )
        return

    # ── Launch subprocess ─────────────────────────────────────────────────────
    env = {
        **os.environ,
        "CASSANDRA_HOST":      CASSANDRA_HOST,
        "CACHE_PENDING_DIR":   pending_dir,
        "CACHE_PROCESSED_DIR": processed_dir,
        "MAX_RECORDS":         str(max_records),
    }

    cmd = [
        "python", pipeline_script,
        "--run-id",      run_id,
        "--max-records", str(max_records),
        "--cassandra",   CASSANDRA_HOST,
        "--pending-dir",   pending_dir,
        "--processed-dir", processed_dir,
    ]

    stdout_lines: List[str] = []
    status = "unknown"
    records_loaded = 0
    errors = 0

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd="/app",
        )

        # Stream output with timeout
        try:
            stdout, _ = proc.communicate(timeout=max_runtime)
            stdout_lines = stdout.splitlines()
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            elapsed = round(time.time() - t_start, 3)
            finished_at = datetime.now(timezone.utc).isoformat()
            msg = f"Pipeline timed out after {max_runtime}s"
            print(f"⏱ [{tenant_id}] Run {run_id}: {msg}")
            persist_log(tenant_id, run_id, started_at, finished_at,
                        "timeout", 0, 0, elapsed, pipeline_script, msg)
            return

        exit_code = proc.returncode

        # Parse JSON log lines from pipeline stdout
        for line in stdout_lines:
            print(f"  [{tenant_id}] {line}")
            try:
                obj = json.loads(line)
                if obj.get("stage") == "complete" and obj.get("status") == "ok":
                    records_loaded = obj.get("records_loaded", 0)
                    errors         = obj.get("errors", 0)
                elif obj.get("status") == "error":
                    errors += 1
            except Exception:
                pass  # non-JSON stdout line, ignore

        status = "success" if exit_code == 0 else "failed"

    except FileNotFoundError:
        status = "failed"
        stdout_lines = [f"Pipeline script not found: {pipeline_script}"]
        print(f"❌ [{tenant_id}] {stdout_lines[0]}")

    except Exception as e:
        status = "failed"
        stdout_lines = [f"Unexpected error: {e}"]
        print(f"❌ [{tenant_id}] Run {run_id} error: {e}")

    elapsed = round(time.time() - t_start, 3)
    finished_at = datetime.now(timezone.utc).isoformat()
    detail = "\n".join(stdout_lines[-50:])  # keep last 50 lines

    print(f"✅ [{tenant_id}] Run {run_id} finished: status={status} "
          f"records={records_loaded} errors={errors} elapsed={elapsed}s")

    persist_log(
        tenant_id, run_id, started_at, finished_at,
        status, records_loaded, errors, elapsed,
        pipeline_script, detail,
    )


# ── scheduler setup ───────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()

def setup_schedules(configs: Dict[str, dict]):
    for tenant_id, cfg in configs.items():
        interval = int(cfg.get("scheduling", {}).get("interval_minutes", 5))
        scheduler.add_job(
            run_pipeline,
            "interval",
            minutes=interval,
            args=[tenant_id, cfg],
            id=f"silver_{tenant_id}",
            replace_existing=True,
        )
        print(f"📅 Scheduled silverpipeline for {tenant_id} every {interval} min")


# ── FastAPI lifespan ──────────────────────────────────────────────────────────

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


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "ok": True,
        "cassandra_ready": _cassandra_ready,
        "tenants": list(TENANT_CONFIGS_MAP.keys()),
    }

@app.get("/tenants")
def list_tenants():
    return {
        tid: {
            "pipeline_script": cfg.get("pipeline_script"),
            "interval_minutes": cfg.get("scheduling", {}).get("interval_minutes"),
            "silver_table": cfg.get("silver_table"),
        }
        for tid, cfg in TENANT_CONFIGS_MAP.items()
    }

@app.post("/tenants/{tenant_id}/run")
def trigger_run(tenant_id: str):
    """Manually trigger a pipeline run for a tenant."""
    if tenant_id not in TENANT_CONFIGS_MAP:
        raise HTTPException(404, detail=f"Unknown tenant: {tenant_id}")
    import threading
    t = threading.Thread(target=run_pipeline, args=[tenant_id, TENANT_CONFIGS_MAP[tenant_id]])
    t.daemon = True
    t.start()
    return {"ok": True, "tenant": tenant_id, "message": "Pipeline triggered in background"}

@app.get("/tenants/{tenant_id}/logs")
def get_logs(tenant_id: str, limit: int = 20):
    """Fetch recent pipeline run logs for a tenant."""
    if not _cassandra_ready:
        raise HTTPException(503, detail="Cassandra not ready")
    try:
        rows = _session.execute(
            f"""SELECT run_id, started_at, finished_at, status,
                       records_loaded, errors, elapsed_sec, pipeline_script
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
    """Simple stats across all tenants for observability."""
    if not _cassandra_ready:
        raise HTTPException(503, detail="Cassandra not ready")
    result = {}
    for tenant_id in TENANT_CONFIGS_MAP:
        try:
            rows = _session.execute(
                f"""SELECT status, COUNT(*) as cnt, SUM(records_loaded) as total_records,
                           SUM(errors) as total_errors, AVG(elapsed_sec) as avg_elapsed
                    FROM platform_logs.silver_pipeline_logs
                    WHERE tenant_id = '{tenant_id}'
                    GROUP BY status
                    ALLOW FILTERING"""
            )
            result[tenant_id] = [dict(r._asdict()) for r in rows]
        except Exception as e:
            result[tenant_id] = {"error": str(e)}
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)