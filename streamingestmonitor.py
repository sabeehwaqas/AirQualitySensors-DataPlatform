# streamingestmonitor.py
import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import requests
from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

APP_PORT = int(os.getenv("MONITOR_PORT", "8002"))
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

# Where to send alerts (manager API)
MANAGER_URL = os.getenv("MANAGER_URL", "http://streamingestmanager:8001")

# Threshold config (tune for your demo)
MAX_AVG_INGEST_MS = float(os.getenv("MAX_AVG_INGEST_MS", "50.0"))  # if avg_ingest_ms > this => alert
MIN_RECORDS_PER_WINDOW = int(os.getenv("MIN_RECORDS_PER_WINDOW", "1"))  # avoid alerting on empty windows

# Retry tuning for Cassandra
RETRY_INITIAL_SEC = float(os.getenv("CASSANDRA_RETRY_INITIAL_SEC", "0.5"))
RETRY_MAX_SEC = float(os.getenv("CASSANDRA_RETRY_MAX_SEC", "10.0"))

cluster: Optional[Cluster] = None
session = None
prepared_insert = None
cassandra_ready = False


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorkerReport(BaseModel):
    tenant_id: str = Field(..., min_length=1)
    worker_id: str = Field(..., min_length=1)
    ts: datetime
    window_sec: int = Field(..., ge=1)

    avg_ingest_ms: float = Field(..., ge=0)
    records: int = Field(..., ge=0)
    bytes: int = Field(..., ge=0)
    errors: int = Field(..., ge=0)


async def _connect_cassandra_with_retry():
    global cluster, session, prepared_insert, cassandra_ready

    delay = RETRY_INITIAL_SEC
    while True:
        try:
            if cluster is None:
                cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()

            prepared_insert = session.prepare(
                """
                INSERT INTO platform_logs.streaming_metrics
                (tenant_id, worker_id, ts, window_sec, avg_ingest_ms, records, bytes, errors)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            cassandra_ready = True
            print(f"✅ Cassandra connected: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return
        except NoHostAvailable:
            cassandra_ready = False
            print(f"⏳ Cassandra not ready. Retrying in {delay:.1f}s...")
        except Exception as e:
            cassandra_ready = False
            print(f"⏳ Cassandra connect error ({type(e).__name__}). Retrying in {delay:.1f}s...")

        await asyncio.sleep(delay)
        delay = min(RETRY_MAX_SEC, delay * 2)


def _send_alert_best_effort(alert_payload: dict) -> None:
    """
    Best-effort: do not block /report on manager connectivity.
    """
    try:
        r = requests.post(f"{MANAGER_URL.rstrip('/')}/monitor/alerts", json=alert_payload, timeout=2.0)
        if r.status_code >= 300:
            print(f"⚠️ manager rejected alert: status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        print(f"⚠️ failed to notify manager: {type(e).__name__}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_connect_cassandra_with_retry())
    try:
        yield
    finally:
        task.cancel()
        global cluster
        try:
            if cluster:
                cluster.shutdown()
        except Exception:
            pass


app = FastAPI(title="mysimbdp-streamingestmonitor", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "ok": True,
        "cassandra_ready": cassandra_ready,
        "cassandra": f"{CASSANDRA_HOST}:{CASSANDRA_PORT}",
        "manager_url": MANAGER_URL,
        "threshold_max_avg_ingest_ms": MAX_AVG_INGEST_MS,
    }


@app.post("/report")
def report(r: WorkerReport):
    if not cassandra_ready or prepared_insert is None:
        raise HTTPException(status_code=503, detail="Cassandra not ready (monitor is retrying)")

    # 1) Persist KPI report
    try:
        session.execute(
            prepared_insert,
            (
                r.tenant_id,
                r.worker_id,
                r.ts,
                r.window_sec,
                float(r.avg_ingest_ms),
                int(r.records),
                int(r.bytes),
                int(r.errors),
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to persist report: {type(e).__name__}: {e}")

    # 2) Threshold check -> notify manager
    if r.records >= MIN_RECORDS_PER_WINDOW and r.avg_ingest_ms > MAX_AVG_INGEST_MS:
        alert = {
            "tenant_id": r.tenant_id,
            "worker_id": r.worker_id,
            "ts": iso_utc_now(),
            "issue": "avg_ingest_ms_high",
            "avg_ingest_ms": float(r.avg_ingest_ms),
            "records": int(r.records),
            "window_sec": int(r.window_sec),
            "threshold": float(MAX_AVG_INGEST_MS),
            "suggested_action": "restart",  # for demo; manager will restart if you keep that logic
        }
        _send_alert_best_effort(alert)

    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)