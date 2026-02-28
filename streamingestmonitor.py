# streamingestmonitor.py
import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

APP_PORT = int(os.getenv("MONITOR_PORT", "8002"))
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

# Retry tuning
RETRY_INITIAL_SEC = float(os.getenv("CASSANDRA_RETRY_INITIAL_SEC", "0.5"))
RETRY_MAX_SEC = float(os.getenv("CASSANDRA_RETRY_MAX_SEC", "10.0"))

cluster: Optional[Cluster] = None
session = None
prepared_insert = None
cassandra_ready = False


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
    """
    Keep trying Cassandra until it becomes available.
    This must NOT crash the API server.
    """
    global cluster, session, prepared_insert, cassandra_ready

    delay = RETRY_INITIAL_SEC
    while True:
        try:
            # Create cluster only once; recreate if needed
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
            print(f"✅ Cassandra connected: host={CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return
        except NoHostAvailable as e:
            cassandra_ready = False
            print(f"⏳ Cassandra not ready ({CASSANDRA_HOST}:{CASSANDRA_PORT}). Retrying in {delay:.1f}s. ({type(e).__name__})")
        except Exception as e:
            cassandra_ready = False
            print(f"⏳ Cassandra connect error. Retrying in {delay:.1f}s. ({type(e).__name__}: {e})")

        await asyncio.sleep(delay)
        delay = min(RETRY_MAX_SEC, delay * 2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background retry task; don't block startup
    task = asyncio.create_task(_connect_cassandra_with_retry())
    try:
        yield
    finally:
        task.cancel()
        # Graceful shutdown Cassandra
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
        "cassandra_host": f"{CASSANDRA_HOST}:{CASSANDRA_PORT}",
        "cassandra_ready": cassandra_ready,
    }


@app.post("/report")
def report(r: WorkerReport):
    if not cassandra_ready or prepared_insert is None:
        raise HTTPException(status_code=503, detail="Cassandra not ready (monitor will keep retrying)")

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

    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)