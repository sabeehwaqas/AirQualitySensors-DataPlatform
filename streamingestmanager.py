# streamingestmanager.py
import json
import os
import time
from typing import Dict, Any

import docker
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

APP_PORT = 8001

MONITOR_URL = os.getenv("MONITOR_URL", "http://streamingestmonitor:8002")
REPORT_WINDOW_SEC = os.getenv("REPORT_WINDOW_SEC", "10")

STATE_PATH = os.getenv("STATE_PATH", "/app/state/workers.json")
IMAGE_NAME = os.getenv("WORKER_IMAGE", "mysimbdp-platform:latest")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")

TENANT_TOPICS = {
    "tenantA": "tenantA.bronze.raw",
    "tenantB": "tenantB.bronze.raw",
}

app = FastAPI(title="mysimbdp-streamingestmanager")


class StartReq(BaseModel):
    instances: int = 1


class StopReq(BaseModel):
    instances: int = 1


class MonitorAlert(BaseModel):
    tenant_id: str
    worker_id: str
    ts: str
    issue: str
    avg_ingest_ms: float | None = None
    records: int | None = None
    window_sec: int | None = None
    threshold: float | None = None
    suggested_action: str | None = None


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        return {"workers": {}, "alerts": []}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        s = json.load(f)
    if "alerts" not in s:
        s["alerts"] = []
    if "workers" not in s:
        s["workers"] = {}
    return s


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_compose_network_name() -> str:
    """
    Auto-detect the docker-compose network by inspecting this container.
    Workers must join the same network so they can reach broker/cassandra/monitor by service name.
    """
    client = docker.from_env()
    me = client.containers.get(os.uname().nodename)
    nets = list(me.attrs["NetworkSettings"]["Networks"].keys())
    if not nets:
        raise RuntimeError("Manager is not attached to a docker network.")
    return nets[0]


@app.get("/health")
def health():
    return {"ok": True, "monitor_url": MONITOR_URL}


@app.get("/tenants/{tenant_id}/workers")
def list_workers(tenant_id: str):
    state = load_state()
    return {"tenant": tenant_id, "workers": state["workers"].get(tenant_id, {})}


@app.get("/monitor/alerts")
def list_alerts(limit: int = 50):
    state = load_state()
    return {"alerts": state["alerts"][-limit:]}


@app.post("/tenants/{tenant_id}/workers/start")
def start_workers(tenant_id: str, req: StartReq):
    if tenant_id not in TENANT_TOPICS:
        raise HTTPException(status_code=400, detail=f"Unknown tenant_id. Use one of {list(TENANT_TOPICS.keys())}")

    client = docker.from_env()
    network = get_compose_network_name()

    worker_id = f"{tenant_id}-worker"
    container_name = f"streamingestworker-{tenant_id}"

    cmd = [
        "python", "streamingestworker.py",
        "--tenant", tenant_id,
        "--kafka", KAFKA_BOOTSTRAP,
        "--cassandra", CASSANDRA_HOST,
    ]

    # If container exists, start it; else create it
    try:
        c = client.containers.get(container_name)
        c.reload()
        if c.status != "running":
            c.start()
        cid = c.id
    except docker.errors.NotFound:
        c = client.containers.run(
            IMAGE_NAME,
            name=container_name,
            command=cmd,
            detach=True,
            network=network,
            restart_policy={"Name": "unless-stopped"},
            # IMPORTANT: pass monitor settings to worker so it can report KPIs
            environment={
                "MONITOR_URL": MONITOR_URL,
                "REPORT_WINDOW_SEC": REPORT_WINDOW_SEC,
            },
        )
        cid = c.id

    state = load_state()
    state["workers"].setdefault(tenant_id, {})
    state["workers"][tenant_id][worker_id] = {
        "container_name": container_name,
        "container_id": cid,
        "status": "running",
        "started_at": int(time.time()),
        "kafka": KAFKA_BOOTSTRAP,
        "cassandra": CASSANDRA_HOST,
        "monitor_url": MONITOR_URL,
    }
    save_state(state)

    return {"tenant": tenant_id, "started": [{"worker_id": worker_id, "container": container_name}]}


@app.post("/tenants/{tenant_id}/workers/stop")
def stop_workers(tenant_id: str, req: StopReq):
    client = docker.from_env()
    container_name = f"streamingestworker-{tenant_id}"

    try:
        c = client.containers.get(container_name)
        c.reload()
        if c.status == "running":
            c.stop(timeout=5)
        stopped = True
    except docker.errors.NotFound:
        stopped = False

    state = load_state()
    wid = f"{tenant_id}-worker"
    if tenant_id in state.get("workers", {}) and wid in state["workers"][tenant_id]:
        state["workers"][tenant_id][wid]["status"] = "stopped"
        state["workers"][tenant_id][wid]["stopped_at"] = int(time.time())
        save_state(state)

    return {"tenant": tenant_id, "stopped": stopped, "container": container_name}


@app.post("/monitor/alerts")
def receive_alert(a: MonitorAlert):
    """
    Called by mysimbdp-streamingestmonitor when KPIs breach a threshold.
    For the assignment, receiving + recording the alert is enough.
    Optionally, we can trigger an operational action (restart).
    """
    state = load_state()
    state["alerts"].append(a.model_dump())
    save_state(state)

    # Optional action: restart worker on severe issue
    # (Keep it simple: only restart if suggested_action == "restart")
    action_taken = None
    if (a.suggested_action or "").lower() == "restart":
        client = docker.from_env()
        container_name = f"streamingestworker-{a.tenant_id}"
        try:
            c = client.containers.get(container_name)
            c.reload()
            c.restart(timeout=5)
            action_taken = f"restarted {container_name}"
        except docker.errors.NotFound:
            action_taken = f"worker container {container_name} not found"

    return {"ok": True, "action_taken": action_taken}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)