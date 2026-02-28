# streamingestmanager.py
import json
import os
import time
from typing import Dict, Any, Optional, List

import docker
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

APP_PORT = 8001

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
    worker_id: Optional[str] = None  # if None => stop all for tenant


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        return {"workers": {}}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_compose_network_name() -> str:
    """
    Auto-detect network by inspecting the manager container's networks.
    The manager runs inside the compose network; we reuse that network
    when launching workers so they can reach broker/cassandra by service name.
    """
    # container hostname is container id-ish in docker, but easiest:
    # use docker to find this container by name and read its networks.
    client = docker.from_env()
    me = client.containers.get(os.uname().nodename)
    nets = list(me.attrs["NetworkSettings"]["Networks"].keys())
    if not nets:
        raise RuntimeError("Manager is not attached to a docker network.")
    return nets[0]


def is_container_running(client: docker.DockerClient, name: str) -> bool:
    try:
        c = client.containers.get(name)
        c.reload()
        return c.status == "running"
    except docker.errors.NotFound:
        return False


def safe_remove_container(client: docker.DockerClient, name: str) -> None:
    try:
        c = client.containers.get(name)
        c.remove(force=True)
    except docker.errors.NotFound:
        return


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/tenants/{tenant_id}/workers")
def list_workers(tenant_id: str):
    state = load_state()
    workers = state["workers"].get(tenant_id, {})
    return {"tenant": tenant_id, "workers": workers}


@app.post("/tenants/{tenant_id}/workers/start")
def start_workers(tenant_id: str, req: StartReq):
    if tenant_id not in TENANT_TOPICS:
        raise HTTPException(status_code=400, detail=f"Unknown tenant_id. Use one of {list(TENANT_TOPICS.keys())}")

    client = docker.from_env()
    network = get_compose_network_name()

    # Fixed container + worker IDs (persistent)
    worker_id = f"{tenant_id}-worker"
    container_name = f"streamingestworker-{tenant_id}"

    # Worker command
    cmd = [
         "python", "streamingestworker.py",
         "--tenant", tenant_id,
         "--kafka", KAFKA_BOOTSTRAP,
          "--cassandra", CASSANDRA_HOST,
        ]

    # If container exists, just start it (idempotent)
    try:
        c = client.containers.get(container_name)
        c.reload()
        if c.status != "running":
            c.start()
        status = "running"
        cid = c.id
    except docker.errors.NotFound:
        # Create it once
        c = client.containers.run(
            IMAGE_NAME,
            name=container_name,
            command=cmd,
            detach=True,
            network=network,
            restart_policy={"Name": "unless-stopped"},
        )
        status = "running"
        cid = c.id

    # Save state (optional but useful)
    state = load_state()
    state["workers"].setdefault(tenant_id, {})
    state["workers"][tenant_id][worker_id] = {
        "container_name": container_name,
        "container_id": cid,
        "status": status,
        "started_at": int(time.time()),
        "kafka": KAFKA_BOOTSTRAP,
        "cassandra": CASSANDRA_HOST,
    }
    save_state(state)

    return {"tenant": tenant_id, "started": [{"worker_id": worker_id, "container": container_name}]}


@app.post("/tenants/{tenant_id}/workers/stop")
def stop_workers(tenant_id: str, req: StopReq):
    client = docker.from_env()

    # Fixed container name (persistent)
    container_name = f"streamingestworker-{tenant_id}"

    try:
        c = client.containers.get(container_name)
        c.reload()
        if c.status == "running":
            c.stop(timeout=5)
        stopped = True
    except docker.errors.NotFound:
        stopped = False

    # Update state (optional)
    state = load_state()
    if tenant_id in state.get("workers", {}):
        wid = f"{tenant_id}-worker"
        if wid in state["workers"][tenant_id]:
            state["workers"][tenant_id][wid]["status"] = "stopped"
            state["workers"][tenant_id][wid]["stopped_at"] = int(time.time())
            save_state(state)

    return {"tenant": tenant_id, "stopped": stopped, "container": container_name}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)