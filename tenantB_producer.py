import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from confluent_kafka import Producer


# ----------------------------
# Config
# ----------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "tenantB.bronze.raw"

URL_PRIMARY = "https://data.sensor.community/static/v2/data.json"
URL_FALLBACK = "https://api.sensor.community/static/v2/data.json"

HEADERS = {
    "User-Agent": "airq-streaming-platform/0.1 (contact: sabeeh.waqas@aalto.fi)"
}

POLL_INTERVAL_SEC = 2
FETCH_TIMEOUT_SEC = 20
MAX_FETCH_ATTEMPTS = 5

DEDUP_MAX_SIZE = 5000


# ----------------------------
# Helpers: HTTP fetch
# ----------------------------
def fetch_random_object() -> Dict[str, Any]:
    urls = [URL_PRIMARY, URL_FALLBACK]
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        for url in urls:
            try:
                r = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT_SEC)
                r.raise_for_status()
                data = r.json()

                if not isinstance(data, list) or not data:
                    raise ValueError("Unexpected payload (not a non-empty list)")

                return random.choice(data)

            except (requests.exceptions.RequestException, ValueError) as e:
                last_err = e

        backoff = 2 * attempt
        print(f"⚠️ Fetch failed (attempt {attempt}/{MAX_FETCH_ATTEMPTS}). "
              f"Retrying in {backoff}s... ({last_err})")
        time.sleep(backoff)

    raise RuntimeError(f"Failed to fetch data after retries. Last error: {last_err}")


def sensordatavalues_to_map(obj: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in obj.get("sensordatavalues", []) or []:
        vt = item.get("value_type")
        v = item.get("value")
        if vt is not None and v is not None:
            out[str(vt)] = str(v)
    return out


def to_epoch_utc(ts_str: Optional[str]) -> Optional[int]:
    """
    API timestamp format example: "2026-02-26 22:05:03"
    Treat it as UTC for consistency (good enough for platform ingestion demos).
    """
    if not ts_str:
        return None
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


# ----------------------------
# Tenant B message structure
# (more nested + metadata envelope)
# ----------------------------
def build_tenantB_event(obj: Dict[str, Any]) -> Dict[str, Any]:
    loc = obj.get("location", {}) or {}
    sensor = obj.get("sensor", {}) or {}
    stype = (sensor.get("sensor_type") or {}) if isinstance(sensor.get("sensor_type"), dict) else {}
    vals = sensordatavalues_to_map(obj)

    event: Dict[str, Any] = {
        "tenant_id": "tenantB",
        "meta": {
            "event_id": obj.get("id"),
            "sampling_rate": obj.get("sampling_rate"),
            "timestamp_str": obj.get("timestamp"),
            "ts_epoch": to_epoch_utc(obj.get("timestamp")),
            "source": {"provider": "sensor.community", "endpoint": "static/v2/data.json"},
            "ingest_ts_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
        "geo": {
            "location_id": loc.get("id"),
            "lat": loc.get("latitude"),
            "lon": loc.get("longitude"),
            "alt": loc.get("altitude"),
            "country": loc.get("country"),
            "indoor": loc.get("indoor"),
            "exact_location": loc.get("exact_location"),
        },
        "device": {
            "sensor_id": sensor.get("id"),
            "pin": sensor.get("pin"),
            "type": {"name": stype.get("name"), "manufacturer": stype.get("manufacturer")},
        },
        "measurements": vals,
        "tags": ["bronze", "telemetry"],
    }
    return event


# ----------------------------
# Kafka producer plumbing
# ----------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")


def make_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "linger.ms": 20,
        "batch.num.messages": 1000,
    })


def choose_partition_key(event: Dict[str, Any]) -> str:
    """
    Choose a stable key for partitioning.
    For tenantB we prefer location_id, then sensor_id.
    """
    geo = event.get("geo") or {}
    dev = event.get("device") or {}
    meta = event.get("meta") or {}

    key = geo.get("location_id") or dev.get("sensor_id") or meta.get("event_id")
    return str(key)


def dedupe_key(event: Dict[str, Any]) -> Tuple[Any, Any]:
    """
    De-dupe by (location_id, timestamp_str) for tenantB.
    """
    geo = event.get("geo") or {}
    meta = event.get("meta") or {}
    return (geo.get("location_id"), meta.get("timestamp_str"))


# ----------------------------
# Main loop
# ----------------------------
def main():
    p = make_kafka_producer()
    seen = set()

    print(f"==> TenantB producer started. Kafka={KAFKA_BOOTSTRAP}, topic={TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            obj = fetch_random_object()
            event = build_tenantB_event(obj)

            dk = dedupe_key(event)
            if dk in seen:
                time.sleep(0.2)
                continue
            seen.add(dk)
            if len(seen) > DEDUP_MAX_SIZE:
                seen.clear()

            key = choose_partition_key(event)

            p.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                callback=delivery_report,
            )

            p.poll(0)

            geo = event.get("geo") or {}
            dev = event.get("device") or {}
            meta = event.get("meta") or {}
            print(f"TenantB -> topic={TOPIC} key={key} ts={meta.get('timestamp_str')} "
                  f"location_id={geo.get('location_id')} sensor_id={dev.get('sensor_id')}")
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n==> Stopping TenantB producer...")

    finally:
        p.flush(10)
        print("✅ TenantB producer stopped.")


if __name__ == "__main__":
    main()