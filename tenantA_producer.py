import json
import random
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests
from confluent_kafka import Producer


# ----------------------------
# Config
# ----------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "tenantA.bronze.raw"

# Prefer data.sensor.community, but fall back if DNS/network issues happen
URL_PRIMARY = "https://data.sensor.community/static/v2/data.json"
URL_FALLBACK = "https://api.sensor.community/static/v2/data.json"

HEADERS = {
    # They request a User-Agent
    "User-Agent": "airq-streaming-platform/0.1 (contact: sabeeh.waqas@aalto.fi)"
}

POLL_INTERVAL_SEC = 2               # how often we poll the API
FETCH_TIMEOUT_SEC = 20              # HTTP timeout
MAX_FETCH_ATTEMPTS = 5              # retries before giving up

# De-dupe settings (avoid re-sending same sensor+timestamp repeatedly)
DEDUP_MAX_SIZE = 5000


# ----------------------------
# Helpers: HTTP fetch
# ----------------------------
def fetch_random_object() -> Dict[str, Any]:
    """
    Fetch the list of measurements and return a random object.
    Retries with backoff; falls back to api.sensor.community if needed.
    """
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
                # Try next URL (fallback) or next attempt

        backoff = 2 * attempt
        print(f"⚠️ Fetch failed (attempt {attempt}/{MAX_FETCH_ATTEMPTS}). "
              f"Retrying in {backoff}s... ({last_err})")
        time.sleep(backoff)

    raise RuntimeError(f"Failed to fetch data after retries. Last error: {last_err}")


def sensordatavalues_to_map(obj: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert sensordatavalues list -> dict {value_type: value}.
    Values are strings in the API; keep them as strings here (bronze-friendly),
    but we will also parse key metrics into floats for convenience.
    """
    out: Dict[str, str] = {}
    for item in obj.get("sensordatavalues", []) or []:
        vt = item.get("value_type")
        v = item.get("value")
        if vt is not None and v is not None:
            out[str(vt)] = str(v)
    return out


def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


# ----------------------------
# Tenant A message structure
# (flat + analytics friendly)
# ----------------------------
def build_tenantA_event(obj: Dict[str, Any]) -> Dict[str, Any]:
    loc = obj.get("location", {}) or {}
    sensor = obj.get("sensor", {}) or {}
    sensor_type = (sensor.get("sensor_type") or {}) if isinstance(sensor.get("sensor_type"), dict) else {}
    vals = sensordatavalues_to_map(obj)

    event: Dict[str, Any] = {
        "tenant_id": "tenantA",
        "event_id": obj.get("id"),
        "timestamp": obj.get("timestamp"),  # keep as string for bronze
        "sampling_rate": obj.get("sampling_rate"),

        "location_id": loc.get("id"),
        "lat": safe_float(loc.get("latitude")),
        "lon": safe_float(loc.get("longitude")),
        "alt": safe_float(loc.get("altitude")),
        "country": loc.get("country"),
        "indoor": loc.get("indoor"),

        "sensor_id": sensor.get("id"),
        "pin": sensor.get("pin"),
        "sensor_type": sensor_type.get("name"),
        "sensor_mfg": sensor_type.get("manufacturer"),

        # Common particulate measures (if present)
        "pm10_P1": safe_float(vals.get("P1")),
        "pm2_5_P2": safe_float(vals.get("P2")),

        # Keep raw map for traceability
        "raw_values": vals,

        "source": "sensor.community/static/v2/data.json",
        "ingest_ts_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    return event


# ----------------------------
# Kafka producer plumbing
# ----------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    # Uncomment for verbose success logging:
    # else:
    #     print(f"✅ Delivered to {msg.topic()} [partition={msg.partition()}] offset={msg.offset()}")


def make_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        # Good defaults for dev reliability:
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "linger.ms": 20,
        "batch.num.messages": 1000,
    })


def choose_partition_key(event: Dict[str, Any]) -> str:
    """
    Key determines partition. We want stable keys for ordering per sensor/location.
    """
    key = event.get("sensor_id") or event.get("location_id") or event.get("event_id")
    return str(key)


def dedupe_key(event: Dict[str, Any]) -> Tuple[Any, Any]:
    """
    Used to avoid sending duplicate sensor records over and over.
    """
    return (event.get("sensor_id"), event.get("timestamp"))


# ----------------------------
# Main loop
# ----------------------------
def main():
    p = make_kafka_producer()
    seen = set()

    print(f"==> TenantA producer started. Kafka={KAFKA_BOOTSTRAP}, topic={TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            obj = fetch_random_object()
            event = build_tenantA_event(obj)

            # De-dupe (avoid repeating same sensor+timestamp)
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

            # Serve delivery callbacks
            p.poll(0)

            print(f"TenantA -> topic={TOPIC} key={key} ts={event.get('timestamp')} sensor_id={event.get('sensor_id')}")
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n==> Stopping TenantA producer...")

    finally:
        # Ensure buffered messages are delivered
        p.flush(10)
        print("✅ TenantA producer stopped.")


if __name__ == "__main__":
    main()