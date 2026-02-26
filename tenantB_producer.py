import json
import time
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer

URL = "https://data.sensor.community/static/v2/data.json"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "tenantB.bronze.raw"

HEADERS = {
    "User-Agent": "airq-streaming-platform/0.1 (contact: you@example.com)"
}

def fetch_one(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        raise ValueError("Unexpected payload from API")
    return data[0]

def list_to_map(obj: dict) -> dict:
    out = {}
    for item in obj.get("sensordatavalues", []):
        vt = item.get("value_type")
        v = item.get("value")
        if vt:
            out[vt] = v
    return out

def to_epoch(ts_str: str) -> int | None:
    # Input like "2026-02-26 22:05:03"
    if not ts_str:
        return None
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def build_tenantB_event(obj: dict) -> dict:
    vals = list_to_map(obj)
    loc = obj.get("location", {}) or {}
    sensor = obj.get("sensor", {}) or {}
    stype = sensor.get("sensor_type", {}) or {}

    # Tenant B schema: more nested + includes computed/derived fields
    return {
        "tenant_id": "tenantB",
        "meta": {
            "event_id": obj.get("id"),
            "ts_epoch": to_epoch(obj.get("timestamp")),
            "sampling_rate": obj.get("sampling_rate"),
            "source": {"provider": "sensor.community", "endpoint": "static/v2/data.json"},
        },
        "device": {
            "sensor_id": sensor.get("id"),
            "pin": sensor.get("pin"),
            "type": {"name": stype.get("name"), "mfg": stype.get("manufacturer")},
        },
        "geo": {
            "location_id": loc.get("id"),
            "lat": loc.get("latitude"),
            "lon": loc.get("longitude"),
            "alt": loc.get("altitude"),
            "country": loc.get("country"),
            "indoor": loc.get("indoor"),
        },
        "measurements": vals,
        "tags": ["bronze", "telemetry"],
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    while True:
        obj = fetch_one(URL)
        event = build_tenantB_event(obj)

        # Partition key: location_id (stable)
        loc_id = (event.get("geo") or {}).get("location_id")
        key = str(loc_id or (event.get("device") or {}).get("sensor_id") or (event.get("meta") or {}).get("event_id"))

        p.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            callback=delivery_report,
        )
        p.poll(0)
        print(f"TenantB -> Kafka topic={TOPIC} key={key}")
        time.sleep(2)

if __name__ == "__main__":
    main()