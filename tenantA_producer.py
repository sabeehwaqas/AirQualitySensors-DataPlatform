import json
import time
import requests
from confluent_kafka import Producer

URL = "https://data.sensor.community/static/v2/data.json"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "tenantA.bronze.raw"

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

def extract_values(obj: dict) -> dict:
    # Convert sensordatavalues list -> dict {value_type: value}
    vals = {}
    for item in obj.get("sensordatavalues", []):
        vt = item.get("value_type")
        v = item.get("value")
        if vt is not None:
            vals[vt] = v
    return vals

def build_tenantA_event(obj: dict) -> dict:
    loc = obj.get("location", {}) or {}
    sensor = obj.get("sensor", {}) or {}
    vals = extract_values(obj)

    # Tenant A schema: flat + “bronze raw but clean”
    return {
        "tenant_id": "tenantA",
        "event_id": obj.get("id"),
        "timestamp": obj.get("timestamp"),  # keep string for bronze
        "location_id": loc.get("id"),
        "lat": float(loc["latitude"]) if loc.get("latitude") else None,
        "lon": float(loc["longitude"]) if loc.get("longitude") else None,
        "country": loc.get("country"),
        "sensor_id": sensor.get("id"),
        "sensor_type": (sensor.get("sensor_type") or {}).get("name"),
        "pm10_P1": float(vals["P1"]) if "P1" in vals else None,
        "pm2_5_P2": float(vals["P2"]) if "P2" in vals else None,
        "raw_values": vals,  # keep for traceability
        "source": "sensor.community/v2"
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    # else: print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    while True:
        obj = fetch_one(URL)
        event = build_tenantA_event(obj)

        # Partition key: sensor_id (stable)
        key = str(event.get("sensor_id") or event.get("location_id") or event.get("event_id"))

        p.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            callback=delivery_report,
        )
        p.poll(0)
        print(f"TenantA -> Kafka topic={TOPIC} key={key}")
        time.sleep(2)  # polling interval

if __name__ == "__main__":
    main()