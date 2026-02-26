import json
import sys
import requests

URL = "https://data.sensor.community/static/v2/data.json"

def fetch_one_object(url: str) -> dict:
    headers = {
        # IMPORTANT: they require a User-Agent header
        "User-Agent": "your-project-name/0.1 (contact: youremail@example.com)"
    }
    # keep it strict + safe for testing
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()

    data = r.json()

    # v1/data.json returns a list of measurements (each item is a JSON object)
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"Unexpected payload type/empty payload: {type(data)}")

    return data[0]

if __name__ == "__main__":
    try:
        obj = fetch_one_object(URL)
        print("Fetched 1 object ✅")
        print(json.dumps(obj, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Fetch failed ❌: {e}", file=sys.stderr)
        sys.exit(1)