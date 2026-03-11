# mysimbdp Deployment Guide

## Folder structure (what goes where)

```
mysimbdp/
├── gcp-key.json                  ← your GCP service account key (root level)
├── Dockerfile.platform
├── docker-compose.yaml
├── requirements.platform.txt
├── bootstrap.sh
├── run_producers.sh
├── logs/                         ← worker logs
├── cache/                        ← created automatically when containers start
│   ├── tenantA/pending/
│   ├── tenantA/processed/
│   ├── tenantB/pending/
│   └── tenantB/processed/
├── platform/
│   ├── batchmanager.py
│   ├── streamingestmanager.py
│   └── streamingestmonitor.py
└── tenants/
    ├── tenantA/
    │   ├── tenantA_producer.py
    │   ├── streamingestworker.py
    │   ├── silverpipeline_tenantA.py
    │   └── tenantA.yaml
    └── tenantB/
        ├── tenantB_producer.py
        ├── streamingestworker.py
        ├── silverpipeline_tenantB.py
        └── tenantB.yaml
```

---

## Before you start (one time only)

Install on your host machine:

```bash
# Docker Desktop must be running

# Python deps for the producers (they run on host, not in Docker)
pip install confluent-kafka requests

# Make scripts executable
chmod +x bootstrap.sh run_producers.sh
```

# GCP Setup

Follow the Setup-GCP-Guide.md to setup a GCP bucker as cloud cache

---

## Every fresh start (normal flow)

### 1. Wipe everything clean

Always use `-v` — without it the old Cassandra schema survives and batchmanager breaks.

```bash
docker compose down -v
docker rmi mysimbdp-platform:latest 2>/dev/null || true
```

### 2. Build the image

```bash
docker compose build --no-cache
```

### 3. Start all containers

```bash
docker compose up -d
```

Check they are all running:

```bash
docker compose ps
```

You should see: broker, cassandra, cqlsh, streamingestmanager, streamingestmonitor, batchmanager — all "Up".

### 4. Run bootstrap

This creates all Kafka topics and Cassandra tables. Wait until you see `✅ Bootstrap complete.`

```bash
./bootstrap.sh
```

### 5. Start the producers (on your host, not in Docker)

```bash
./run_producers.sh
```

Check they are working:

```bash
tail -f logs/tenantA_producer.log
tail -f logs/tenantB_producer.log
```

### 6. Start the streaming workers

```bash
curl -X POST http://localhost:8001/tenants/tenantA/workers/start \
     -H "Content-Type: application/json" -d '{}'

curl -X POST http://localhost:8001/tenants/tenantB/workers/start \
     -H "Content-Type: application/json" -d '{}'
```

Check workers are running:

```bash
docker ps | grep streamingestworker
```

### 7. Trigger silver pipelines (don't wait 5 min)

```bash
curl -X POST http://localhost:8003/tenants/tenantA/run
curl -X POST http://localhost:8003/tenants/tenantB/run
```

---

## Health checks

```bash
# Is the streaming manager up?
curl http://localhost:8001/health

# Is the monitor up?
curl http://localhost:8002/health

# Is the batchmanager up? (cassandra_ready must be true)
curl http://localhost:8003/health

# What tenants are registered?
curl http://localhost:8003/tenants
```

---

## Check data in Cassandra

Open cqlsh first:

```bash
docker exec -it cqlsh cqlsh cassandra 9042
```

Then run these queries:

```sql
-- Did bronze records arrive?
SELECT sensor_id, ingest_ts FROM tenantA_bronze.records LIMIT 10;
SELECT location_id, ingest_ts FROM tenantB_bronze.records LIMIT 10;

-- How many bronze records total?
SELECT COUNT(*) FROM tenantA_bronze.records;
SELECT COUNT(*) FROM tenantB_bronze.records;

-- Did silver records get created?
SELECT sensor_id, silver_ts, pm10, pm2_5, aqi_bucket
FROM tenantA_silver.air_quality LIMIT 10;

SELECT location_id, silver_ts, lat, lon, has_pm_data
FROM tenantB_silver.geo_measurements LIMIT 10;

-- Silver pipeline run history
SELECT tenant_id, run_id, started_at, status,
       records_loaded, elapsed_sec, extract_sec,
       transform_sec, data_size_bytes, cache_mode
FROM platform_logs.silver_pipeline_logs LIMIT 20;

-- Streaming KPIs from workers
SELECT tenant_id, ts, avg_ingest_ms, records, errors
FROM platform_logs.streaming_metrics
WHERE tenant_id = 'tenantA' LIMIT 10;

-- Current watermarks (last bronze ts processed)
SELECT * FROM platform_logs.silver_watermarks;
```

---

## Performance and stats

```bash
# Platform-wide stats (avg timings, records, cache modes)
curl http://localhost:8003/stats

# Recent pipeline run logs per tenant
curl http://localhost:8003/tenants/tenantA/logs
curl http://localhost:8003/tenants/tenantB/logs

# Alerts fired by the monitor
curl http://localhost:8001/monitor/alerts

# Worker container status
curl http://localhost:8001/tenants/tenantA/workers
curl http://localhost:8001/tenants/tenantB/workers
```

---

## Testing constraint violations (P2.4b)

**Test 1 — missing silver table:**

```bash
# Drop the table
docker exec -it cqlsh cqlsh cassandra 9042 -e \
  "DROP TABLE tenantA_silver.air_quality;"

# Trigger a run — it must be blocked
curl -X POST http://localhost:8003/tenants/tenantA/run

# Check logs — status should be constraint_violation
curl http://localhost:8003/tenants/tenantA/logs

# Restore
./bootstrap.sh
```

**Test 2 — pending dir overflow (tenantA limit is 20 files):**

```bash
# Create 21 fake files
for i in $(seq 1 21); do
  touch ./cache/tenantA/pending/fake_$i.jsonl
done

# Trigger — must be blocked
curl -X POST http://localhost:8003/tenants/tenantA/run
curl http://localhost:8003/tenants/tenantA/logs

# Cleanup
rm ./cache/tenantA/pending/fake_*.jsonl
```

**Test 3 — empty bronze table:**

```bash
# In cqlsh:
TRUNCATE tenantA_bronze.records;
DELETE FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantA';

# Trigger — must be blocked
curl -X POST http://localhost:8003/tenants/tenantA/run
curl http://localhost:8003/tenants/tenantA/logs

# Restore — restart producers and wait 30s
./run_producers.sh
```

---

## Testing under-provisioned worker (P1.3)

In `tenants/tenantA/streamingestworker.py` (or the platform copy used by the manager),
open `streamingestmanager.py` and uncomment lines 136-137:

```python
mem_limit="256m",
cpu_quota=50000,
```

Then rebuild and restart:

```bash
docker compose down -v
docker compose build --no-cache
docker compose up -d
./bootstrap.sh
./run_producers.sh
curl -X POST http://localhost:8001/tenants/tenantA/workers/start \
     -H "Content-Type: application/json" -d '{}'
```

Watch high ingest_ms in worker logs:

```bash
docker logs -f streamingestworker-tenantA
```

Check alerts fired:

```bash
curl http://localhost:8001/monitor/alerts
```

After capturing screenshots, comment the limits back out and rebuild.

---

## Testing monitor threshold alert (P1.5)

To force alerts without needing resource limits, lower the threshold in `docker-compose.yaml`:

```yaml
MAX_AVG_INGEST_MS: "5"
```

Restart monitor only (no rebuild needed):

```bash
docker compose up -d streamingestmonitor
```

Wait 10 seconds then check:

```bash
curl http://localhost:8001/monitor/alerts
```

Restore after:

```yaml
MAX_AVG_INGEST_MS: "50"
```

---

## Testing local vs GCS performance (P2.4c)

**Run 1 — local only:**

In `docker-compose.yaml` comment out:

```yaml
# GCS_BUCKET: "airqualitybdpasg"
```

```bash
docker compose up -d batchmanager

# Reset watermarks so pipeline has data to process
docker exec -it cqlsh cqlsh cassandra 9042 -e \
  "DELETE FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantA';"
docker exec -it cqlsh cqlsh cassandra 9042 -e \
  "DELETE FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantB';"

curl -X POST http://localhost:8003/tenants/tenantA/run
curl -X POST http://localhost:8003/tenants/tenantB/run
curl http://localhost:8003/tenants/tenantA/logs
```

Note `cache_mode=local` and `extract_sec`.

**Run 2 — local + GCS:**

Restore in `docker-compose.yaml`:

```yaml
GCS_BUCKET: "airqualitybdpasg"
```

```bash
docker compose up -d batchmanager

# Reset watermarks again
docker exec -it cqlsh cqlsh cassandra 9042 -e \
  "DELETE FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantA';"

curl -X POST http://localhost:8003/tenants/tenantA/run
curl http://localhost:8003/tenants/tenantA/logs
```

Note `cache_mode=local+gcs` and higher `extract_sec` (due to GCS upload time).

---

## Stop everything

```bash
# Stop producers
kill $(cat logs/tenantA_producer.pid) $(cat logs/tenantB_producer.pid)

# Stop all containers (keeps data)
docker compose down

# Stop all containers AND wipe all data
docker compose down -v
```

---

## Common problems

**batchmanager health shows `cassandra_ready: false`**
Schema columns are missing from an old volume. Run:

```bash
chmod +x fix_schema.sh && ./fix_schema.sh
```

Or just do a full fresh start with `docker compose down -v`.

**No rows in bronze tables even though workers are running**

```bash
docker logs streamingestworker-tenantA
```

If you see "Cassandra not ready", stop and restart the worker:

```bash
curl -X POST http://localhost:8001/tenants/tenantA/workers/stop \
     -H "Content-Type: application/json" -d '{}'
curl -X POST http://localhost:8001/tenants/tenantA/workers/start \
     -H "Content-Type: application/json" -d '{}'
```

**Silver pipeline keeps showing `no_new_data`**
The watermark is stuck. Reset it:

```bash
docker exec -it cqlsh cqlsh cassandra 9042 -e \
  "DELETE FROM platform_logs.silver_watermarks WHERE tenant_id = 'tenantA';"
```

**GCS upload failing**

```bash
# Check key file exists
ls -la gcp-key.json

# Test credentials
docker exec batchmanager python -c \
  "from google.cloud import storage; print(list(storage.Client().list_buckets()))"
```
