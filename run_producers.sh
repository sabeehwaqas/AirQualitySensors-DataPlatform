#!/usr/bin/env bash
# ------------------------------------------------------------
# run_producers.sh
# - Starts tenantA and tenantB Python producers in the background
# - Saves logs to ./logs/
# - Saves process IDs (PIDs) so you can stop them later
# ------------------------------------------------------------

set -euo pipefail

# Directory for logs and PID files
mkdir -p logs

echo "==> Starting tenant producers (background)..."

# Start Tenant A producer in the background and log output
# nohup keeps it running even if the terminal closes
nohup python3 tenantA_producer.py > logs/tenantA_producer.log 2>&1 &
echo $! > logs/tenantA_producer.pid
echo "✅ Tenant A producer started (PID=$(cat logs/tenantA_producer.pid))"

# Start Tenant B producer in the background and log output
nohup python3 tenantB_producer.py > logs/tenantB_producer.log 2>&1 &
echo $! > logs/tenantB_producer.pid
echo "✅ Tenant B producer started (PID=$(cat logs/tenantB_producer.pid))"

echo ""
echo "Logs:"
echo "  tail -f logs/tenantA_producer.log"
echo "  tail -f logs/tenantB_producer.log"
echo ""
echo "To stop producers:"
echo "  kill \$(cat logs/tenantA_producer.pid) \$(cat logs/tenantB_producer.pid)"