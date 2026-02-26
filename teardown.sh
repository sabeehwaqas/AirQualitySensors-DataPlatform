#!/usr/bin/env bash
# ------------------------------------------------------------
# teardown.sh
# - Stops the tenant producers if running (using saved PID files)
# - Stops Kafka via docker compose down
# - Optionally removes volumes (use --volumes flag)
# ------------------------------------------------------------

set -euo pipefail

REMOVE_VOLUMES="${1:-}"  # pass "--volumes" if you want to delete kafka data

stop_if_running() {
  local pid_file="$1"
  local name="$2"

  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}")"

    # Check if process is still running
    if kill -0 "${pid}" >/dev/null 2>&1; then
      echo "==> Stopping ${name} (PID=${pid})..."
      kill "${pid}"
      echo "✅ Stopped ${name}"
    else
      echo "==> ${name} PID file exists but process is not running (PID=${pid})"
    fi

    # Clean up pid file
    rm -f "${pid_file}"
  else
    echo "==> No PID file for ${name} (nothing to stop)"
  fi
}

# Stop producers (if started via run_producers.sh)
stop_if_running "logs/tenantA_producer.pid" "Tenant A producer"
stop_if_running "logs/tenantB_producer.pid" "Tenant B producer"

echo "==> Stopping docker compose services..."
if [[ "${REMOVE_VOLUMES}" == "--volumes" ]]; then
  # This removes volumes (Kafka data) too
  docker compose down -v
  echo "✅ Docker services stopped (volumes removed)."
else
  docker compose down
  echo "✅ Docker services stopped."
  echo "Tip: run './teardown.sh --volumes' if you want to wipe Kafka data."
fi