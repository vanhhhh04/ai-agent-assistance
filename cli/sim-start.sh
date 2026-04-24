#!/usr/bin/env bash
# Starts the 3 data-source simulators in the background inside the data-source
# container. Each one's stdout/stderr is redirected to /var/log/sim_<name>.log
# (mounted to ./logs/ on the host so you can tail from outside Docker).
#
# Usage:
#   bash cli/sim-start.sh            # start all 3
#   bash cli/sim-start.sh warehouse  # start only one
#
# After starting, tail with:  bash cli/sim-logs.sh <warehouse|erp|payment>

set -e
cd "$(dirname "$0")/.."

# Ensure the host log directory exists (docker-compose will mount it)
mkdir -p ./logs

start_sim() {
  local name="$1"
  local script="sim_${name}.py"

  # Kill any existing process with the same script name
  docker exec data-source bash -c "pkill -f '${script}' || true"
  sleep 1

  echo "→ starting ${script} (logs: ./logs/sim_${name}.log)"
  docker exec -d data-source bash -c \
    "python -u ${script} > /app/logs/sim_${name}.log 2>&1"
}

TARGET="${1:-all}"

if [[ "${TARGET}" == "all" ]]; then
  start_sim warehouse
  sleep 2       # warehouse must bootstrap products first
  start_sim erp
  sleep 1
  start_sim payment
elif [[ "${TARGET}" =~ ^(warehouse|erp|payment)$ ]]; then
  start_sim "${TARGET}"
else
  echo "Unknown target: ${TARGET}"
  echo "Usage: bash cli/sim-start.sh [warehouse|erp|payment|all]"
  exit 1
fi

echo ""
echo "All started. Trace with:"
echo "  bash cli/sim-logs.sh warehouse"
echo "  bash cli/sim-logs.sh erp"
echo "  bash cli/sim-logs.sh payment"
echo "  bash cli/pipeline-status.sh   # overall pipeline health"
