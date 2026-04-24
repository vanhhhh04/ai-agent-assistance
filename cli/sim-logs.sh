#!/usr/bin/env bash
# Tail the log of one simulator, live.
# Usage: bash cli/sim-logs.sh <warehouse|erp|payment>

set -e
cd "$(dirname "$0")/.."

TARGET="${1:-}"
if [[ ! "${TARGET}" =~ ^(warehouse|erp|payment)$ ]]; then
  echo "Usage: bash cli/sim-logs.sh <warehouse|erp|payment>"
  exit 1
fi

LOGFILE="./logs/sim_${TARGET}.log"
if [[ ! -f "${LOGFILE}" ]]; then
  echo "Log file not found: ${LOGFILE}"
  echo "Did you run 'bash cli/sim-start.sh' first?"
  exit 1
fi

echo "Tailing ${LOGFILE}  (Ctrl-C to stop)"
echo "─────────────────────────────────────────────────────"
tail -f "${LOGFILE}"
