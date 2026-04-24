#!/usr/bin/env bash
# Stops all running simulators inside data-source.
set -e

echo "Stopping simulators…"
docker exec data-source bash -c "pkill -f 'sim_warehouse.py' || true"
docker exec data-source bash -c "pkill -f 'sim_erp.py' || true"
docker exec data-source bash -c "pkill -f 'sim_payment.py' || true"
echo "Stopped."
