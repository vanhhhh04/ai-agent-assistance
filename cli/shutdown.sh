#!/usr/bin/env bash
# Clean shutdown — stops simulators first, then containers gracefully.
# ALWAYS run this before rebooting your laptop to prevent Kafka data loss.

cd "$(dirname "$0")/.."

echo "▶ Stopping simulators..."
bash cli/sim-stop.sh 2>/dev/null

echo "▶ Stopping all containers gracefully..."
docker compose stop
echo "  done."

echo ""
echo "Safe to power off / reboot now."
echo "When you come back, run:  bash cli/startup.sh"
