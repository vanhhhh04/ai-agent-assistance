#!/usr/bin/env bash
# Recover Kafka after a "NodeExistsException: /brokers/ids/1" startup crash.
#
# Why this happens:
#   ZooKeeper persists ephemeral session state in ./data/zookeeper/.
#   When Docker Desktop / the host reboots, ZK reloads the snapshot —
#   broker registration nodes from the previous session may still be there.
#   Kafka tries to register broker.id=1, sees the node exists, and exits.
#
# This is a focused fix that does NOT touch the rest of the stack
# (no sims kill, no NiFi reset, no schema rebuild). Idempotent.
#
# Usage:  bash cli/kafka-recover.sh

cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'
step() { echo ""; echo "${B}▶ $1${N}"; }
ok()   { echo "  ${G}✓${N} $1"; }
warn() { echo "  ${Y}!${N} $1"; }
err()  { echo "  ${R}✗${N} $1"; }

step "1. Stop Kafka stack"
docker compose stop kafka-connect kafka zookeeper 2>&1 | grep -E "Stopped|Stopping" | sed 's/^/  /'
ok "stopped"

step "2. Wipe ZooKeeper + Kafka cached state"
# Use an Alpine helper container to delete files owned by root inside the
# bind-mount — Git Bash sudo won't reach those.
docker run --rm -v "$(pwd)/data:/data" alpine sh -c \
  "rm -rf /data/zookeeper/data/* /data/zookeeper/log/* /data/kafka/*" 2>/dev/null
ok "ZK ephemerals + Kafka log dirs cleared"

step "3. Start ZooKeeper"
docker compose start zookeeper > /dev/null
for i in $(seq 1 15); do
  if docker exec zookeeper bash -c "echo ruok | nc -w 2 localhost 2181" 2>/dev/null | grep -q imok; then
    ok "zookeeper ready"; break
  fi
  sleep 2
  [[ $i -eq 15 ]] && warn "zookeeper not responding to ruok after 30s — may still come up"
done

step "4. Start Kafka broker"
docker compose start kafka > /dev/null
for i in $(seq 1 30); do
  if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    ok "kafka broker registered"; break
  fi
  sleep 2
  if [[ $i -eq 30 ]]; then
    err "kafka did not become ready in 60s — check 'docker logs kafka --tail 50'"
    exit 1
  fi
done

step "5. Start Kafka Connect (Debezium worker)"
docker compose start kafka-connect > /dev/null
for i in $(seq 1 30); do
  if curl -sf http://localhost:8083/ > /dev/null 2>&1; then
    ok "kafka-connect REST ready"; break
  fi
  sleep 3
  [[ $i -eq 30 ]] && warn "kafka-connect REST not responding after 90s"
done

step "6. Verify Debezium connector"
status=$(curl -sf http://localhost:8083/connectors/erp-postgres-cdc/status 2>/dev/null)
if [[ -z "${status}" ]]; then
  warn "connector erp-postgres-cdc not registered — registering..."
  bash nifi/init_debezium.sh > /dev/null 2>&1 && ok "registered" || err "registration failed"
else
  conn_state=$(echo "${status}" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
  task_state=$(echo "${status}" | grep -o '"state":"[^"]*"' | sed -n 2p | cut -d'"' -f4)
  if [[ "${conn_state}" == "RUNNING" && "${task_state}" == "RUNNING" ]]; then
    ok "connector + task RUNNING"
  else
    warn "connector=${conn_state} task=${task_state} — restarting..."
    curl -sX POST http://localhost:8083/connectors/erp-postgres-cdc/restart > /dev/null
    ok "restart issued"
  fi
fi

echo ""
echo "${B}▶ Kafka stack recovered.${N}"
echo "  Verify topics:  http://localhost:8888"
echo "  Connector:      curl http://localhost:8083/connectors/erp-postgres-cdc/status"
