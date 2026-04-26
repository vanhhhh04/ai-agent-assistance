#!/usr/bin/env bash
# Shows end-to-end pipeline health at a glance:
#   1. Which sim processes are running
#   2. PostgreSQL row counts (ERP source of truth)
#   3. Kafka topic message counts (what reached Kafka)
#   4. NiFi processor status (ingestion health)
#   5. Debezium connector status (ERP CDC health)
#
# No `set -e`: each section is independent — a failure in one shouldn't
# hide the output of the others.

cd "$(dirname "$0")/.."

# Pick a working Python — Windows Git Bash needs `python`, Linux/macOS `python3`.
if command -v python3 >/dev/null 2>&1 && python3 --version >/dev/null 2>&1; then
  PY=python3
elif [[ -x venv/Scripts/python.exe ]]; then
  PY=venv/Scripts/python.exe
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  PY=python3
fi

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'

echo "${B}=== SIMULATOR PROCESSES ===${N}"
# data-source container has no ps/pgrep; scan /proc/*/cmdline instead.
# Only report `python ... sim_XXX.py` — skip bash wrappers and this script itself.
docker exec data-source bash -c '
  found=0
  for pid in /proc/[0-9]*/cmdline; do
    [ -r "$pid" ] || continue
    cmd=$(tr "\0" " " < "$pid" 2>/dev/null)
    case "$cmd" in
      python*sim_warehouse.py*|python*sim_erp.py*|python*sim_payment.py*)
        p=$(echo "$pid" | sed "s|/proc/||;s|/cmdline||")
        echo "  pid=$p  $cmd"
        found=1
        ;;
    esac
  done
  if [ $found -eq 0 ]; then echo "  (none running)"; fi
  exit 0
'
echo ""

echo "${B}=== POSTGRES ROW COUNTS (ERP source) ===${N}"
docker exec postgres psql -U postgres -d ecommerce -t -c "
  SELECT 'customers:  ' || COUNT(*) FROM customers UNION ALL
  SELECT 'orders:     ' || COUNT(*) FROM orders UNION ALL
  SELECT 'order_items:' || COUNT(*) FROM order_items UNION ALL
  SELECT 'products:   ' || COUNT(*) FROM products UNION ALL
  SELECT 'payments:   ' || COUNT(*) FROM payments UNION ALL
  SELECT 'shipping:   ' || COUNT(*) FROM shipping;
" 2>/dev/null | sed 's/^/  /'
echo ""

echo "${B}=== KAFKA TOPIC MESSAGE COUNTS ===${N}"
for topic in erp.public.customers erp.public.orders erp.public.order_items erp.public.coupons warehouse.events payment.events; do
  count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list kafka:29092 --topic "${topic}" 2>/dev/null \
      | awk -F: '{sum+=$NF} END {print sum+0}')
  printf "  %-30s %s\n" "${topic}" "${count}"
done
echo ""

echo "${B}=== NIFI RUNNING PROCESSORS ===${N}"
TOKEN=$(curl -sk -d "username=admin&password=adminadminadmin" \
  https://localhost:8443/nifi-api/access/token 2>/dev/null)
if [[ -n "${TOKEN}" ]]; then
  curl -sk -H "Authorization: Bearer ${TOKEN}" \
    "https://localhost:8443/nifi-api/flow/process-groups/root" 2>/dev/null \
    | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
procs = d['processGroupFlow']['flow']['processors']
for p in procs:
    s = p['component']
    stats = p['status']['aggregateSnapshot']
    print(f\"  {s['name']:30s} state={s['state']:10s} in={stats.get('flowFilesIn',0):5d} out={stats.get('flowFilesOut',0):5d}\")
" 2>/dev/null || echo "  (failed to parse NiFi response)"
else
  echo "  ${R}NiFi not reachable${N}"
fi
echo ""

echo "${B}=== DEBEZIUM CONNECTOR STATUS ===${N}"
curl -sf http://localhost:8083/connectors/erp-postgres-cdc/status 2>/dev/null \
  | "$PY" -m json.tool 2>/dev/null \
  | sed 's/^/  /' || echo "  ${R}(connector not registered or kafka-connect down)${N}"
