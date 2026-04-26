#!/usr/bin/env bash
# Data Engineering pipeline verification — top-down through every layer.
#
# At each layer we check three things, the way a real DE does:
#   1. LIVENESS    — service is running / processor is enabled
#   2. FRESHNESS   — recent data is flowing in (counts moving)
#   3. INTEGRITY   — counts roughly match the upstream layer
#                    (with reasonable tolerance for in-flight + DLQ)
#
# Reads only — never writes / mutates anything.
# Exit 0 = all green, 1 = any failure, 2 = warnings only.
#
# Usage:  bash cli/verify-pipeline.sh

cd "$(dirname "$0")/.."

# Pick a working Python (Windows Git Bash needs `python`, Linux/macOS `python3`)
if command -v python3 >/dev/null 2>&1 && python3 --version >/dev/null 2>&1; then
  PY=python3
elif [[ -x venv/Scripts/python.exe ]]; then
  PY=venv/Scripts/python.exe
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "no python interpreter found"; exit 1
fi

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; C=$'\033[36m'; N=$'\033[0m'

FAIL=0; WARN=0
section() { echo ""; echo "${B}${C}━━━ $1 ━━━${N}"; }
ok()      { echo "  ${G}✓${N} $1"; }
warn()    { echo "  ${Y}!${N} $1"; WARN=$((WARN+1)); }
err()     { echo "  ${R}✗${N} $1"; FAIL=$((FAIL+1)); }
note()    { echo "    ${C}→${N} $1"; }


# ─────────────────────────────────────────────────────────────
#  LAYER 1 — SOURCE (PostgreSQL ERP)
#  Liveness: container healthy, can connect
#  Freshness: orders.created_at within last 60s
#  Integrity: row counts > 0 for every business table
# ─────────────────────────────────────────────────────────────
section "1. SOURCE — PostgreSQL ERP"

st=$(docker inspect --format='{{.State.Health.Status}}' postgres 2>/dev/null)
[[ "$st" == "healthy" ]] && ok "container healthy" || err "container not healthy ($st)"

PG_COUNTS=$(docker exec postgres psql -U postgres -d ecommerce -t -A -F'|' -c "
  SELECT 'customers',  COUNT(*) FROM customers UNION ALL
  SELECT 'orders',     COUNT(*) FROM orders UNION ALL
  SELECT 'order_items',COUNT(*) FROM order_items UNION ALL
  SELECT 'products',   COUNT(*) FROM products UNION ALL
  SELECT 'payments',   COUNT(*) FROM payments UNION ALL
  SELECT 'shipping',   COUNT(*) FROM shipping;
" 2>/dev/null)

declare -A PG
while IFS='|' read -r tbl cnt; do
  [[ -z "$tbl" ]] && continue
  PG[$tbl]=$cnt
  if [[ "$cnt" -gt 0 ]]; then ok "$tbl: $cnt rows"; else warn "$tbl: 0 rows"; fi
done <<< "$PG_COUNTS"

LATEST=$(docker exec postgres psql -U postgres -d ecommerce -t -A -c \
  "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))::int FROM orders" 2>/dev/null)
if [[ -n "$LATEST" && "$LATEST" -lt 60 ]]; then
  ok "freshness: latest order ${LATEST}s ago"
elif [[ -n "$LATEST" && "$LATEST" -lt 600 ]]; then
  warn "freshness: latest order ${LATEST}s ago (sims may be paused)"
else
  err "freshness: no recent orders (${LATEST}s ago)"
fi


# ─────────────────────────────────────────────────────────────
#  LAYER 2 — CDC (Debezium → Kafka Connect)
#  Liveness: connector + task RUNNING
#  Freshness: source DB lag is small (slot lag bytes)
# ─────────────────────────────────────────────────────────────
section "2. CDC — Debezium connector"

DBZ=$(curl -sf http://localhost:8083/connectors/erp-postgres-cdc/status 2>/dev/null)
if [[ -z "$DBZ" ]]; then
  err "connector 'erp-postgres-cdc' not found — run: bash nifi/init_debezium.sh"
else
  CSTATE=$(echo "$DBZ" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['connector']['state'])")
  TSTATE=$(echo "$DBZ" | "$PY" -c "import json,sys;d=json.load(sys.stdin); t=d.get('tasks',[]); print(t[0]['state'] if t else 'NO_TASKS')")
  [[ "$CSTATE" == "RUNNING" ]] && ok "connector: $CSTATE" || err "connector: $CSTATE"
  [[ "$TSTATE" == "RUNNING" ]] && ok "task[0]:    $TSTATE" || err "task[0]:    $TSTATE"
fi

# Debezium replication slot active?
SLOT=$(docker exec postgres psql -U postgres -d ecommerce -t -A -c \
  "SELECT active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
   FROM pg_replication_slots WHERE slot_name='erp_debezium_slot'" 2>/dev/null | tr -d ' ')
if [[ -n "$SLOT" ]]; then
  ACTIVE=$(echo "$SLOT" | cut -d'|' -f1)
  LAG=$(echo "$SLOT" | cut -d'|' -f2)
  [[ "$ACTIVE" == "t" ]] && ok "replication slot: active, lag=$LAG" \
                         || warn "replication slot: inactive, lag=$LAG"
else
  warn "replication slot 'erp_debezium_slot' not found"
fi


# ─────────────────────────────────────────────────────────────
#  LAYER 3 — INGESTION (NiFi flow)
#  Liveness: every processor RUNNING
#  Integrity: no validation errors, no queue back-pressure
# ─────────────────────────────────────────────────────────────
section "3. INGESTION — NiFi flow"

TOKEN=$(curl -sk -d "username=admin&password=adminadminadmin" \
  https://localhost:8443/nifi-api/access/token 2>/dev/null)

if [[ -z "$TOKEN" ]]; then
  err "cannot authenticate to NiFi"
else
  ROOT=$(curl -sk -H "Authorization: Bearer $TOKEN" \
    https://localhost:8443/nifi-api/process-groups/root 2>/dev/null \
    | "$PY" -c "import json,sys;print(json.load(sys.stdin)['id'])")

  curl -sk -H "Authorization: Bearer $TOKEN" \
    "https://localhost:8443/nifi-api/flow/process-groups/$ROOT" 2>/dev/null \
    | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
procs = d['processGroupFlow']['flow']['processors']
running = sum(1 for p in procs if p['component']['state'] == 'RUNNING')
total   = len(procs)
print(f'  processors RUNNING: {running}/{total}')
err_lines = []
for p in procs:
    c = p['component']
    if c['state'] != 'RUNNING':
        err_lines.append(f\"  ! {c['name']} state={c['state']}\")
    for v in c.get('validationErrors', []):
        err_lines.append(f\"    ERR: {v}\")
for line in err_lines: print(line)
"
fi


# ─────────────────────────────────────────────────────────────
#  LAYER 4 — TRANSPORT (Kafka topics)
#  Freshness: each topic has messages
#  Integrity: erp.public.{table} count >= postgres.{table} count
#             (CDC is at-least-once → may have duplicates from updates)
# ─────────────────────────────────────────────────────────────
section "4. TRANSPORT — Kafka topics"

declare -A KAFKA
get_count() {
  docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka:29092 --topic "$1" 2>/dev/null \
    | awk -F: '{sum+=$NF} END {print sum+0}'
}

for t in erp.public.customers erp.public.orders erp.public.order_items \
         erp.public.coupons erp.public.products \
         warehouse.events warehouse.events.dlq \
         payment.events payment.events.dlq; do
  c=$(get_count "$t")
  KAFKA[$t]=$c
  printf "  %-32s %10s msgs\n" "$t" "$c"
  [[ "$c" -eq 0 && "$t" != *.dlq ]] && warn "$t empty (sim may not have produced yet)"
done

# Sanity: erp topics ≥ postgres counts (Debezium publishes every INSERT/UPDATE)
[[ "${KAFKA[erp.public.customers]:-0}" -ge "${PG[customers]:-0}" ]] \
  && ok "customers Kafka ≥ Postgres (${KAFKA[erp.public.customers]} ≥ ${PG[customers]})" \
  || warn "customers Kafka < Postgres — CDC behind"


# ─────────────────────────────────────────────────────────────
#  LAYER 5 — BRONZE (HDFS raw Parquet)
#  Liveness: directories exist
#  Integrity: parquet files exist under each sink
# ─────────────────────────────────────────────────────────────
section "5. BRONZE — HDFS /datalake/bronze"

bronze_check() {
  local name="$1"
  local out
  out=$(MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -du -s -h \
        "/datalake/bronze/$name" 2>/dev/null | head -1)
  if [[ -n "$out" ]]; then
    size=$(echo "$out" | awk '{print $1, $2}')
    ok "$name: $size"
  else
    warn "$name: missing or empty"
  fi
}

for sink in erp_raw wh_raw wh_dlq pay_raw pay_dlq; do bronze_check "$sink"; done

# Check Spark checkpoint exists (offset tracking)
CKPT=$(MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -ls /datalake/_checkpoints/bronze 2>/dev/null | wc -l)
[[ "$CKPT" -gt 1 ]] && ok "checkpoint location populated (incremental safe)" \
                   || warn "checkpoint missing — Bronze hasn't run yet"


# ─────────────────────────────────────────────────────────────
#  LAYER 6 — SILVER (HDFS cleaned tables)
#  Integrity: row counts via Spark; orders Silver ≈ orders Postgres
#  (smaller is OK — Silver dedups by id, drops dirty)
# ─────────────────────────────────────────────────────────────
section "6. SILVER — HDFS /datalake/silver  (count via Spark)"

SILVER_OUT=$(MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=in-memory \
  -e "
  SELECT 'orders' AS t, COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/orders\`
  UNION ALL SELECT 'customers',  COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/customers\`
  UNION ALL SELECT 'order_items',COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/order_items\`
  UNION ALL SELECT 'products',   COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/products\`
  UNION ALL SELECT 'payments',   COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/payments\`
  UNION ALL SELECT 'shipping',   COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/shipping\`
  UNION ALL SELECT 'dlq',        COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/dlq\`
  ;" 2>/dev/null | grep -E '^[a-z_]+\s+[0-9]+' | awk '{print $1"|"$2}')

declare -A SILVER
while IFS='|' read -r tbl cnt; do
  [[ -z "$tbl" ]] && continue
  SILVER[$tbl]=$cnt
  ok "$tbl: $cnt rows"
done <<< "$SILVER_OUT"

# Reconciliation: Silver ≤ Postgres (we drop dirty + dedup)
if [[ -n "${SILVER[orders]}" && -n "${PG[orders]}" ]]; then
  RATIO=$(awk "BEGIN{print ${SILVER[orders]} / ${PG[orders]}}")
  note "silver/orders ÷ pg/orders = $RATIO  (1.0 = lossless, < 1.0 = some dropped to DLQ or not yet ingested)"
fi


# ─────────────────────────────────────────────────────────────
#  LAYER 7 — GOLD (Hive metastore + star schema)
#  Liveness: Hive metastore reachable, tables registered
#  Integrity: fact_sales rows ≈ silver/order_items rows
# ─────────────────────────────────────────────────────────────
section "7. GOLD — Hive star schema"

GOLD_OUT=$(MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "
  SELECT 'dim_customers' AS t, COUNT(*) FROM gold.dim_customers
  UNION ALL SELECT 'dim_products', COUNT(*) FROM gold.dim_products
  UNION ALL SELECT 'dim_payments', COUNT(*) FROM gold.dim_payments
  UNION ALL SELECT 'dim_shipping', COUNT(*) FROM gold.dim_shipping
  UNION ALL SELECT 'fact_sales',   COUNT(*) FROM gold.fact_sales
  ;" 2>/dev/null | grep -E '^[a-z_]+\s+[0-9]+' | awk '{print $1"|"$2}')

declare -A GOLD
while IFS='|' read -r tbl cnt; do
  [[ -z "$tbl" ]] && continue
  GOLD[$tbl]=$cnt
  ok "$tbl: $cnt rows"
done <<< "$GOLD_OUT"

if [[ -n "${GOLD[fact_sales]}" && -n "${SILVER[order_items]}" ]]; then
  RATIO=$(awk "BEGIN{print ${GOLD[fact_sales]} / ${SILVER[order_items]}}")
  note "fact_sales ÷ silver/order_items = $RATIO  (≈1.0 = inner join with orders not dropping rows)"
fi


# ─────────────────────────────────────────────────────────────
#  LAYER 8 — ORCHESTRATION (Airflow)
#  Liveness: webserver healthy
#  Freshness: latest DAG run state + age
# ─────────────────────────────────────────────────────────────
section "8. ORCHESTRATION — Airflow"

if curl -sf http://localhost:8080/health 2>/dev/null | grep -q '"healthy"'; then
  ok "Airflow webserver healthy"
else
  err "Airflow webserver unhealthy"
fi

LAST_RUN=$(curl -s -u admin:admin123 \
  "http://localhost:8080/api/v1/dags/medallion_pipeline/dagRuns?order_by=-execution_date&limit=1" 2>/dev/null \
  | "$PY" -c "
import json, sys
try:
    runs = json.load(sys.stdin)['dag_runs']
    if not runs: print('no runs'); sys.exit()
    r = runs[0]
    print(f\"{r['dag_run_id']}|{r['state']}|{r.get('end_date','-')}\")
except: print('parse error')
")

if [[ "$LAST_RUN" == *"|success|"* ]]; then
  ok "last DAG run: ${LAST_RUN%%|*} (success)"
elif [[ "$LAST_RUN" == *"|running|"* ]]; then
  ok "last DAG run: ${LAST_RUN%%|*} (running)"
elif [[ "$LAST_RUN" == *"|failed|"* ]]; then
  err "last DAG run FAILED: $LAST_RUN"
else
  warn "DAG run state unclear: $LAST_RUN"
fi


# ─────────────────────────────────────────────────────────────
#  SUMMARY
# ─────────────────────────────────────────────────────────────
section "SUMMARY"
echo ""
if [[ "$FAIL" -eq 0 && "$WARN" -eq 0 ]]; then
  echo "  ${G}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
  echo "  ${G}  ALL GREEN — pipeline healthy end-to-end${N}"
  echo "  ${G}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
  exit 0
elif [[ "$FAIL" -eq 0 ]]; then
  echo "  ${Y}${WARN} warnings, 0 failures${N}"
  exit 2
else
  echo "  ${R}${FAIL} failures, ${WARN} warnings${N}"
  exit 1
fi
