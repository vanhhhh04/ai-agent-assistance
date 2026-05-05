#!/usr/bin/env bash
# End-to-end data integrity trace — pick one record from each source and
# follow it through every layer of the pipeline:
#
#   ERP:       Postgres orders     → Kafka → Bronze → Silver → Gold (fact_sales)
#   Warehouse: Postgres products   → CSV   → Kafka → Bronze (wh_raw)   → ALSO via Debezium CDC → Bronze (erp_raw) → Silver products → Gold dim_products
#   Payment:   Postgres payments   → HTTP  → Kafka → Bronze → Silver → Gold (dim_payments)
#
# Each layer prints the key fields so you can visually verify they match.
# A divergence (missing record / different value) flags an integrity issue.
#
# Usage:
#   bash cli/trace-record.sh                  # trace 1 record from each source (default)
#   bash cli/trace-record.sh erp              # only ERP order
#   bash cli/trace-record.sh erp 12345        # specific order id
#   bash cli/trace-record.sh warehouse        # warehouse product
#   bash cli/trace-record.sh payment          # payment

cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; C=$'\033[36m'; N=$'\033[0m'
section() { echo ""; echo "${B}${C}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"; echo "${B}${C}  $1${N}"; echo "${B}${C}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"; }
step()    { echo ""; echo "${B}${C}▶ $1${N}"; }
ok()      { echo "  ${G}✓${N} $1"; }
fail()    { echo "  ${R}✗${N} $1"; }

# ─── helpers ───
pg_q() {
  docker exec postgres psql -U postgres -d ecommerce -t -c "$1" 2>/dev/null
}

spark_q() {
  # Uses spark-sql against Spark cluster. Slower cold-start (~25s) but reliable.
  MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
    --conf spark.driver.memory=512m \
    -e "$1" 2>&1 | grep -v "^[0-9][0-9]/" | grep -v "INFO\|WARN" | grep -v "^Spark\|^To adjust\|^Setting\|^Time taken"
}

kafka_count_for_id() {
  local topic="$1" id="$2"
  # `grep -c` always prints the count; ignore its exit code via `|| true`.
  local n
  n=$(docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
        --topic "$topic" --from-beginning --max-messages 1000 \
        --timeout-ms 8000 2>/dev/null \
      | grep -c "\"id\":${id}[,}]" || true)
  echo "${n:-0}"
}


# ──────────────────────────────────────────────────────────────────────────
trace_erp() {
  local id="$1"
  if [[ -z "$id" ]]; then
    # Pick an order >30 min old so it has surely flowed through DAG (which
    # runs every 15 min). Avoids "record too new, not in Silver yet" issue.
    id=$(pg_q "SELECT id FROM orders
               WHERE total_amount > 0 AND status IS NOT NULL
                 AND created_at < NOW() - INTERVAL '30 minutes'
               ORDER BY id DESC LIMIT 1" | tr -d ' ')
  fi
  section "ERP TRACE — order_id = $id"

  step "Layer 1 — POSTGRES (source of truth)"
  pg_q "SELECT id, customer_id, status, total_amount::text, order_date::text
        FROM orders WHERE id=$id;"

  step "Layer 2 — KAFKA topic erp.public.orders (sampling — Bronze is authoritative)"
  hits=$(kafka_count_for_id "erp.public.orders" "$id")
  echo "  events with id=$id in last 1000 sampled messages: $hits"
  if [[ "$hits" -gt 0 ]]; then
    ok "found in Kafka sample"
  else
    echo "  ${Y}!${N} not in last 1000 (topic may have 1M+ msgs — see Bronze for authoritative copy)"
  fi

  step "Layer 3 — BRONZE /datalake/bronze/erp_raw (Spark snapshot of Kafka)"
  spark_q "
    SELECT _source_topic, get_json_object(raw_data, '\$.id') AS id,
           get_json_object(raw_data, '\$.status') AS status,
           get_json_object(raw_data, '\$.total_amount') AS total
    FROM parquet.\`hdfs://namenode:9000/datalake/bronze/erp_raw\`
    WHERE _source_topic='erp.public.orders'
      AND get_json_object(raw_data, '\$.id') = '$id'
    LIMIT 5;"

  step "Layer 4 — SILVER /datalake/silver/orders (cleaned + deduped)"
  spark_q "
    SELECT id, customer_id, status, total_amount, order_date
    FROM parquet.\`hdfs://namenode:9000/datalake/silver/orders\`
    WHERE id=$id;"

  step "Layer 5 — GOLD gold.fact_sales (star schema)"
  spark_q "
    SELECT order_key, customer_name, brand, quantity, item_total, order_date
    FROM gold.fact_sales WHERE order_key=$id;"
}


# ──────────────────────────────────────────────────────────────────────────
trace_warehouse() {
  local id="$1"
  if [[ -z "$id" ]]; then
    id=$(pg_q "SELECT id FROM products
               WHERE is_active=TRUE AND price > 0
                 AND created_at < NOW() - INTERVAL '30 minutes'
               ORDER BY id DESC LIMIT 1" | tr -d ' ')
  fi
  section "WAREHOUSE TRACE — product_id = $id"

  local sku
  sku=$(pg_q "SELECT sku FROM products WHERE id=$id" | tr -d ' ')

  step "Layer 1 — POSTGRES products"
  pg_q "SELECT id, sku, name, brand, price::text, stock_quantity FROM products WHERE id=$id;"

  step "Layer 2a — KAFKA warehouse.events (CSV → NiFi → Kafka path)"
  hits=$(docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
    --topic warehouse.events --from-beginning --max-messages 500 \
    --timeout-ms 8000 2>/dev/null | grep -c "\"sku\":\"$sku\"" || true)
  echo "  events with sku=$sku in last 500: ${hits:-0}"

  step "Layer 2b — KAFKA erp.public.products (Debezium CDC path — canonical)"
  hits=$(kafka_count_for_id "erp.public.products" "$id")
  echo "  events with id=$id in last 1000: $hits"
  [[ "$hits" -gt 0 ]] && ok "found in CDC topic" || fail "NOT in CDC topic"

  step "Layer 3 — BRONZE /datalake/bronze/erp_raw (CDC snapshot)"
  spark_q "
    SELECT get_json_object(raw_data, '\$.id') AS id,
           get_json_object(raw_data, '\$.sku') AS sku,
           get_json_object(raw_data, '\$.brand') AS brand,
           get_json_object(raw_data, '\$.price') AS price
    FROM parquet.\`hdfs://namenode:9000/datalake/bronze/erp_raw\`
    WHERE _source_topic='erp.public.products'
      AND get_json_object(raw_data, '\$.id') = '$id'
    LIMIT 3;"

  step "Layer 4 — SILVER /datalake/silver/products (canonical from ERP CDC)"
  spark_q "
    SELECT id, sku, name, brand, price, stock_quantity
    FROM parquet.\`hdfs://namenode:9000/datalake/silver/products\`
    WHERE id=$id;"

  step "Layer 5 — GOLD gold.dim_products (star schema dimension)"
  spark_q "
    SELECT product_key, sku, product_name, brand, list_price
    FROM gold.dim_products WHERE product_key=$id;"
}


# ──────────────────────────────────────────────────────────────────────────
trace_payment() {
  local id="$1"
  if [[ -z "$id" ]]; then
    id=$(pg_q "SELECT id FROM payments
               WHERE transaction_id IS NOT NULL
                 AND created_at < NOW() - INTERVAL '30 minutes'
               ORDER BY id DESC LIMIT 1" | tr -d ' ')
  fi
  section "PAYMENT TRACE — payment_id = $id"

  local txn
  txn=$(pg_q "SELECT transaction_id FROM payments WHERE id=$id" | tr -d ' ')

  step "Layer 1 — POSTGRES payments"
  pg_q "SELECT id, order_id, payment_method, amount::text, status, transaction_id
        FROM payments WHERE id=$id;"

  step "Layer 2 — KAFKA payment.events (HTTP → NiFi → Kafka path)"
  if [[ -n "$txn" ]]; then
    hits=$(docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
      --topic payment.events --from-beginning --max-messages 500 \
      --timeout-ms 8000 2>/dev/null | grep -c "\"transaction_id\":\"$txn\"" || true)
    hits="${hits:-0}"
    echo "  events with txn_id=$txn in last 500: $hits"
    [[ "$hits" -gt 0 ]] && ok "found in Kafka" || fail "NOT in Kafka"
  fi

  step "Layer 3 — BRONZE /datalake/bronze/pay_raw"
  spark_q "
    SELECT get_json_object(raw_data, '\$.payload.transaction_id') AS txn,
           get_json_object(raw_data, '\$.payload.amount')         AS amount,
           get_json_object(raw_data, '\$._event_type')            AS event_type
    FROM parquet.\`hdfs://namenode:9000/datalake/bronze/pay_raw\`
    WHERE get_json_object(raw_data, '\$.payload.transaction_id') = '$txn'
    LIMIT 5;"

  step "Layer 4 — SILVER /datalake/silver/payments"
  spark_q "
    SELECT order_id, payment_method, amount, status, transaction_id
    FROM parquet.\`hdfs://namenode:9000/datalake/silver/payments\`
    WHERE transaction_id='$txn';"

  step "Layer 5 — GOLD gold.dim_payments"
  spark_q "
    SELECT order_id, payment_method, payment_amount, payment_status, transaction_id
    FROM gold.dim_payments WHERE transaction_id='$txn';"
}


# ──────────────────────────────────────────────────────────────────────────
reconciliation() {
  section "COUNT RECONCILIATION (totals across all layers)"

  step "Postgres source"
  pg_q "SELECT 'orders'      AS t, COUNT(*) FROM orders UNION ALL
        SELECT 'customers',    COUNT(*) FROM customers UNION ALL
        SELECT 'products',     COUNT(*) FROM products UNION ALL
        SELECT 'payments',     COUNT(*) FROM payments UNION ALL
        SELECT 'shipping',     COUNT(*) FROM shipping;"

  step "Kafka topic offsets"
  for t in erp.public.customers erp.public.orders erp.public.order_items erp.public.products warehouse.events warehouse.events.dlq payment.events payment.events.dlq; do
    c=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka:29092 --topic "$t" 2>/dev/null \
        | awk -F: '{sum+=$NF} END {print sum+0}')
    printf "    %-32s %s\n" "$t" "$c"
  done

  step "Silver + Gold counts (via Spark — single session)"
  spark_q "
    SELECT 'silver/orders'      AS t, COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/orders\` UNION ALL
    SELECT 'silver/customers',    COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/customers\` UNION ALL
    SELECT 'silver/order_items',  COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/order_items\` UNION ALL
    SELECT 'silver/products',     COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/products\` UNION ALL
    SELECT 'silver/payments',     COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/payments\` UNION ALL
    SELECT 'silver/shipping',     COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/shipping\` UNION ALL
    SELECT 'silver/dlq',          COUNT(*) FROM parquet.\`hdfs://namenode:9000/datalake/silver/dlq\` UNION ALL
    SELECT 'gold/fact_sales',     COUNT(*) FROM gold.fact_sales UNION ALL
    SELECT 'gold/dim_customers',  COUNT(*) FROM gold.dim_customers UNION ALL
    SELECT 'gold/dim_products',   COUNT(*) FROM gold.dim_products UNION ALL
    SELECT 'gold/dim_payments',   COUNT(*) FROM gold.dim_payments UNION ALL
    SELECT 'gold/dim_shipping',   COUNT(*) FROM gold.dim_shipping;"
}


# ──────────────────────────────────────────────────────────────────────────
# main
SOURCE="${1:-all}"
ID="${2:-}"

case "$SOURCE" in
  erp)        trace_erp "$ID" ;;
  warehouse)  trace_warehouse "$ID" ;;
  payment)    trace_payment "$ID" ;;
  recon|reconciliation) reconciliation ;;
  all|"")
    trace_erp ""
    trace_warehouse ""
    trace_payment ""
    reconciliation
    ;;
  *)
    echo "Usage: bash cli/trace-record.sh [erp|warehouse|payment|recon|all] [id]"
    exit 1
    ;;
esac

section "DONE"
echo "  Visual UIs for deeper inspection:"
echo "    NiFi Provenance:  https://localhost:8443/nifi  (right-click processor → View Data Provenance)"
echo "    Kafka messages:   http://localhost:8888  (click topic → Messages)"
echo "    HDFS browse:      http://localhost:9870  (Utilities → Browse)"
echo "    Live dashboard:   http://localhost:5555"
