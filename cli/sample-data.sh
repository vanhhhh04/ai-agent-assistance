#!/usr/bin/env bash
# Print 3-5 sample records from EACH layer of the pipeline.
# Useful when you want to "see" actual data without opening 6 UIs.
#
# Usage:  bash cli/sample-data.sh [N]   # default N=3 records per layer

cd "$(dirname "$0")/.."

# Pick a Python with `requests` available
if [[ -x venv/Scripts/python.exe ]]; then PY=venv/Scripts/python.exe
elif [[ -x venv/bin/python ]];    then PY=venv/bin/python
elif command -v python3 >/dev/null 2>&1; then PY=python3
else PY=python; fi

N="${1:-3}"
B=$'\033[1m'; G=$'\033[32m'; C=$'\033[36m'; N_=$'\033[0m'

section() { echo ""; echo "${B}${C}━━━ $1 ━━━${N_}"; }

# ─────────────────────────────────────────────────────────────
section "1. POSTGRES — sample orders"
docker exec postgres psql -U postgres -d ecommerce -c "
  SELECT id, customer_id, status, total_amount, order_date
  FROM orders ORDER BY id DESC LIMIT $N;" 2>/dev/null

section "1b. POSTGRES — sample products"
docker exec postgres psql -U postgres -d ecommerce -c "
  SELECT id, sku, name, brand, price FROM products
  WHERE is_active = TRUE ORDER BY id DESC LIMIT $N;" 2>/dev/null

# ─────────────────────────────────────────────────────────────
section "2. KAFKA — sample erp.public.orders (latest $N)"
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic erp.public.orders \
  --from-beginning --max-messages "$N" \
  --timeout-ms 10000 2>/dev/null \
  | "$PY" -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try: d = json.loads(line); print(json.dumps(d, indent=2, ensure_ascii=False)[:500] + '...'); print('---')
    except: print(line[:300])" 2>/dev/null

section "2b. KAFKA — sample warehouse.events"
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic warehouse.events \
  --from-beginning --max-messages "$N" \
  --timeout-ms 10000 2>/dev/null | head -c 1500
echo ""

section "2c. KAFKA — sample warehouse.events.dlq (DIRTY records)"
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic warehouse.events.dlq \
  --from-beginning --max-messages "$N" \
  --timeout-ms 10000 2>/dev/null | head -c 1500
echo ""

# ─────────────────────────────────────────────────────────────
section "3. BRONZE — raw_data column from erp_raw parquet"
MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  -e "SELECT _source_topic, raw_data FROM parquet.\`hdfs://namenode:9000/datalake/bronze/erp_raw\` LIMIT $N;" \
  2>/dev/null | grep -v "^[0-9]*/" | tail -20

# ─────────────────────────────────────────────────────────────
section "4. SILVER — clean orders"
MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  -e "SELECT id, customer_id, status, total_amount, order_date
      FROM parquet.\`hdfs://namenode:9000/datalake/silver/orders\`
      ORDER BY id DESC LIMIT $N;" 2>/dev/null | grep -v "^[0-9]*/" | tail -10

section "4b. SILVER — DLQ samples (dirty data caught)"
MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  -e "SELECT id, _source, _quality_flag, _bronze_ingested_at
      FROM parquet.\`hdfs://namenode:9000/datalake/silver/dlq\` LIMIT $N;" \
  2>/dev/null | grep -v "^[0-9]*/" | tail -10

# ─────────────────────────────────────────────────────────────
section "5. GOLD — fact_sales joined view"
MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "SELECT order_key, customer_name, product_name, brand, quantity, item_total, order_date
      FROM gold.fact_sales
      WHERE customer_name IS NOT NULL
      ORDER BY order_date DESC LIMIT $N;" 2>/dev/null | grep -v "^[0-9]*/" | tail -10

section "5b. GOLD — top brands by sales"
MSYS_NO_PATHCONV=1 docker exec spark-master /opt/bitnami/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "SELECT brand, COUNT(*) AS items_sold, SUM(item_total) AS revenue
      FROM gold.fact_sales WHERE brand IS NOT NULL
      GROUP BY brand ORDER BY revenue DESC LIMIT 5;" 2>/dev/null | grep -v "^[0-9]*/" | tail -10

echo ""
echo "${G}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N_}"
echo "${G}  Sample dump complete. Open UIs for deeper inspection:${N_}"
echo "    Postgres:   http://localhost:8081  (Adminer)"
echo "    Kafka:      http://localhost:8888  (Kafka UI)"
echo "    NiFi:       https://localhost:8443/nifi"
echo "    HDFS:       http://localhost:9870"
echo "    Spark:      http://localhost:8090"
echo "    Airflow:    http://localhost:8080"
echo "${G}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N_}"
