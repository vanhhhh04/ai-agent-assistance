# ╔══════════════════════════════════════════════════════════════╗
# ║  AI Agent Assistance — Makefile                             ║
# ║  Step-by-step Data Engineering setup                        ║
# ╚══════════════════════════════════════════════════════════════╝

.PHONY: help step1 step2 step3 step4 step5 step6 step7 step8 step9 step10 \
        up down status seed hdfs-setup debezium simulate pipeline \
        kafka-topics logs clean reset install-cli

# ── Default target ─────────────────────────────────────────────
help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════╗"
	@echo "║     AI Agent Assistance — Data Engineering Setup        ║"
	@echo "╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  Quick start (run in order):"
	@echo "    make install-cli   Install CLI dependencies"
	@echo "    make step1         Start all Docker services"
	@echo "    make step2         Check service health"
	@echo "    make step3         Create HDFS directory structure"
	@echo "    make step4         Seed data into PostgreSQL"
	@echo "    make step5         Register Debezium CDC connector"
	@echo "    make step6         Configure NiFi flows"
	@echo "    make step7         Start data simulators"
	@echo "    make step8         Run Bronze→Silver→Gold pipeline"
	@echo "    make step9         Verify Gold tables in HDFS"
	@echo "    make step10        Verify Kafka topics"
	@echo ""
	@echo "  Utilities:"
	@echo "    make status        Check all services"
	@echo "    make kafka-topics  List Kafka topics"
	@echo "    make pipeline      Trigger Airflow DAG"
	@echo "    make logs s=<svc>  Tail logs (e.g. make logs s=spark-master)"
	@echo "    make clean         Stop & remove containers + volumes"
	@echo ""

# ── Install CLI dependencies ───────────────────────────────────
install-cli:
	pip install -r cli-requirements.txt

# ── Step 1: Start infrastructure ──────────────────────────────
step1: up
	@echo ""
	@echo "✅ Step 1 done — services starting."
	@echo "   Wait ~2 minutes, then run: make step2"

up:
	docker compose up -d
	@echo "Waiting 15s for containers to initialize..."
	sleep 15

down:
	docker compose down

# ── Step 2: Check service health ──────────────────────────────
step2: status

status:
	python cli.py status

# ── Step 3: HDFS setup ────────────────────────────────────────
step3: hdfs-setup

hdfs-setup:
	python cli.py hdfs setup

# ── Step 4: Seed data ─────────────────────────────────────────
step4: seed

seed:
	python cli.py seed

# ── Step 5: Register Debezium ─────────────────────────────────
step5: debezium

debezium:
	python cli.py debezium setup

debezium-status:
	python cli.py debezium status

# ── Step 6: NiFi flows ────────────────────────────────────────
step6:
	@echo ""
	@echo "NiFi automated setup:"
	@echo "  pip install requests"
	@echo "  python scripts/nifi_setup.py"
	@echo ""
	@echo "  OR configure manually:"
	@echo "  python cli.py nifi"
	@echo ""
	@echo "  URL: https://localhost:8443/nifi  (admin / adminadminadmin)"

# ── Step 7: Start simulators ──────────────────────────────────
step7: simulate

simulate:
	@echo "Starting ERP simulator in background..."
	docker compose exec -T data-source python sim_erp.py &
	@echo "Starting Warehouse simulator in background..."
	docker compose exec -T data-source python sim_warehouse.py &
	@echo "Starting Payment simulator in background..."
	docker compose exec -T data-source python sim_payment.py &
	@echo "✅ Simulators running. Check Kafka UI: http://localhost:8888"

simulate-erp:
	docker compose exec data-source python sim_erp.py

simulate-warehouse:
	docker compose exec data-source python sim_warehouse.py

simulate-payment:
	docker compose exec data-source python sim_payment.py

# ── Step 8: Run pipeline ──────────────────────────────────────
step8: pipeline

pipeline:
	python cli.py pipeline run

pipeline-status:
	python cli.py pipeline status

# ── Step 9: Verify Gold layer ─────────────────────────────────
step9:
	python cli.py hdfs ls /datalake/gold
	python cli.py hdfs du /datalake

# ── Step 10: Verify Kafka ─────────────────────────────────────
step10: kafka-topics

kafka-topics:
	python cli.py kafka topics

# ── Spark jobs (manual) ───────────────────────────────────────
spark-bronze:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		/opt/bitnami/spark/jobs/bronze_ingestion.py

spark-silver:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/jobs/silver_transform.py

spark-gold:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.catalogImplementation=hive \
		/opt/bitnami/spark/jobs/gold_transform.py

# ── Utilities ─────────────────────────────────────────────────
logs:
	docker compose logs --tail=100 -f $(s)

clean:
	@echo "⚠  This removes ALL containers and named volumes!"
	@read -p "Continue? [y/N] " ans; [ "$$ans" = "y" ] && docker compose down -v || echo "Aborted"

reset: clean up
	@echo "Platform reset complete."

# ── Service URLs ──────────────────────────────────────────────
urls:
	@echo ""
	@echo "  Service URLs:"
	@echo "  ┌──────────────────────────────────────────────────────┐"
	@echo "  │  Kafka UI          http://localhost:8888             │"
	@echo "  │  HDFS Namenode     http://localhost:9870             │"
	@echo "  │  Spark Master      http://localhost:8090             │"
	@echo "  │  Airflow           http://localhost:8080             │"
	@echo "  │  NiFi              https://localhost:8443/nifi       │"
	@echo "  │  HiveServer2 UI    http://localhost:10002            │"
	@echo "  │  Kafka Connect     http://localhost:8083             │"
	@echo "  │  AI Agent API      http://localhost:8000/docs        │"
	@echo "  └──────────────────────────────────────────────────────┘"
	@echo ""
