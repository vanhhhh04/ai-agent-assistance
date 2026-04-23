#!/usr/bin/env python3
"""
AI Agent Assistance — Data Engineering CLI
==========================================
Step-by-step management tool for the Medallion Data Platform.

Usage:
  python cli.py status                         # check all service health
  python cli.py up                             # start all containers
  python cli.py down                           # stop all containers
  python cli.py seed                           # load initial data into PostgreSQL
  python cli.py debezium setup                 # register CDC connector
  python cli.py debezium status                # check connector status
  python cli.py hdfs setup                     # create HDFS datalake dirs
  python cli.py hdfs ls [path]                 # list HDFS directory
  python cli.py pipeline run                   # trigger medallion pipeline
  python cli.py pipeline status                # check last DAG run status
  python cli.py simulate warehouse             # run warehouse CSV simulator
  python cli.py simulate erp                   # run ERP simulator
  python cli.py simulate payment               # run payment/shipping simulator
  python cli.py kafka topics                   # list Kafka topics
  python cli.py kafka lag                      # show consumer group lag
  python cli.py logs <service>                 # tail service logs
"""

import subprocess
import sys
import json
import time
import os
from typing import Optional

import click
import requests

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    console = Console()
    HAS_RICH = True
except ImportError:
    console = None
    HAS_RICH = False

# ── Config ────────────────────────────────────────────────────
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
AIRFLOW_URL       = os.getenv("AIRFLOW_URL",       "http://localhost:8080")
AIRFLOW_USER      = os.getenv("AIRFLOW_USER",      "admin")
AIRFLOW_PASS      = os.getenv("AIRFLOW_PASS",      "admin123")
AI_AGENT_URL      = os.getenv("AI_AGENT_URL",      "http://localhost:8000")

SERVICES = {
    "postgres":        ("http://localhost:5433",  "PostgreSQL (ERP Source)"),
    "kafka":           ("http://localhost:9092",  "Kafka Broker"),
    "kafka-ui":        ("http://localhost:8888",  "Kafka UI"),
    "kafka-connect":   (f"{KAFKA_CONNECT_URL}",  "Kafka Connect / Debezium"),
    "nifi":            ("https://localhost:8443", "Apache NiFi"),
    "namenode":        ("http://localhost:9870",  "HDFS Namenode"),
    "spark-master":    ("http://localhost:8090",  "Spark Master"),
    "hiveserver2":     ("http://localhost:10002", "HiveServer2"),
    "airflow":         (f"{AIRFLOW_URL}/health",  "Airflow"),
    "ai-agent":        (f"{AI_AGENT_URL}/health", "AI Agent"),
}

CONNECTOR_NAME = "erp-postgres-connector"
MEDALLION_DAG  = "medallion_pipeline"


# ── Helpers ───────────────────────────────────────────────────
def info(msg):
    if console:
        console.print(f"[cyan]{msg}[/cyan]")
    else:
        print(msg)

def ok(msg):
    if console:
        console.print(f"[green]✓ {msg}[/green]")
    else:
        print(f"✓ {msg}")

def err(msg):
    if console:
        console.print(f"[red]✗ {msg}[/red]")
    else:
        print(f"✗ {msg}")

def warn(msg):
    if console:
        console.print(f"[yellow]⚠  {msg}[/yellow]")
    else:
        print(f"⚠  {msg}")

def docker_compose(*args, capture: bool = False):
    cmd = ["docker", "compose"] + list(args)
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True)
    return subprocess.run(cmd)

def docker_exec(service: str, *cmd, interactive: bool = False):
    flags = ["-it"] if interactive else ["-T"]
    return subprocess.run(
        ["docker", "compose", "exec"] + flags + [service] + list(cmd)
    )

def check_service(name: str, url: str) -> bool:
    try:
        r = requests.get(url, timeout=5, verify=False)
        return r.status_code < 500
    except Exception:
        return False

def airflow_api(method: str, path: str, **kwargs):
    return requests.request(
        method,
        f"{AIRFLOW_URL}/api/v1{path}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        headers={"Content-Type": "application/json"},
        timeout=30,
        **kwargs,
    )


# ── CLI groups ────────────────────────────────────────────────
@click.group()
@click.version_option("1.0.0")
def cli():
    """Data Engineering CLI for the AI Agent Assistance platform."""


# ── status ────────────────────────────────────────────────────
@cli.command()
def status():
    """Check health of all platform services."""
    if console:
        table = Table(title="Platform Service Health", show_lines=True)
        table.add_column("Service",     style="bold")
        table.add_column("Status",      justify="center")
        table.add_column("URL",         style="dim")
        table.add_column("Description")

        for svc, (url, desc) in SERVICES.items():
            up = check_service(svc, url)
            status_str = "[green]UP[/green]" if up else "[red]DOWN[/red]"
            table.add_row(svc, status_str, url, desc)

        console.print(table)
    else:
        for svc, (url, desc) in SERVICES.items():
            up = check_service(svc, url)
            symbol = "✓" if up else "✗"
            print(f"  {symbol} {svc:20s} {desc}")


# ── up / down ─────────────────────────────────────────────────
@cli.command()
@click.option("--service", "-s", default=None, help="Start a specific service only")
def up(service: Optional[str]):
    """Start all Docker Compose services (or a single one)."""
    args = ["up", "-d"]
    if service:
        args.append(service)
    docker_compose(*args)
    info("\nWaiting 15s for services to initialize...")
    time.sleep(15)
    ok("Services started. Run: python cli.py status")


@cli.command()
def down():
    """Stop all Docker Compose services."""
    docker_compose("down")


# ── seed ──────────────────────────────────────────────────────
@cli.command()
def seed():
    """Load initial CSV data into PostgreSQL (runs migrate.py in data-source)."""
    info("Seeding data into PostgreSQL...")
    result = docker_exec("data-source", "python", "/app/migrate.py")
    if result.returncode == 0:
        ok("Seed complete.")
    else:
        err("Seed failed — check: docker compose logs data-source")


# ── debezium ─────────────────────────────────────────────────
@cli.group()
def debezium():
    """Manage the Debezium CDC connector."""


@debezium.command("setup")
@click.option("--wait/--no-wait", default=True, help="Wait for Kafka Connect to be ready")
def debezium_setup(wait: bool):
    """Register the PostgreSQL CDC connector with Kafka Connect."""
    if wait:
        info("Waiting for Kafka Connect...")
        for attempt in range(24):
            try:
                r = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
                if r.status_code == 200:
                    ok("Kafka Connect is ready")
                    break
            except Exception:
                pass
            time.sleep(10)
            print(f"  attempt {attempt+1}/24...")
        else:
            err("Kafka Connect did not become ready in time")
            sys.exit(1)

    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class":                        "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname":                      "postgres",
            "database.port":                          "5432",
            "database.user":                          "postgres",
            "database.password":                      "postgres",
            "database.dbname":                        "ecommerce",
            "topic.prefix":                           "erp",
            "table.include.list":                     "public.customers,public.orders,public.order_items,public.coupons",
            "plugin.name":                            "pgoutput",
            "slot.name":                              "erp_debezium_slot",
            "publication.name":                       "erp_publication",
            "snapshot.mode":                          "initial",
            "transforms":                             "unwrap",
            "transforms.unwrap.type":                 "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.add.fields":           "op,table,source.ts_ms",
            "transforms.unwrap.add.headers":          "db",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "key.converter":                          "org.apache.kafka.connect.json.JsonConverter",
            "value.converter":                        "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable":           "false",
            "value.converter.schemas.enable":         "false",
        },
    }

    # Delete if exists
    existing = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}", timeout=5)
    if existing.status_code == 200:
        warn(f"Connector '{CONNECTOR_NAME}' exists — deleting...")
        requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}", timeout=10)
        time.sleep(3)

    r = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        json=connector_config,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    if r.status_code in (200, 201):
        ok(f"Connector '{CONNECTOR_NAME}' registered")
        info("\nTopics that will be created:")
        for t in ["erp.public.customers", "erp.public.orders", "erp.public.order_items", "erp.public.coupons"]:
            print(f"    {t}")
        info("\nMonitor at: http://localhost:8888 (Kafka UI)")
    else:
        err(f"Registration failed: {r.status_code} — {r.text}")
        sys.exit(1)


@debezium.command("status")
def debezium_status():
    """Show Debezium connector status and task details."""
    try:
        r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=5)
        if r.status_code == 404:
            warn(f"Connector '{CONNECTOR_NAME}' not found. Run: python cli.py debezium setup")
            return
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        err(f"Cannot reach Kafka Connect: {e}")
        return

    connector_state = data["connector"]["state"]
    symbol = "✓" if connector_state == "RUNNING" else "✗"
    info(f"\nConnector: {CONNECTOR_NAME}")
    print(f"  {symbol} State: {connector_state}")

    for i, task in enumerate(data.get("tasks", [])):
        ts = task["state"]
        print(f"  Task {i}: {ts}")
        if task.get("trace"):
            err(f"  Error: {task['trace'][:300]}")

    # List topics
    try:
        t = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/topics", timeout=5)
        topics = t.json().get(CONNECTOR_NAME, {}).get("topics", [])
        if topics:
            info(f"\nActive topics:")
            for topic in topics:
                print(f"    {topic}")
    except Exception:
        pass


# ── hdfs ─────────────────────────────────────────────────────
@cli.group()
def hdfs():
    """Manage HDFS datalake directories."""


@hdfs.command("setup")
def hdfs_setup():
    """Create the Bronze/Silver/Gold directory structure on HDFS."""
    dirs = [
        "/datalake/bronze/erp_raw",
        "/datalake/bronze/wh_raw",
        "/datalake/bronze/pay_raw",
        "/datalake/silver/orders",
        "/datalake/silver/customers",
        "/datalake/silver/order_items",
        "/datalake/silver/products",
        "/datalake/silver/payments",
        "/datalake/silver/shipping",
        "/datalake/silver/dlq",
        "/datalake/gold/fact_sales",
        "/datalake/gold/dim_customers",
        "/datalake/gold/dim_products",
        "/datalake/gold/dim_payments",
        "/datalake/gold/dim_shipping",
        "/user/hive/warehouse",
    ]
    info("Creating HDFS Medallion directory structure...")
    for d in dirs:
        result = docker_exec("namenode", "hdfs", "dfs", "-mkdir", "-p", d, interactive=False)
        layer = d.split("/")[2] if len(d.split("/")) > 2 else "root"
        if result.returncode == 0:
            print(f"  ✓ {d}")
        else:
            print(f"  (exists) {d}")

    docker_exec("namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/datalake", interactive=False)
    docker_exec("namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/user/hive/warehouse", interactive=False)
    ok("HDFS setup complete. Monitor: http://localhost:9870")


@hdfs.command("ls")
@click.argument("path", default="/datalake")
def hdfs_ls(path: str):
    """List files in an HDFS directory."""
    docker_exec("namenode", "hdfs", "dfs", "-ls", "-h", path, interactive=False)


@hdfs.command("du")
@click.argument("path", default="/datalake")
def hdfs_du(path: str):
    """Show disk usage of HDFS paths."""
    docker_exec("namenode", "hdfs", "dfs", "-du", "-h", "-s", f"{path}/*", interactive=False)


# ── pipeline ──────────────────────────────────────────────────
@cli.group()
def pipeline():
    """Trigger and monitor the Bronze → Silver → Gold Airflow pipeline."""


@pipeline.command("run")
@click.option("--wait/--no-wait", default=False, help="Wait for completion")
def pipeline_run(wait: bool):
    """Trigger the medallion_pipeline DAG in Airflow."""
    run_id = f"cli_trigger_{int(time.time())}"
    info(f"Triggering DAG: {MEDALLION_DAG} (run_id={run_id})")
    try:
        r = airflow_api("POST", f"/dags/{MEDALLION_DAG}/dagRuns",
                        json={"dag_run_id": run_id})
        if r.status_code in (200, 200):
            ok(f"DAG triggered. Run ID: {run_id}")
            info(f"Monitor at: {AIRFLOW_URL}/dags/{MEDALLION_DAG}")
        else:
            # Try unpausing first
            airflow_api("PATCH", f"/dags/{MEDALLION_DAG}",
                        json={"is_paused": False})
            r2 = airflow_api("POST", f"/dags/{MEDALLION_DAG}/dagRuns",
                             json={"dag_run_id": run_id})
            if r2.status_code in (200, 201):
                ok(f"DAG unpaused and triggered. Run ID: {run_id}")
            else:
                err(f"Failed: {r2.status_code} — {r2.text[:300]}")
    except Exception as e:
        err(f"Cannot reach Airflow: {e}")
        info("Tip: docker compose exec airflow-webserver airflow dags trigger medallion_pipeline")

    if wait:
        info("Waiting for pipeline to complete...")
        for _ in range(120):
            time.sleep(10)
            try:
                runs = airflow_api("GET", f"/dags/{MEDALLION_DAG}/dagRuns?order_by=-start_date&limit=1")
                state = runs.json()["dag_runs"][0]["state"]
                print(f"  State: {state}")
                if state in ("success", "failed"):
                    break
            except Exception:
                pass


@pipeline.command("status")
def pipeline_status():
    """Show the last N runs of the medallion_pipeline DAG."""
    try:
        r = airflow_api("GET", f"/dags/{MEDALLION_DAG}/dagRuns?order_by=-start_date&limit=5")
        runs = r.json().get("dag_runs", [])
    except Exception as e:
        err(f"Cannot reach Airflow: {e}")
        return

    if not runs:
        warn("No runs found. Trigger with: python cli.py pipeline run")
        return

    if console:
        table = Table(title=f"DAG: {MEDALLION_DAG} — Last 5 runs", show_lines=True)
        table.add_column("Run ID",        style="dim", max_width=30)
        table.add_column("State",         justify="center")
        table.add_column("Start",         style="dim")
        table.add_column("End",           style="dim")
        for run in runs:
            state = run["state"]
            color = {"success": "green", "failed": "red", "running": "cyan"}.get(state, "yellow")
            table.add_row(
                run["dag_run_id"],
                f"[{color}]{state}[/{color}]",
                (run.get("start_date") or "")[:19],
                (run.get("end_date") or "—")[:19],
            )
        console.print(table)
    else:
        for run in runs:
            print(f"  {run['state']:10s} {run['dag_run_id']}  {run.get('start_date','')[:19]}")


# ── simulate ──────────────────────────────────────────────────
@cli.group()
def simulate():
    """Run data source simulators (ERP / Warehouse / Payment)."""


@simulate.command("erp")
def simulate_erp():
    """Run the ERP simulator (generates orders/customers in PostgreSQL)."""
    info("Starting ERP simulator (Ctrl+C to stop)...")
    docker_exec("data-source", "python", "sim_erp.py", interactive=True)


@simulate.command("warehouse")
def simulate_warehouse():
    """Run the Warehouse CSV simulator (writes CSVs → NiFi picks up)."""
    info("Starting Warehouse simulator (Ctrl+C to stop)...")
    docker_exec("data-source", "python", "sim_warehouse.py", interactive=True)


@simulate.command("payment")
def simulate_payment():
    """Run the Payment/Shipping simulator (POSTs to NiFi HTTP)."""
    info("Starting Payment simulator (Ctrl+C to stop)...")
    docker_exec("data-source", "python", "sim_payment.py", interactive=True)


@simulate.command("all")
def simulate_all():
    """Start all three simulators as background processes."""
    info("Starting all simulators in background...")
    for script in ("sim_erp.py", "sim_warehouse.py", "sim_payment.py"):
        subprocess.Popen(
            ["docker", "compose", "exec", "-T", "data-source", "python", script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        ok(f"  {script} started")
    info("Monitor with: python cli.py kafka topics")


# ── kafka ─────────────────────────────────────────────────────
@cli.group()
def kafka():
    """Inspect Kafka topics and consumer lag."""


@kafka.command("topics")
def kafka_topics():
    """List all Kafka topics and their partition counts."""
    docker_exec(
        "kafka",
        "kafka-topics", "--bootstrap-server", "kafka:29092",
        "--list",
        interactive=False,
    )


@kafka.command("describe")
@click.argument("topic")
def kafka_describe(topic: str):
    """Show partition and offset details for a topic."""
    docker_exec(
        "kafka",
        "kafka-topics", "--bootstrap-server", "kafka:29092",
        "--describe", "--topic", topic,
        interactive=False,
    )


@kafka.command("lag")
@click.option("--group", default="spark-streaming", help="Consumer group name")
def kafka_lag(group: str):
    """Show consumer lag for a consumer group."""
    docker_exec(
        "kafka",
        "kafka-consumer-groups", "--bootstrap-server", "kafka:29092",
        "--describe", "--group", group,
        interactive=False,
    )


@kafka.command("tail")
@click.argument("topic")
@click.option("--max-messages", "-n", default=20)
def kafka_tail(topic: str, max_messages: int):
    """Read the last N messages from a Kafka topic."""
    docker_exec(
        "kafka",
        "kafka-console-consumer",
        "--bootstrap-server", "kafka:29092",
        "--topic", topic,
        "--from-beginning",
        "--max-messages", str(max_messages),
        interactive=True,
    )


# ── logs ──────────────────────────────────────────────────────
@cli.command()
@click.argument("service")
@click.option("--lines", "-n", default=50, help="Number of lines to show")
@click.option("--follow", "-f", is_flag=True, help="Follow log output")
def logs(service: str, lines: int, follow: bool):
    """Tail logs for a Docker Compose service."""
    args = ["logs", f"--tail={lines}"]
    if follow:
        args.append("-f")
    args.append(service)
    docker_compose(*args)


# ── nifi ──────────────────────────────────────────────────────
@cli.command()
def nifi():
    """Open NiFi UI setup instructions."""
    info("\n── NiFi Configuration ──────────────────────────────────")
    info("  URL:      https://localhost:8443/nifi")
    info("  Login:    admin / adminadminadmin")
    info("")
    info("  Automated setup:")
    info("    pip install requests")
    info("    python scripts/nifi_setup.py")
    info("")
    info("  Processor groups to create manually:")
    info("    1. warehouse-pipeline")
    info("       GetFile (/opt/nifi/csv_input) → ConvertRecord (CSV→JSON)")
    info("       → PublishKafka (topic: warehouse.events)")
    info("")
    info("    2. payment-pipeline")
    info("       ListenHTTP (port 8181, path: payment-events)")
    info("       → PublishKafka (topic: payment.events)")


# ── step-by-step guide ────────────────────────────────────────
@cli.command()
def guide():
    """Print the step-by-step platform setup guide."""
    steps = [
        ("Step 1 — Start infrastructure",
         "python cli.py up\n  Wait ~2 min for all services to initialize"),
        ("Step 2 — Check services",
         "python cli.py status"),
        ("Step 3 — Set up HDFS directories",
         "python cli.py hdfs setup"),
        ("Step 4 — Seed initial data",
         "python cli.py seed"),
        ("Step 5 — Register Debezium CDC",
         "python cli.py debezium setup"),
        ("Step 6 — Configure NiFi flows",
         "python cli.py nifi\n  (follow instructions to configure warehouse + payment pipelines)"),
        ("Step 7 — Start data simulators",
         "python cli.py simulate erp\n  python cli.py simulate warehouse\n  python cli.py simulate payment"),
        ("Step 8 — Run medallion pipeline",
         "python cli.py pipeline run --wait"),
        ("Step 9 — Verify Gold tables",
         "python cli.py hdfs ls /datalake/gold"),
        ("Step 10 — Verify Kafka topics",
         "python cli.py kafka topics"),
    ]

    if console:
        for title, cmd in steps:
            console.print(Panel(
                f"[bold cyan]{cmd}[/bold cyan]",
                title=f"[bold]{title}[/bold]",
                border_style="blue",
            ))
    else:
        for title, cmd in steps:
            print(f"\n{'='*60}")
            print(f"  {title}")
            print(f"{'='*60}")
            print(f"  {cmd}")


if __name__ == "__main__":
    cli()
