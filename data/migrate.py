"""
initial_load.py
===============
Mục đích: Dump toàn bộ data lịch sử từ PostgreSQL → local Parquet (Bronze layer)
          TRƯỚC khi CDC/NiFi bắt đầu capture realtime.

Cách dùng:
    1. STOP 3 file sim (Ctrl+C)
    2. python initial_load.py
    3. START lại 3 file sim
    4. Bật NiFi/CDC — realtime từ đây trở đi
# Bước 0: Cài thư viện (nếu chưa có)
pip install pandas pyarrow psycopg2-binary

# Bước 1: STOP 3 file sim trước
# (Ctrl+C trên 3 terminal đang chạy sim_erp, sim_warehouse, sim_payment)

# Bước 2: Chạy initial load
python initial_load.py

# Bước 3: START lại 3 file sim
Folder structure (chuẩn Bronze Medallion):
    ./data_lake/
    └── bronze/
        ├── erp/
        │   ├── customers/    → customers.parquet
        │   ├── addresses/    → addresses.parquet
        │   ├── coupons/      → coupons.parquet
        │   ├── orders/       → orders.parquet
        │   └── order_items/  → order_items.parquet
        ├── warehouse/
        │   ├── categories/   → categories.parquet
        │   └── products/     → products.parquet
        └── payment/
            ├── payments/     → payments.parquet
            ├── shipping/     → shipping.parquet
            ├── reviews/      → reviews.parquet
            └── feedback/     → feedback.parquet

Sau này khi có HDFS thật, chỉ cần đổi:
    BASE_PATH = "hdfs:///data_lake"   ← thay dòng này là xong, không đổi gì khác
"""

import os
import logging
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import pandas as pd

# ─────────────────────────────────────────────
#  CONFIG — copy y chang tu cac file sim_*.py
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# Đổi dòng này thành "hdfs:///data_lake" khi có HDFS
BASE_PATH = Path(os.getenv("BRONZE_PATH", "./data_lake/bronze"))

# Batch size khi đọc bảng lớn (tránh load hết RAM)
BATCH_SIZE = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INITIAL_LOAD] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def add_metadata(df: pd.DataFrame, source_system: str, table_name: str) -> pd.DataFrame:
    """
    Thêm metadata columns vào mỗi record — đây là kỹ thuật quan trọng.

    Tại sao cần _load_type?
    → Spark Silver cần phân biệt data lịch sử (historical) với
      CDC realtime mới vào sau. Logic xử lý có thể khác nhau.
      Ví dụ: historical không cần check duplicate vì đã stable,
             CDC realtime cần dedup theo _event_id.

    Tại sao cần _loaded_at?
    → Audit trail — biết data này được load lúc nào,
      hữu ích khi debug hoặc cần re-load một phần.
    """
    df["_load_type"]      = "historical"
    df["_source_system"]  = source_system
    df["_table_name"]     = table_name
    df["_loaded_at"]      = datetime.now().isoformat()
    return df


def dump_table(
    cur,
    query: str,
    source_system: str,
    table_name: str,
    output_dir: Path,
) -> int:
    """
    Đọc 1 bảng từ PostgreSQL và ghi ra Parquet.

    Dùng server-side cursor (named cursor) để tránh load
    toàn bộ bảng lớn vào RAM một lần — kỹ thuật chuẩn khi
    xử lý bảng triệu rows trên máy cá nhân.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{table_name}.parquet"

    log.info(f"  Đang dump {source_system}.{table_name} → {output_path}")

    # Named cursor = server-side cursor → stream data, không load hết vào RAM
    named_cur = cur.connection.cursor(
        name=f"cursor_{table_name}",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    named_cur.execute(query)

    chunks = []
    total_rows = 0

    while True:
        rows = named_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        df_chunk = pd.DataFrame(rows)
        df_chunk = add_metadata(df_chunk, source_system, table_name)
        chunks.append(df_chunk)
        total_rows += len(rows)
        log.info(f"    ... đã đọc {total_rows:,} rows")

    named_cur.close()

    if chunks:
        df_final = pd.concat(chunks, ignore_index=True)

        # Decimal → float để Parquet không bị lỗi type
        for col in df_final.select_dtypes(include=["object"]).columns:
            try:
                df_final[col] = df_final[col].apply(
                    lambda x: float(x) if hasattr(x, "__float__") and not isinstance(x, str) else x
                )
            except Exception:
                pass

        df_final.to_parquet(output_path, index=False, engine="pyarrow")
        log.info(f"  ✓ {table_name}: {total_rows:,} rows → {output_path}")
    else:
        log.warning(f"  ⚠ {table_name}: bảng rỗng, bỏ qua")

    return total_rows


# ─────────────────────────────────────────────
#  SNAPSHOT LOGIC
# ─────────────────────────────────────────────
def take_snapshot():
    """
    Mở 1 transaction REPEATABLE READ duy nhất cho toàn bộ quá trình dump.

    Tại sao REPEATABLE READ?
    → Đảm bảo tất cả bảng được đọc tại CÙNG 1 thời điểm nhất quán.
      Nếu sim_erp.py INSERT order mới trong lúc bạn đang đọc,
      transaction này sẽ KHÔNG thấy order đó → consistent snapshot.
    → Đây là cách Debezium Snapshot Mode hoạt động trong thực tế.

    pg_current_wal_lsn():
    → LSN = Log Sequence Number — vị trí hiện tại trong WAL của PostgreSQL.
    → Ghi lại LSN này để sau này bảo NiFi/CDC: "bắt đầu từ đây".
    → Trong môi trường local không có Debezium thật, LSN chỉ dùng để log,
      không cần làm gì thêm — nhưng tập thói quen ghi lại là tốt.
    """
    conn = get_conn()
    conn.autocommit = False

    cur = conn.cursor()

    # Bước 1: Mở consistent snapshot
    cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    cur.execute("SELECT pg_current_wal_lsn()")
    snapshot_lsn = cur.fetchone()[0]
    snapshot_time = datetime.now().isoformat()

    log.info("=" * 60)
    log.info("  INITIAL LOAD — Consistent Snapshot")
    log.info(f"  Snapshot LSN : {snapshot_lsn}")
    log.info(f"  Snapshot Time: {snapshot_time}")
    log.info("  (CDC/NiFi nên bắt đầu từ LSN này trở đi)")
    log.info("=" * 60)

    stats = {}

    # ── ERP tables ──────────────────────────────
    log.info("\n[1/3] ERP tables (PostgreSQL nguồn chính)...")

    stats["customers"] = dump_table(
        cur,
        "SELECT * FROM customers ORDER BY id",
        source_system="erp",
        table_name="customers",
        output_dir=BASE_PATH / "erp" / "customers",
    )
    stats["addresses"] = dump_table(
        cur,
        "SELECT * FROM addresses ORDER BY id",
        source_system="erp",
        table_name="addresses",
        output_dir=BASE_PATH / "erp" / "addresses",
    )
    stats["coupons"] = dump_table(
        cur,
        "SELECT * FROM coupons ORDER BY id",
        source_system="erp",
        table_name="coupons",
        output_dir=BASE_PATH / "erp" / "coupons",
    )
    stats["orders"] = dump_table(
        cur,
        "SELECT * FROM orders ORDER BY id",
        source_system="erp",
        table_name="orders",
        output_dir=BASE_PATH / "erp" / "orders",
    )
    stats["order_items"] = dump_table(
        cur,
        "SELECT * FROM order_items ORDER BY id",
        source_system="erp",
        table_name="order_items",
        output_dir=BASE_PATH / "erp" / "order_items",
    )

    # ── Warehouse tables ─────────────────────────
    log.info("\n[2/3] Warehouse tables (categories + products)...")

    stats["categories"] = dump_table(
        cur,
        "SELECT * FROM categories ORDER BY id",
        source_system="warehouse",
        table_name="categories",
        output_dir=BASE_PATH / "warehouse" / "categories",
    )
    stats["products"] = dump_table(
        cur,
        "SELECT * FROM products ORDER BY id",
        source_system="warehouse",
        table_name="products",
        output_dir=BASE_PATH / "warehouse" / "products",
    )

    # ── Payment tables ───────────────────────────
    log.info("\n[3/3] Payment tables...")

    payment_tables = ["payments", "shipping", "reviews", "feedback"]
    for table in payment_tables:
        try:
            stats[table] = dump_table(
                cur,
                f"SELECT * FROM {table} ORDER BY id",
                source_system="payment",
                table_name=table,
                output_dir=BASE_PATH / "payment" / table,
            )
        except psycopg2.errors.UndefinedTable:
            log.warning(f"  ⚠ Bảng '{table}' chưa có dữ liệu — bỏ qua")
            conn.rollback()
            cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")

    # Bước 2: Đóng transaction (không commit gì cả vì chỉ đọc)
    conn.rollback()
    cur.close()
    conn.close()

    # ── Ghi snapshot metadata ra file ───────────
    meta_path = BASE_PATH / "_snapshot_meta.txt"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w") as f:
        f.write(f"snapshot_lsn={snapshot_lsn}\n")
        f.write(f"snapshot_time={snapshot_time}\n")
        f.write(f"tables_loaded={list(stats.keys())}\n")
        for table, count in stats.items():
            f.write(f"{table}_rows={count}\n")

    log.info("\n" + "=" * 60)
    log.info("  INITIAL LOAD HOÀN THÀNH")
    log.info(f"  Metadata lưu tại: {meta_path}")
    log.info("  Tổng kết:")
    for table, count in stats.items():
        log.info(f"    {table:20s}: {count:>10,} rows")
    log.info("=" * 60)
    log.info("\nBước tiếp theo:")
    log.info("  1. Start lại 3 file sim (sim_erp, sim_warehouse, sim_payment)")
    log.info("  2. Bật NiFi → CDC capture realtime từ đây trở đi")
    log.info(f"  3. Spark Silver đọc Parquet tại: {BASE_PATH}")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Kiểm tra kết nối PostgreSQL...")
    try:
        conn = get_conn()
        conn.close()
        log.info("✓ Kết nối OK")
    except Exception as e:
        log.error(f"✗ Không kết nối được PostgreSQL: {e}")
        exit(1)

    try:
        import pyarrow  # noqa
    except ImportError:
        log.error("Thiếu thư viện! Chạy: pip install pandas pyarrow psycopg2-binary")
        exit(1)

    take_snapshot()