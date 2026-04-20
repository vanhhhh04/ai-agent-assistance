"""
sim_warehouse.py  (DE-grade version)
=====================================
Nguon: CSV / Warehouse
Bang:  categories, products

THAY DOI SO VOI VERSION CU:
  + Source envelope trong moi CSV event (source_system, schema_version, event_id, ingested_at)
  + Dirty data injection co kiem soat (DIRTY_RATE = 5%)
  + SKU normalization helper de Spark Silver co the canonicalize
  + quality_flag: CLEAN | DIRTY — de Spark routing ve Dead Letter Queue

Chay truoc tien de ERP co the doc product_id.

Giai doan 1 — Bootstrap:
    Insert 1k categories + 100k products vao PostgreSQL
    dong thoi ghi ra CSV de NiFi GetFile doc

Giai doan 2 — Realtime loop:
    - Moi 2p: them san pham moi        → INSERT products
    - Moi 10s: cap nhat stock/gia      → UPDATE products
    - Moi 10p: them danh muc moi       → INSERT categories
    Moi thay doi ghi ra file CSV delta → NiFi GetFile poll

Cai dat:
    pip install faker psycopg2-binary

Chay:
    python sim_warehouse.py
"""

import csv
import os
import uuid
import random
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

BOOTSTRAP_CATEGORIES = 1_000
BOOTSTRAP_PRODUCTS   = 100_000
BATCH_SIZE           = 5_000
CSV_OUTPUT_DIR       = Path(os.getenv("CSV_DIR", "./csv_output"))

INTERVAL_NEW_PRODUCT  = 120
INTERVAL_UPDATE_STOCK = 10
INTERVAL_NEW_CATEGORY = 600

# ── Dirty data control ──────────────────────
# Tỉ lệ dirty records được inject — chỉnh để practice
DIRTY_RATE = 0.05   # 5% records sẽ có lỗi
# Loại lỗi được inject (bật/tắt từng loại để luyện từng kỹ thuật)
DIRTY_CONFIG = {
    "null_price":         True,   # price = NULL (thiếu dữ liệu)
    "negative_stock":     True,   # stock_quantity < 0 (invalid range)
    "zero_price":         True,   # price = 0 (flash sale hay data lỗi?)
    "sku_format_variant": True,   # SKU-001 vs SKU001 vs sku_001 (format inconsistency)
    "duplicate_sku":      True,   # inject SKU trùng để test dedup
    "future_created_at":  True,   # created_at > NOW() (clock skew)
    "truncated_name":     True,   # name bị cắt giữa chừng (encoding lỗi)
}

SOURCE_SYSTEM    = "warehouse"
SCHEMA_VERSION   = "1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WAREHOUSE] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

fake = Faker("vi_VN")
Faker.seed(2001)
random.seed(2001)

CSV_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Pool SKU đã dùng — để inject duplicate có chủ ý
_used_skus: list[str] = []


# ─────────────────────────────────────────────
#  SOURCE ENVELOPE
#  Đây là pattern DE thực tế dùng để Spark biết:
#  - Event đến từ hệ thống nào
#  - Schema version là gì (để handle schema evolution)
#  - Event ID để dedup idempotently
#  - Ingested_at để tính toán latency
# ─────────────────────────────────────────────
def make_envelope(event_type: str, payload: dict, quality_flag: str = "CLEAN") -> dict:
    """
    Bọc data trong source envelope — Spark sẽ unwrap tầng này.

    quality_flag:
        CLEAN      → Spark ghi vào Silver layer bình thường
        DIRTY      → Spark ghi vào Dead Letter Queue để review
        QUARANTINE → Spark giữ lại, cần manual review
    """
    return {
        # ── Envelope metadata (Spark đọc tầng này trước) ──
        "_source_system":  SOURCE_SYSTEM,
        "_schema_version": SCHEMA_VERSION,
        "_event_id":       str(uuid.uuid4()),      # idempotency key — Spark dùng để dedup
        "_event_type":     event_type,             # "product.created" | "product.updated"
        "_ingested_at":    datetime.now().isoformat(),
        "_quality_flag":   quality_flag,           # CLEAN | DIRTY | QUARANTINE

        # ── Actual data payload ──
        "payload": payload,
    }


# ─────────────────────────────────────────────
#  DIRTY DATA INJECTORS
#  Mỗi hàm inject 1 loại lỗi cụ thể.
#  DE thực tế phải biết TỪNG LOẠI LỖI này đến từ đâu
#  để viết đúng transformation logic trong Spark.
# ─────────────────────────────────────────────
def maybe_dirty_product(row: dict) -> tuple[dict, str]:
    """
    Với xác suất DIRTY_RATE, inject 1 loại dirty vào product row.
    Trả về (row_đã_dirty, quality_flag).

    Tại sao inject ở đây thay vì random trong Spark?
    → Vì dirty data thực tế xuất phát từ SOURCE, không phải từ pipeline.
    → Source system có lỗi, pipeline phải detect và xử lý.
    """
    if random.random() > DIRTY_RATE:
        return row, "CLEAN"

    dirty_type = random.choice([k for k, v in DIRTY_CONFIG.items() if v])
    row = row.copy()  # không mutate original

    if dirty_type == "null_price":
        # Xảy ra khi: import CSV từ legacy system bị mất cột price
        row["price"] = None
        row["_dirty_reason"] = "price is NULL — missing from source CSV"

    elif dirty_type == "negative_stock":
        # Xảy ra khi: oversell hoặc return chưa được reconcile
        row["stock_quantity"] = random.randint(-100, -1)
        row["_dirty_reason"] = "negative stock_quantity — oversell or unreconciled return"

    elif dirty_type == "zero_price":
        # Ambiguous: có thể là flash sale hợp lệ HOẶC lỗi
        # → DE phải join với promotion table để phân biệt
        row["price"] = 0
        row["_dirty_reason"] = "price=0 — ambiguous: flash sale or data error, needs enrichment"

    elif dirty_type == "sku_format_variant":
        # Thực tế: mỗi warehouse region có convention khác nhau
        # "SKU-AB1234CD" có thể đến dưới dạng "SKUAB1234CD" hoặc "sku_ab1234cd"
        original_sku = row.get("sku", "SKU-XX0000XX")
        variant = random.choice([
            original_sku.replace("SKU-", "SKU"),        # bỏ dấu gạch
            original_sku.replace("SKU-", "sku_"),        # lowercase + underscore
            original_sku.replace("SKU-", ""),            # strip prefix hoàn toàn
            original_sku + "-VN",                        # thêm suffix region
        ])
        row["sku"] = variant
        row["_dirty_reason"] = f"SKU format variant: '{variant}' (original: '{original_sku}')"

    elif dirty_type == "duplicate_sku":
        # Xảy ra khi: import batch file bị lặp, hoặc reprocessing
        if _used_skus:
            row["sku"] = random.choice(_used_skus)
            row["_dirty_reason"] = f"duplicate SKU: '{row['sku']}' — reprocessed or import error"
        else:
            return row, "CLEAN"  # chưa có SKU nào để dup

    elif dirty_type == "future_created_at":
        # Xảy ra khi: server clock không sync (NTP drift)
        row["created_at"] = (datetime.now() + timedelta(hours=random.randint(1, 48))).isoformat()
        row["_dirty_reason"] = "created_at in future — clock skew on source system"

    elif dirty_type == "truncated_name":
        # Xảy ra khi: encoding lỗi cắt UTF-8 giữa chừng
        name = row.get("name", "Product")
        row["name"] = name[:random.randint(3, 8)]  # cắt ngắn ngẫu nhiên
        row["_dirty_reason"] = "name truncated — likely UTF-8 encoding error in CSV export"

    return row, "DIRTY"


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def sync_pk_sequences(cur):
    """
    Ensure SERIAL/IDENTITY sequences follow current max(id)
    to avoid duplicate key errors after seeded imports.
    """
    for table in ("categories", "products"):
        cur.execute(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{table}', 'id'),
                COALESCE((SELECT MAX(id) FROM {table}), 1),
                true
            )
            """
        )


def rand_ts(days_ago_max: int, days_ago_min: int = 0) -> datetime:
    minutes = random.randint(days_ago_min * 1440, days_ago_max * 1440)
    return datetime.now() - timedelta(minutes=minutes)


def write_csv_delta(filename: str, rows: list[dict], fieldnames: list[str]):
    """
    Ghi CSV delta với envelope metadata.
    NiFi GetFile poll thư mục này và đẩy vào Kafka warehouse.events topic.

    Fieldnames bao gồm cả envelope fields (_source_system, _event_id, v.v.)
    để Spark có thể đọc flat CSV thay vì nested JSON.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = CSV_OUTPUT_DIR / f"{filename}_{ts}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"CSV delta → {path.name}  ({len(rows)} rows)")


# ─────────────────────────────────────────────
#  DATA TEMPLATES
# ─────────────────────────────────────────────
CATEGORY_PARENTS = [
    "Điện tử", "Thời trang", "Gia dụng", "Sách & Văn phòng",
    "Thể thao", "Mẹ & Bé", "Làm đẹp", "Thực phẩm", "Đồ chơi", "Ô tô & Xe máy"
]
CATEGORY_CHILDREN = {
    "Điện tử":          ["Điện thoại", "Laptop", "Tai nghe", "Màn hình", "Máy tính bảng", "Phụ kiện"],
    "Thời trang":       ["Áo nam", "Áo nữ", "Quần nam", "Quần nữ", "Giày dép", "Túi xách", "Đồng hồ"],
    "Gia dụng":         ["Đồ bếp", "Nội thất", "Đèn chiếu sáng", "Điều hòa", "Máy giặt"],
    "Sách & Văn phòng": ["Sách giáo khoa", "Văn học", "Kỹ năng sống", "Văn phòng phẩm"],
    "Thể thao":         ["Dụng cụ gym", "Đồ ngoài trời", "Bơi lội", "Bóng đá", "Yoga"],
    "Mẹ & Bé":          ["Sữa bột", "Đồ chơi trẻ em", "Quần áo trẻ em", "Địu & xe đẩy"],
    "Làm đẹp":          ["Chăm sóc da", "Trang điểm", "Nước hoa", "Chăm sóc tóc"],
    "Thực phẩm":        ["Đồ khô", "Đồ uống", "Snack", "Thực phẩm hữu cơ"],
    "Đồ chơi":          ["Lego", "Xe điều khiển", "Búp bê", "Board game"],
    "Ô tô & Xe máy":    ["Phụ tùng ô tô", "Phụ tùng xe máy", "Dầu nhớt", "Đồ chơi xe"],
}
BRANDS = [
    "Apple", "Samsung", "Sony", "LG", "Xiaomi", "Asus", "Dell", "HP",
    "Nike", "Adidas", "Uniqlo", "H&M", "Philips", "Panasonic",
    "Vinamilk", "TH True Milk", "Biti's", "Trung Nguyên", "Generic"
]


def make_product_row(category_id: int) -> dict:
    price = round(random.randint(50, 50_000) * 1_000, -3)
    cost  = round(price * random.uniform(0.5, 0.8), -3)
    sku   = f"SKU-{fake.unique.bothify('??####??').upper()}"
    _used_skus.append(sku)  # track để inject duplicate sau này
    return {
        "category_id":    category_id,
        "name":           fake.catch_phrase()[:100],
        "description":    fake.paragraph(nb_sentences=3),
        "sku":            sku,
        "price":          price,
        "cost":           cost,
        "stock_quantity": random.randint(0, 1000),
        "brand":          random.choice(BRANDS),
        "weight":         round(random.uniform(0.1, 30.0), 2),
        "is_active":      random.random() > 0.05,
        "created_at":     rand_ts(500).isoformat(),
        "updated_at":     rand_ts(30).isoformat(),
    }


# ─────────────────────────────────────────────
#  BOOTSTRAP
# ─────────────────────────────────────────────
def bootstrap_categories(cur) -> list[int]:
    log.info("Bootstrap categories...")
    parent_rows = [(name, fake.sentence(nb_words=5), None, rand_ts(500))
                   for name in CATEGORY_PARENTS]
    execute_values(
        cur,
        "INSERT INTO categories (name, description, parent_category_id, created_at) VALUES %s RETURNING id",
        parent_rows,
    )
    parent_db_ids = [r[0] for r in cur.fetchall()]
    parent_ids_map = {name: pid for name, pid in zip(CATEGORY_PARENTS, parent_db_ids)}

    child_rows = []
    for parent_name, children in CATEGORY_CHILDREN.items():
        pid = parent_ids_map[parent_name]
        for child_name in children:
            child_rows.append((child_name, fake.sentence(nb_words=5), pid, rand_ts(400)))

    execute_values(
        cur,
        "INSERT INTO categories (name, description, parent_category_id, created_at) VALUES %s RETURNING id",
        child_rows,
    )
    child_ids = [r[0] for r in cur.fetchall()]
    all_ids = parent_db_ids + child_ids
    log.info(f"  → {len(all_ids)} categories inserted")
    return all_ids


def bootstrap_products(cur, category_ids: list[int]) -> list[int]:
    log.info(f"Bootstrap {BOOTSTRAP_PRODUCTS:,} products (dirty_rate={DIRTY_RATE:.0%})...")
    all_ids     = []
    inserted    = 0
    dirty_count = 0

    # CSV envelope fields — flat structure để Spark đọc dễ
    CSV_FIELDS = [
        "_source_system", "_schema_version", "_event_id",
        "_event_type", "_ingested_at", "_quality_flag", "_dirty_reason",
        "category_id", "name", "description", "sku", "price", "cost",
        "stock_quantity", "brand", "weight", "is_active", "created_at", "updated_at",
    ]

    while inserted < BOOTSTRAP_PRODUCTS:
        batch_size = min(BATCH_SIZE, BOOTSTRAP_PRODUCTS - inserted)
        raw_rows   = [make_product_row(random.choice(category_ids)) for _ in range(batch_size)]

        # Inject dirty data trước khi insert vào DB
        # Lý do insert cả DIRTY vào DB: source system thực tế không validate,
        # DE pipeline mới là nơi phát hiện và xử lý
        csv_rows     = []
        db_data      = []

        for row in raw_rows:
            dirty_row, quality_flag = maybe_dirty_product(row)
            if quality_flag == "DIRTY":
                dirty_count += 1

            envelope = make_envelope("product.created", dirty_row, quality_flag)

            # Flatten envelope cho CSV (NiFi đọc flat CSV, không phải nested JSON)
            csv_row = {
                "_source_system":  envelope["_source_system"],
                "_schema_version": envelope["_schema_version"],
                "_event_id":       envelope["_event_id"],
                "_event_type":     envelope["_event_type"],
                "_ingested_at":    envelope["_ingested_at"],
                "_quality_flag":   envelope["_quality_flag"],
                "_dirty_reason":   dirty_row.get("_dirty_reason", ""),
                **{k: dirty_row.get(k, "") for k in [
                    "category_id", "name", "description", "sku", "price",
                    "cost", "stock_quantity", "brand", "weight", "is_active",
                    "created_at", "updated_at"
                ]},
            }
            csv_rows.append(csv_row)

            # DB insert: dùng original row (PostgreSQL sẽ validate constraint)
            # Trong thực tế, source DB không có constraint này — đây là DE tradeoff
            db_data.append((
                dirty_row["category_id"],
                dirty_row["name"],
                dirty_row.get("description", ""),
                dirty_row["sku"],
                dirty_row.get("price"),    # có thể NULL
                dirty_row.get("cost"),
                dirty_row.get("stock_quantity", 0),
                dirty_row.get("brand", "Generic"),
                dirty_row.get("weight", 1.0),
                dirty_row.get("is_active", True),
                dirty_row.get("created_at", datetime.now().isoformat()),
                dirty_row.get("updated_at", datetime.now().isoformat()),
            ))

        # Filter out rows với NULL price để DB không reject (simulate source without constraint)
        valid_db_data = [r for r in db_data if r[4] is not None and r[3]]  # price và sku not null
        if valid_db_data:
            execute_values(
                cur,
                """INSERT INTO products
                   (category_id, name, description, sku, price, cost,
                    stock_quantity, brand, weight, is_active, created_at, updated_at)
                   VALUES %s
                   ON CONFLICT (sku) DO NOTHING
                   RETURNING id""",
                valid_db_data,
            )
            ids = [r[0] for r in cur.fetchall()]
            all_ids.extend(ids)

        inserted += len(raw_rows)
        write_csv_delta("products_bootstrap", csv_rows, CSV_FIELDS)
        log.info(f"  → products: {inserted:,}/{BOOTSTRAP_PRODUCTS:,}  dirty: {dirty_count}")

    return all_ids


# ─────────────────────────────────────────────
#  REALTIME LOOP
# ─────────────────────────────────────────────
def realtime_loop(category_ids: list[int], product_ids: list[int]):
    log.info("Bat dau realtime loop (Ctrl+C de dung)...")
    last_new_product  = time.time()
    last_update_stock = time.time()
    last_new_category = time.time()

    CSV_FIELDS = [
        "_source_system", "_schema_version", "_event_id",
        "_event_type", "_ingested_at", "_quality_flag", "_dirty_reason",
        "product_id", "event", "updated_at", "new_stock", "new_price",
    ]

    while True:
        now = time.time()

        # ── UPDATE stock/price (mỗi 10s) ──
        if now - last_update_stock >= INTERVAL_UPDATE_STOCK:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                sample_ids = random.sample(product_ids, min(50, len(product_ids)))
                csv_rows   = []
                for pid in sample_ids:
                    new_stock = random.randint(0, 1000)
                    new_price = round(random.randint(50, 50_000) * 1_000, -3)
                    cur.execute(
                        "UPDATE products SET stock_quantity=%s, price=%s, updated_at=NOW() WHERE id=%s",
                        (new_stock, new_price, pid),
                    )
                    payload = {
                        "product_id": pid,
                        "event": "stock_update",
                        "updated_at": datetime.now().isoformat(),
                        "new_stock": new_stock,
                        "new_price": new_price,
                    }
                    # Stock update ít dirty hơn — 2% thay vì 5%
                    quality_flag = "DIRTY" if random.random() < 0.02 else "CLEAN"
                    envelope = make_envelope("product.stock_updated", payload, quality_flag)
                    csv_rows.append({
                        "_source_system":  envelope["_source_system"],
                        "_schema_version": envelope["_schema_version"],
                        "_event_id":       envelope["_event_id"],
                        "_event_type":     envelope["_event_type"],
                        "_ingested_at":    envelope["_ingested_at"],
                        "_quality_flag":   envelope["_quality_flag"],
                        "_dirty_reason":   "negative_stock injected" if quality_flag == "DIRTY" else "",
                        **payload,
                    })
                conn.commit()
                write_csv_delta("products_stock_update", csv_rows, CSV_FIELDS)
                log.info(f"Updated stock/price for {len(sample_ids)} products")
            finally:
                cur.close(); conn.close()
            last_update_stock = now

        # ── INSERT sản phẩm mới (mỗi 2p) ──
        if now - last_new_product >= INTERVAL_NEW_PRODUCT:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                new_products = [make_product_row(random.choice(category_ids))
                                for _ in range(random.randint(3, 10))]
                for row in new_products:
                    dirty_row, quality_flag = maybe_dirty_product(row)
                    if dirty_row.get("price") is None or not dirty_row.get("sku"):
                        continue  # skip DB insert cho invalid records
                    cur.execute(
                        """INSERT INTO products
                           (category_id, name, description, sku, price, cost,
                            stock_quantity, brand, weight, is_active, created_at, updated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                           ON CONFLICT (sku) DO NOTHING
                           RETURNING id""",
                        (dirty_row["category_id"], dirty_row["name"], dirty_row.get("description",""),
                         dirty_row["sku"], dirty_row["price"], dirty_row.get("cost"),
                         dirty_row.get("stock_quantity", 0), dirty_row.get("brand","Generic"),
                         dirty_row.get("weight", 1.0), dirty_row.get("is_active", True)),
                    )
                    row_result = cur.fetchone()
                    if row_result:
                        product_ids.append(row_result[0])
                conn.commit()
                log.info(f"INSERT {len(new_products)} sản phẩm mới (tổng: {len(product_ids):,})")
            finally:
                cur.close(); conn.close()
            last_new_product = now

        # ── INSERT danh mục mới (mỗi 10p) ──
        if now - last_new_category >= INTERVAL_NEW_CATEGORY:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                new_name = fake.word().capitalize() + " " + fake.word().capitalize()
                parent   = random.choice(category_ids[:10])
                cur.execute(
                    """INSERT INTO categories (name, description, parent_category_id, created_at)
                       VALUES (%s, %s, %s, NOW()) RETURNING id""",
                    (new_name, fake.sentence(), parent),
                )
                new_id = cur.fetchone()[0]
                category_ids.append(new_id)
                conn.commit()
                log.info(f"INSERT category mới: '{new_name}' (id={new_id})")
            finally:
                cur.close(); conn.close()
            last_new_category = now

        time.sleep(1)


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  sim_warehouse.py — DE-grade version")
    log.info(f"  Dirty rate: {DIRTY_RATE:.0%} | Source: {SOURCE_SYSTEM} v{SCHEMA_VERSION}")
    log.info("=" * 60)

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM categories")
        cat_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM products")
        prod_count = cur.fetchone()[0]

        if cat_count > 0 and prod_count > 0:
            log.info(f"Đã có {cat_count:,} categories, {prod_count:,} products — bỏ qua bootstrap")
            cur.execute("SELECT id FROM categories")
            category_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id FROM products")
            product_ids  = [r[0] for r in cur.fetchall()]
        else:
            category_ids = bootstrap_categories(cur)
            conn.commit()
            product_ids  = bootstrap_products(cur, category_ids)
            conn.commit()
            log.info("Bootstrap hoàn thành!")

        # Keep sequences aligned with existing IDs (important after seeded imports).
        sync_pk_sequences(cur)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    log.info(f"Load {len(category_ids):,} category_ids, {len(product_ids):,} product_ids vào RAM")
    realtime_loop(category_ids, product_ids)


if __name__ == "__main__":
    main()
