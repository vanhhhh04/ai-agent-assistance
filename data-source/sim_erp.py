"""
sim_erp.py  (DE-grade version)
================================
Nguon: PostgreSQL / ERP
Bang:  customers, addresses, coupons, orders, order_items

THAY DOI SO VOI VERSION CU:
  + Source envelope trong moi CDC event (_source_system, _event_id, _op_type)
  + _op_type: "c" (create) | "u" (update) | "d" (delete) — giong Debezium CDC format
  + Dirty data injection co kiem soat (DIRTY_RATE = 5%)
  + Fix bug: INTERVAL_NEW_ORDER phai la variable trong loop, khong phai global
  + Simulate late CDC: doi ngu nhien 1-3s truoc khi commit → Payment co the den truoc
  + surrogate_key_hint: MD5(source_system:source_id) de Spark Silver tao surrogate key

Chay sau sim_warehouse.py (can co products trong DB truoc).

Giai doan 1 — Bootstrap:
    Insert 100k customers, 200k addresses, 100k coupons
    Insert 500k orders + order_items tuong ung

Giai doan 2 — Realtime loop:
    - Moi 30s : khach moi dang ky          → INSERT customers + addresses
    - Moi 2-5s: don hang moi               → INSERT orders + order_items
    - Moi 10s : cap nhat trang thai don    → UPDATE orders.status
    - Moi 5p  : coupon moi / het han       → INSERT/UPDATE coupons
    CDC/WAL tu dong capture → erp.events

Cai dat:
    pip install faker psycopg2-binary

Chay:
    python sim_erp.py
"""

import os
import uuid
import hashlib
import random
import time
import logging
from datetime import datetime, timedelta, date
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

BOOTSTRAP_CUSTOMERS = 100_000
BOOTSTRAP_ORDERS    = 500_000
BATCH_SIZE          = 5_000

INTERVAL_NEW_CUSTOMER = 30
INTERVAL_UPDATE_ORDER = 10
INTERVAL_COUPON       = 300

# ── Dirty data control ──────────────────────
DIRTY_RATE = 0.05
DIRTY_CONFIG = {
    "null_email":         True,   # email = NULL
    "invalid_email":      True,   # email thiếu "@"
    "phone_too_long":     True,   # phone > 20 chars
    "duplicate_email":    True,   # email trùng với customer đã có
    "status_typo":        True,   # "COMFIRMED", "SHIPED" thay vì đúng
    "future_order_date":  True,   # order_date > NOW()
    "negative_amount":    True,   # total_amount < 0
    "late_cdc_delay":     True,   # simulate CDC lag (sleep trước commit)
    "missing_items":      True,   # order không có order_items
}

SOURCE_SYSTEM    = "erp"
SCHEMA_VERSION   = "1.0"

# CDC op types — giống Debezium format
CDC_OP_CREATE = "c"
CDC_OP_UPDATE = "u"
CDC_OP_DELETE = "d"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ERP] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

fake = Faker("vi_VN")
Faker.seed(1001)
random.seed(1001)

# Pool email đã dùng — để inject duplicate
_used_emails: list[str] = []
_email_seq = 0


# ─────────────────────────────────────────────
#  SOURCE ENVELOPE + SURROGATE KEY
# ─────────────────────────────────────────────
def make_surrogate_key(source_system: str, source_id: int | str) -> str:
    """
    Tạo surrogate key — đây là kỹ thuật cốt lõi của DE.

    Tại sao cần surrogate key?
    → ERP dùng BIGINT serial, Payment GW dùng string ID, Warehouse dùng INT.
    → Khi merge vào fact table, cần 1 key thống nhất không phụ thuộc vào
      source system's internal ID scheme.
    → MD5(source_system:source_id) đảm bảo:
       - Deterministic: cùng input → cùng output (idempotent)
       - Unique across systems: "erp:123" ≠ "payment_gw:123"
       - Stable: không thay đổi khi source DB migrate

    Trong Spark Silver layer, bạn sẽ viết:
        df = df.withColumn("surrogate_key",
            md5(concat(lit("erp:"), col("order_id").cast("string"))))
    """
    raw = f"{source_system}:{source_id}"
    return hashlib.md5(raw.encode()).hexdigest()


def make_envelope(event_type: str, payload: dict,
                  op_type: str = CDC_OP_CREATE,
                  quality_flag: str = "CLEAN") -> dict:
    """
    CDC-style envelope — giống format Debezium output.

    op_type: "c" | "u" | "d" — Spark dùng để biết phải INSERT hay UPDATE dim table.
    Thiếu field này là Gap 4 trong version cũ: Spark không thể phân biệt
    CREATE vs UPDATE event, dẫn đến duplicate rows trong Silver layer.
    """
    source_id = payload.get("id") or payload.get("order_id") or payload.get("customer_id")
    return {
        "_source_system":   SOURCE_SYSTEM,
        "_schema_version":  SCHEMA_VERSION,
        "_event_id":        str(uuid.uuid4()),
        "_event_type":      event_type,
        "_op_type":         op_type,       # "c" | "u" | "d" — Debezium convention
        "_ingested_at":     datetime.now().isoformat(),
        "_quality_flag":    quality_flag,
        "_surrogate_hint":  make_surrogate_key(SOURCE_SYSTEM, source_id) if source_id else "",
        "payload": payload,
    }


# ─────────────────────────────────────────────
#  DIRTY DATA INJECTORS
# ─────────────────────────────────────────────
def maybe_dirty_customer(row: dict) -> tuple[dict, str]:
    """
    Inject lỗi vào customer record.
    Customer dirty data thường đến từ:
    - Form validation yếu ở frontend
    - Import từ legacy CRM
    - API không validate trước khi INSERT
    """
    if random.random() > DIRTY_RATE:
        return row, "CLEAN"

    dirty_type = random.choice([k for k, v in DIRTY_CONFIG.items()
                                 if v and k in ("null_email", "invalid_email",
                                                "phone_too_long", "duplicate_email")])
    row = row.copy()

    if dirty_type == "null_email":
        row["email"] = None
        row["_dirty_reason"] = "email is NULL — not collected at registration"

    elif dirty_type == "invalid_email":
        row["email"] = fake.first_name() + "gmail.com"  # thiếu @
        row["_dirty_reason"] = f"invalid email format: '{row['email']}' — missing @"

    elif dirty_type == "phone_too_long":
        row["phone"] = "0" + "".join([str(random.randint(0, 9)) for _ in range(25)])
        row["_dirty_reason"] = f"phone too long: {len(row['phone'])} chars (max 20)"

    elif dirty_type == "duplicate_email":
        if _used_emails:
            row["email"] = random.choice(_used_emails)
            row["_dirty_reason"] = f"duplicate email: '{row['email']}' — likely re-registration"
        else:
            return row, "CLEAN"

    return row, "DIRTY"


def maybe_dirty_order(row: dict) -> tuple[dict, str]:
    """
    Inject lỗi vào order record.
    Order dirty data thường đến từ:
    - Race condition giữa checkout service và inventory service
    - Network timeout làm duplicate submit
    - Timezone mismatch giữa app server và DB server
    """
    if random.random() > DIRTY_RATE:
        return row, "CLEAN"

    dirty_type = random.choice([k for k, v in DIRTY_CONFIG.items()
                                 if v and k in ("status_typo", "future_order_date",
                                                "negative_amount", "missing_items")])
    row = row.copy()

    if dirty_type == "status_typo":
        typos = ["PANDING", "PROCCESSING", "SHIPED", "DELIVERD", "CANCLED"]
        row["status"] = random.choice(typos)
        row["_dirty_reason"] = f"status typo: '{row['status']}' — not in allowed enum"

    elif dirty_type == "future_order_date":
        row["order_date"] = (datetime.now() + timedelta(days=random.randint(1, 30))).date().isoformat()
        row["_dirty_reason"] = "order_date in future — timezone conversion error"

    elif dirty_type == "negative_amount":
        row["total_amount"] = -abs(float(row["total_amount"]))
        row["_dirty_reason"] = "negative total_amount — refund processed as new order"

    elif dirty_type == "missing_items":
        row["_has_items"] = False
        row["_dirty_reason"] = "order has no items — checkout timeout or race condition"

    return row, "DIRTY"


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def sync_pk_sequences(cur):
    """
    Ensure SERIAL/IDENTITY sequences follow existing max(id) values.
    Useful when tables were seeded with explicit IDs.
    """
    for table in ("customers", "addresses", "coupons", "orders", "order_items"):
        cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table,))
        seq = cur.fetchone()[0]
        if seq:
            cur.execute(
                f"SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {table}), 1), true)",
                (seq,),
            )


def rand_ts(days_ago_max: int, days_ago_min: int = 0) -> datetime:
    minutes = random.randint(days_ago_min * 1440, days_ago_max * 1440)
    return datetime.now() - timedelta(minutes=minutes)


def rand_date(days_ago_max: int, days_ago_min: int = 0) -> date:
    return (datetime.now() - timedelta(days=random.randint(days_ago_min, days_ago_max))).date()


def rand_price(lo: float, hi: float) -> Decimal:
    return Decimal(str(round(random.uniform(lo, hi), 2)))


# ─────────────────────────────────────────────
#  ROW MAKERS
# ─────────────────────────────────────────────
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered",
                  "delivered", "delivered", "cancelled", "refunded"]
STATUS_FLOW = {
    "pending":    "processing",
    "processing": "shipped",
    "shipped":    "delivered",
}


def make_customer() -> dict:
    global _email_seq
    gender  = random.choice(["male", "female"])
    created = rand_ts(365 * 3)
    # Avoid Faker unique pool exhaustion for large bootstrap volumes.
    local_part = f"{fake.user_name()}.{_email_seq}"
    email = f"{local_part[:64]}@example.com"
    _email_seq += 1
    _used_emails.append(email)
    return {
        "first_name":    fake.first_name_male() if gender == "male" else fake.first_name_female(),
        "last_name":     fake.last_name(),
        "email":         email,
        "phone":         fake.phone_number()[:20],
        "gender":        gender,
        "date_of_birth": rand_date(365 * 50, 365 * 18).isoformat(),
        "created_at":    created.isoformat(),
        "updated_at":    created.isoformat(),
    }


def make_address(customer_id: int, is_default: bool = False) -> tuple:
    addr_type = "shipping" if is_default else random.choice(["billing", "shipping"])
    return (
        customer_id,
        addr_type,
        fake.street_address(),
        fake.city(),
        fake.state(),
        fake.postcode(),
        "Vietnam",
        is_default,
        rand_ts(365 * 3),
    )


def make_coupon() -> tuple:
    discount_type = random.choice(["percentage", "fixed"])
    value = (random.randint(5, 50) if discount_type == "percentage"
             else random.randint(1, 500) * 10_000)
    valid_from  = rand_date(90)
    valid_until = (datetime.now() + timedelta(days=random.randint(7, 180))).date()
    return (
        f"COUPON{fake.unique.bothify('??###').upper()}",
        discount_type,
        Decimal(str(value)),
        Decimal(str(random.randint(0, 500) * 1_000)),
        random.randint(100, 10_000),
        0,
        valid_from,
        valid_until,
        True,
        rand_ts(90),
    )


def make_order(customer_id: int, ship_addr_id: int, bill_addr_id: int,
               coupon_id, product_ids: list[int]) -> dict:
    subtotal = rand_price(100_000, 50_000_000)
    discount = rand_price(0, float(subtotal) * 0.3) if coupon_id else Decimal("0")
    tax      = ((subtotal - discount) * Decimal("0.08")).quantize(Decimal("0.01"))
    shipping = rand_price(15_000, 50_000)
    total    = (subtotal - discount + tax + shipping).quantize(Decimal("0.01"))
    return {
        "customer_id":          customer_id,
        "shipping_address_id":  ship_addr_id,
        "billing_address_id":   bill_addr_id,
        "coupon_id":            coupon_id,
        "status":               random.choice(ORDER_STATUSES),
        "subtotal":             subtotal,
        "discount_amount":      discount.quantize(Decimal("0.01")),
        "tax_amount":           tax,
        "shipping_cost":        shipping,
        "total_amount":         total,
        "order_date":           rand_date(365).isoformat(),
        "created_at":           rand_ts(365).isoformat(),
        "updated_at":           rand_ts(30).isoformat(),
        "_has_items":           True,  # default; dirty injector có thể set False
    }


def make_order_items(order_id: int, product_ids: list[int]) -> list[tuple]:
    n_items  = random.randint(1, 5)
    products = random.sample(product_ids, min(n_items, len(product_ids)))
    rows = []
    for pid in products:
        qty        = random.randint(1, 5)
        unit_price = rand_price(50_000, 10_000_000)
        rows.append((
            order_id, pid, qty,
            unit_price,
            (unit_price * qty).quantize(Decimal("0.01")),
            rand_ts(365),
        ))
    return rows


# ─────────────────────────────────────────────
#  BOOTSTRAP
# ─────────────────────────────────────────────
def bootstrap_customers(cur) -> tuple[list[int], dict[int, list[int]]]:
    log.info(f"Bootstrap {BOOTSTRAP_CUSTOMERS:,} customers (dirty_rate={DIRTY_RATE:.0%})...")
    customer_ids = []
    address_map: dict[int, list[int]] = {}
    inserted = 0

    while inserted < BOOTSTRAP_CUSTOMERS:
        batch = min(BATCH_SIZE, BOOTSTRAP_CUSTOMERS - inserted)
        raw_rows  = [make_customer() for _ in range(batch)]
        clean_rows = []
        for row in raw_rows:
            dirty_row, _ = maybe_dirty_customer(row)
            # Chỉ insert vào DB nếu email không NULL (DB có NOT NULL constraint)
            if dirty_row.get("email"):
                clean_rows.append((
                    dirty_row["first_name"], dirty_row["last_name"],
                    dirty_row["email"][:255], dirty_row["phone"][:20] if dirty_row.get("phone") else None,
                    dirty_row["gender"], dirty_row["date_of_birth"],
                    dirty_row["created_at"], dirty_row["updated_at"],
                ))

        if clean_rows:
            execute_values(
                cur,
                """INSERT INTO customers
                   (first_name, last_name, email, phone, gender, date_of_birth, created_at, updated_at)
                   VALUES %s
                   ON CONFLICT (email) DO NOTHING
                   RETURNING id""",
                clean_rows,
            )
            ids = [r[0] for r in cur.fetchall()]
            customer_ids.extend(ids)

            addr_rows = []
            for cid in ids:
                n = random.randint(1, 3)
                for j in range(n):
                    addr_rows.append(make_address(cid, j == 0))

            execute_values(
                cur,
                """INSERT INTO addresses
                   (customer_id, type, street, city, state, zip_code, country, is_default, created_at)
                   VALUES %s RETURNING id, customer_id""",
                addr_rows,
            )
            for aid, cid in cur.fetchall():
                address_map.setdefault(cid, []).append(aid)

        inserted += batch
        log.info(f"  → customers: {inserted:,}/{BOOTSTRAP_CUSTOMERS:,}")

    return customer_ids, address_map


def bootstrap_coupons(cur) -> list[int]:
    log.info("Bootstrap 1,000 coupons...")
    rows = [make_coupon() for _ in range(1_000)]
    execute_values(
        cur,
        """INSERT INTO coupons
           (code, discount_type, discount_value, min_order_amount, max_uses,
            times_used, valid_from, valid_until, is_active, created_at)
           VALUES %s RETURNING id""",
        rows,
    )
    ids = [r[0] for r in cur.fetchall()]
    log.info(f"  → {len(ids)} coupons inserted")
    return ids


def bootstrap_orders(cur, customer_ids, address_map, coupon_ids, product_ids) -> list[int]:
    log.info(f"Bootstrap {BOOTSTRAP_ORDERS:,} orders (dirty_rate={DIRTY_RATE:.0%})...")
    coupon_nullable = coupon_ids + [None] * len(coupon_ids)
    order_ids  = []
    inserted   = 0

    while inserted < BOOTSTRAP_ORDERS:
        batch_size = min(BATCH_SIZE, BOOTSTRAP_ORDERS - inserted)
        new_order_ids = []

        order_rows = []
        for _ in range(batch_size):
            cid   = random.choice(customer_ids)
            addrs = address_map.get(cid, [])
            if not addrs:
                continue
            ship = random.choice(addrs)
            bill = random.choice(addrs)
            raw  = make_order(cid, ship, bill, random.choice(coupon_nullable), product_ids)
            dirty_row, quality_flag = maybe_dirty_order(raw)

            # Normalize status: typo → "pending" để DB không reject
            status = dirty_row["status"]
            if status not in ("pending", "processing", "shipped", "delivered", "cancelled", "refunded"):
                status = "pending"  # DE note: trong thực tế đây là DIRTY record cần routing

            order_rows.append((
                dirty_row["customer_id"],
                dirty_row["shipping_address_id"],
                dirty_row["billing_address_id"],
                dirty_row["coupon_id"],
                status,
                dirty_row["subtotal"],
                dirty_row["discount_amount"],
                dirty_row["tax_amount"],
                dirty_row["shipping_cost"],
                abs(float(dirty_row["total_amount"])),  # abs để tránh DB reject negative
                dirty_row["order_date"],
                dirty_row["created_at"],
                dirty_row["updated_at"],
            ))

        if order_rows:
            execute_values(
                cur,
                """INSERT INTO orders
                   (customer_id, shipping_address_id, billing_address_id, coupon_id,
                    status, subtotal, discount_amount, tax_amount, shipping_cost,
                    total_amount, order_date, created_at, updated_at)
                   VALUES %s RETURNING id""",
                order_rows,
            )
            new_order_ids = [r[0] for r in cur.fetchall()]
            order_ids.extend(new_order_ids)

        item_rows = []
        for oid in new_order_ids:
            # Simulate missing_items dirty: 5% orders không có items
            if random.random() > DIRTY_RATE:
                item_rows.extend(make_order_items(oid, product_ids))

        if item_rows:
            execute_values(
                cur,
                """INSERT INTO order_items
                   (order_id, product_id, quantity, unit_price, total_price, created_at)
                   VALUES %s""",
                item_rows,
            )

        inserted += len(order_rows) if order_rows else batch_size
        log.info(f"  → orders: {inserted:,}/{BOOTSTRAP_ORDERS:,}  items: {len(item_rows)}")

    return order_ids


# ─────────────────────────────────────────────
#  REALTIME LOOP
# ─────────────────────────────────────────────
def realtime_loop(customer_ids, address_map, coupon_ids, order_ids, product_ids):
    log.info("Bắt đầu realtime ERP loop (Ctrl+C để dừng)...")
    last_new_customer = time.time()
    last_new_order    = time.time()
    last_update_order = time.time()
    last_coupon       = time.time()
    coupon_nullable   = coupon_ids + [None] * len(coupon_ids)

    # FIX BUG: interval_new_order phải là local variable trong loop
    # Version cũ: INTERVAL_NEW_ORDER = random.randint(2, 5) ở global scope
    # → Python tạo local var, không ghi được vào global → interval không thay đổi
    interval_new_order = random.randint(2, 5)

    while True:
        now = time.time()

        # ── UPDATE order status (mỗi 10s) ──
        if now - last_update_order >= INTERVAL_UPDATE_ORDER:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                sample = random.sample(order_ids, min(30, len(order_ids)))
                for oid in sample:
                    cur.execute("SELECT status FROM orders WHERE id=%s", (oid,))
                    row = cur.fetchone()
                    if row and row[0] in STATUS_FLOW:
                        new_status = STATUS_FLOW[row[0]]
                        cur.execute(
                            "UPDATE orders SET status=%s, updated_at=NOW() WHERE id=%s",
                            (new_status, oid),
                        )
                        cur.execute(
                            "UPDATE coupons SET times_used = times_used + 1 WHERE id = "
                            "(SELECT coupon_id FROM orders WHERE id=%s AND coupon_id IS NOT NULL)",
                            (oid,),
                        )
                conn.commit()
                log.info(f"UPDATE status cho {len(sample)} orders")
            finally:
                cur.close(); conn.close()
            last_update_order = now

        # ── INSERT order mới (mỗi 2-5s) ──
        if now - last_new_order >= interval_new_order:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                cid   = random.choice(customer_ids)
                addrs = address_map.get(cid, [])
                if addrs:
                    ship = random.choice(addrs)
                    bill = random.choice(addrs)
                    raw  = make_order(cid, ship, bill, random.choice(coupon_nullable), product_ids)
                    dirty_row, quality_flag = maybe_dirty_order(raw)

                    # Simulate late CDC delay — đây là dirty pattern quan trọng nhất
                    # Payment GW sẽ POST event TRƯỚC khi CDC capture order này
                    # → Spark nhận payment.created nhưng chưa có erp.order → JOIN miss
                    if DIRTY_CONFIG.get("late_cdc_delay") and quality_flag == "DIRTY":
                        delay = random.uniform(1.0, 3.0)
                        log.info(f"  [DIRTY] Simulating CDC lag: {delay:.1f}s delay before commit")
                        time.sleep(delay)

                    status = dirty_row["status"]
                    if status not in ("pending", "processing", "shipped", "delivered", "cancelled", "refunded"):
                        status = "pending"

                    cur.execute(
                        """INSERT INTO orders
                           (customer_id, shipping_address_id, billing_address_id, coupon_id,
                            status, subtotal, discount_amount, tax_amount, shipping_cost,
                            total_amount, order_date, created_at, updated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                           RETURNING id""",
                        (cid, ship, bill, random.choice(coupon_nullable),
                         "pending",
                         dirty_row["subtotal"], dirty_row["discount_amount"],
                         dirty_row["tax_amount"], dirty_row["shipping_cost"],
                         abs(float(dirty_row["total_amount"])),
                         dirty_row["order_date"]),
                    )
                    new_oid = cur.fetchone()[0]
                    order_ids.append(new_oid)

                    # missing_items dirty: skip items cho 5% orders
                    if dirty_row.get("_has_items", True):
                        items = make_order_items(new_oid, product_ids)
                        execute_values(
                            cur,
                            """INSERT INTO order_items
                               (order_id, product_id, quantity, unit_price, total_price, created_at)
                               VALUES %s""",
                            items,
                        )
                    else:
                        log.info(f"  [DIRTY] Order #{new_oid} inserted WITHOUT items")
                        items = []

                    conn.commit()

                    # Log envelope info để trace
                    envelope = make_envelope(
                        "order.created",
                        {"id": new_oid, "customer_id": cid, "status": "pending"},
                        op_type=CDC_OP_CREATE,
                        quality_flag=quality_flag,
                    )
                    log.info(
                        f"INSERT order #{new_oid} | {len(items)} items | "
                        f"flag={quality_flag} | surrogate={envelope['_surrogate_hint'][:8]}..."
                    )
            finally:
                cur.close(); conn.close()
            last_new_order    = now
            interval_new_order = random.randint(2, 5)  # FIX: update local var đúng cách

        # ── INSERT customer mới (mỗi 30s) ──
        if now - last_new_customer >= INTERVAL_NEW_CUSTOMER:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                raw = make_customer()
                dirty_row, quality_flag = maybe_dirty_customer(raw)
                if dirty_row.get("email"):
                    cur.execute(
                        """INSERT INTO customers
                           (first_name, last_name, email, phone, gender, date_of_birth, created_at, updated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
                           ON CONFLICT (email) DO NOTHING
                           RETURNING id""",
                        (dirty_row["first_name"], dirty_row["last_name"],
                         dirty_row["email"][:255],
                         dirty_row["phone"][:20] if dirty_row.get("phone") else None,
                         dirty_row["gender"], dirty_row["date_of_birth"]),
                    )
                    result = cur.fetchone()
                    if result:
                        new_cid = result[0]
                        customer_ids.append(new_cid)
                        cur.execute(
                            """INSERT INTO addresses
                               (customer_id, type, street, city, state, zip_code, country, is_default, created_at)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                            make_address(new_cid, True),
                        )
                        new_aid = cur.fetchone()[0]
                        address_map[new_cid] = [new_aid]
                        conn.commit()
                        log.info(f"INSERT customer #{new_cid} | flag={quality_flag}")
            finally:
                cur.close(); conn.close()
            last_new_customer = now

        # ── INSERT/UPDATE coupon (mỗi 5p) ──
        if now - last_coupon >= INTERVAL_COUPON:
            conn = get_conn()
            cur  = conn.cursor()
            try:
                cur.execute(
                    """INSERT INTO coupons
                       (code, discount_type, discount_value, min_order_amount, max_uses,
                        times_used, valid_from, valid_until, is_active, created_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) RETURNING id""",
                    make_coupon(),
                )
                new_cid = cur.fetchone()[0]
                coupon_ids.append(new_cid)
                cur.execute("UPDATE coupons SET is_active=FALSE WHERE valid_until < NOW() AND is_active=TRUE")
                conn.commit()
                log.info(f"INSERT coupon #{new_cid}, deactivate expired coupons")
            finally:
                cur.close(); conn.close()
            last_coupon = now

        time.sleep(1)


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  sim_erp.py — DE-grade version")
    log.info(f"  Dirty rate: {DIRTY_RATE:.0%} | Source: {SOURCE_SYSTEM} v{SCHEMA_VERSION}")
    log.info("=" * 60)

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM products")
    prod_count = cur.fetchone()[0]
    if prod_count == 0:
        log.error("Chưa có products! Hãy chạy sim_warehouse.py trước.")
        cur.close(); conn.close()
        return

    cur.execute("SELECT id FROM products WHERE is_active = TRUE")
    product_ids = [r[0] for r in cur.fetchall()]
    log.info(f"Load {len(product_ids):,} product_ids từ DB")

    # Keep PK sequences aligned before any insert path.
    sync_pk_sequences(cur)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM customers")
    cust_count = cur.fetchone()[0]

    if cust_count >= BOOTSTRAP_CUSTOMERS:
        log.info(f"Đã có {cust_count:,} customers — bỏ qua bootstrap, load IDs...")
        cur.execute("SELECT id FROM customers")
        customer_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id, customer_id FROM addresses")
        address_map: dict[int, list[int]] = {}
        for aid, cid in cur.fetchall():
            address_map.setdefault(cid, []).append(aid)
        cur.execute("SELECT id FROM coupons WHERE is_active=TRUE")
        coupon_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id FROM orders ORDER BY id DESC LIMIT 100000")
        order_ids = [r[0] for r in cur.fetchall()]
    else:
        customer_ids, address_map = bootstrap_customers(cur)
        conn.commit()
        coupon_ids = bootstrap_coupons(cur)
        conn.commit()
        order_ids = bootstrap_orders(cur, customer_ids, address_map, coupon_ids, product_ids)
        conn.commit()
        log.info("Bootstrap ERP hoàn thành!")

    cur.close(); conn.close()
    log.info(f"Ready: {len(customer_ids):,} customers · {len(coupon_ids):,} coupons · {len(order_ids):,} orders")
    realtime_loop(customer_ids, address_map, coupon_ids, order_ids, product_ids)


if __name__ == "__main__":
    main()
