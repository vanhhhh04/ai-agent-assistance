"""
Catalog indexer — Hive Metastore (Gold layer) → finch_catalog index.

For each table in the Gold database, emits:
  - 1 table-level doc   (doc_type=table)   describing the table as a whole
  - 1 column-level doc  (doc_type=column)  per column, with sample values

Idempotent: re-runs upsert by stable doc_id, so it's safe to run on a schedule.

Embeddings are NOT computed here — that's Part 3. Docs are searchable via BM25
on `description`, `synonyms`, `sample_values` immediately.

Usage:
  python -m opensearch.indexers.catalog_indexer
  HIVE_HOST=hive-server2 python -m opensearch.indexers.catalog_indexer  # in container
"""

from __future__ import annotations

import sys
import logging
from datetime import datetime, timezone
from typing import Iterable

from .. import config
from ..client import get_client, bulk_upsert, ping
from ..embedder import embed

log = logging.getLogger("catalog_indexer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)


# ─────────────────────────────────────────────────────────────
#  Hive helpers
# ─────────────────────────────────────────────────────────────
def hive_cursor():
    """
    Open a HiveServer2 connection. Imports pyhive lazily so a missing optional
    dep only blows up when this script is actually run, not on module import.
    """
    try:
        from pyhive import hive
    except ImportError as e:
        log.error("pyhive not installed — pip install pyhive thrift thrift_sasl")
        raise SystemExit(2) from e

    try:
        kwargs = {
            "host":     config.HIVE_HOST,
            "port":     config.HIVE_PORT,
            "database": config.HIVE_DB,
            "auth":     config.HIVE_AUTH,
        }
        # auth=NONE/LDAP/CUSTOM use SASL/PLAIN — supply a username so the
        # PLAIN mech doesn't fall back to getpass.getuser() (= 'root' in this
        # container, which the server may reject).
        if config.HIVE_AUTH.upper() in ("NONE", "LDAP", "CUSTOM"):
            kwargs["username"] = config.HIVE_USERNAME
        conn = hive.Connection(**kwargs)
        return conn, conn.cursor()
    except Exception as e:
        log.error(
            "Could not connect to HiveServer2 at %s:%s (%s: %s). "
            "Is the full pipeline up? Try: bash cli/startup.sh",
            config.HIVE_HOST, config.HIVE_PORT, type(e).__name__, e,
        )
        raise SystemExit(3) from e


def list_tables(cur, db: str) -> list[str]:
    cur.execute(f"SHOW TABLES IN {db}")
    return [row[0] for row in cur.fetchall()]


def describe_table(cur, db: str, table: str) -> list[tuple[str, str, str | None]]:
    """
    Returns list of (column_name, column_type, comment).
    DESCRIBE returns columns then a separator line then partition info — we stop
    at the separator. Hive includes commented partition columns again at the end.
    """
    cur.execute(f"DESCRIBE {db}.{table}")
    rows = cur.fetchall()
    cols: list[tuple[str, str, str | None]] = []
    for r in rows:
        # Each row: (col_name, data_type, comment)
        if not r or not r[0] or r[0].startswith("#"):
            break
        if r[0].strip() == "":
            break
        cols.append((r[0], r[1] or "", (r[2] or "").strip() or None))
    # De-dup (DESCRIBE may repeat partition columns)
    seen: set[str] = set()
    out = []
    for name, typ, com in cols:
        if name in seen:
            continue
        seen.add(name)
        out.append((name, typ, com))
    return out


def sample_values(cur, db: str, table: str, column: str, limit: int) -> list[str]:
    """
    Pull up to `limit` distinct values for a column. Skips on error
    (high-cardinality string columns or computed columns can blow up).
    """
    try:
        cur.execute(
            f"SELECT DISTINCT {column} FROM {db}.{table} "
            f"WHERE {column} IS NOT NULL LIMIT {limit}"
        )
        return [str(r[0]) for r in cur.fetchall() if r[0] is not None]
    except Exception as e:
        log.debug("sample_values skipped %s.%s.%s: %s", db, table, column, e)
        return []


def row_count(cur, db: str, table: str) -> int | None:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {db}.{table}")
        return int(cur.fetchone()[0])
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
#  Doc builders
# ─────────────────────────────────────────────────────────────
def _table_kind(name: str) -> str:
    if name.startswith("fact_"):
        return "fact"
    if name.startswith("dim_"):
        return "dim"
    return "other"


def _column_is_pii(table: str, column: str) -> bool:
    """
    Heuristic PII tagging — Phase 4 will refine. For now flag obvious fields
    so the catalog already carries the metadata, and guardrails can use it
    once they're built.
    """
    pii_keywords = ("email", "phone", "address", "street", "zip_code",
                    "date_of_birth", "tracking_number", "transaction_id")
    return any(k in column.lower() for k in pii_keywords)


def build_table_doc(db: str, table: str, columns, n_rows: int | None) -> dict:
    kind = _table_kind(table)
    doc_id = f"table::{db}.{table}"
    description = f"{kind.upper()} table {db}.{table} with {len(columns)} columns"
    if n_rows is not None:
        description += f" and approximately {n_rows:,} rows"
    return {
        "doc_id":        doc_id,
        "doc_type":      "table",
        "database":      db,
        "table_name":    table,
        "table_kind":    kind,
        "table_grain":   None,
        "description":   description,
        "tags":          [kind, db],
        "related_tables": [],
        "indexed_at":    datetime.now(timezone.utc).isoformat(),
        "schema_version": "1",
    }


def build_column_doc(
    db: str,
    table: str,
    column: str,
    col_type: str,
    comment: str | None,
    samples: list[str],
) -> dict:
    doc_id = f"column::{db}.{table}.{column}"
    description = comment or f"Column {column} of type {col_type} in {db}.{table}"
    return {
        "doc_id":        doc_id,
        "doc_type":      "column",
        "database":      db,
        "table_name":    table,
        "column_name":   column,
        "column_type":   col_type,
        "is_pii":        _column_is_pii(table, column),
        "is_partition":  False,   # set by caller if known
        "is_nullable":   True,
        "description":   description,
        "synonyms":      "",
        "sample_values": samples,
        "tags":          [_table_kind(table), db],
        "indexed_at":    datetime.now(timezone.utc).isoformat(),
        "schema_version": "1",
    }


# ─────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────
def index_all() -> int:
    """Returns process exit code (0 = success, non-zero = error)."""
    os_client = get_client()
    if not ping(os_client):
        log.error("OpenSearch not reachable at %s — start it via cli/opensearch-up.sh",
                  config.OPENSEARCH_URL)
        return 1

    conn, cur = hive_cursor()
    try:
        tables = list_tables(cur, config.HIVE_DB)
        log.info("Found %d tables in %s: %s", len(tables), config.HIVE_DB, tables)
        if not tables:
            log.warning("No tables in %s — has the medallion pipeline run yet?",
                        config.HIVE_DB)
            return 0

        docs: list[dict] = []
        for table in tables:
            cols = describe_table(cur, config.HIVE_DB, table)
            n_rows = row_count(cur, config.HIVE_DB, table)
            log.info("  %s: %d cols, %s rows",
                     table, len(cols),
                     f"{n_rows:,}" if n_rows is not None else "?")

            docs.append(build_table_doc(config.HIVE_DB, table, cols, n_rows))

            for col_name, col_type, comment in cols:
                samples: list[str] = []
                # Only sample for low-cardinality types — skip text/json/blobs
                if any(col_type.lower().startswith(t)
                       for t in ("string", "varchar", "char", "boolean", "int",
                                 "tinyint", "smallint", "bigint", "date")):
                    samples = sample_values(
                        cur, config.HIVE_DB, table, col_name,
                        config.SAMPLE_VALUES_LIMIT,
                    )
                docs.append(build_column_doc(
                    config.HIVE_DB, table, col_name, col_type, comment, samples,
                ))

        # Compute embeddings — same builder as embed_backfill.TEXT_BUILDERS
        log.info("Embedding %d docs …", len(docs))
        texts = [
            " | ".join(filter(None, [
                d.get("table_name"),
                d.get("column_name"),
                d.get("description"),
                d.get("synonyms"),
                " ".join(d.get("sample_values") or []) if isinstance(d.get("sample_values"), list) else None,
            ]))
            for d in docs
        ]
        vectors = embed(texts)
        for d, v in zip(docs, vectors):
            d["embedding"] = v

        log.info("Bulk upserting %d docs into %s …", len(docs), config.INDEX_CATALOG)
        ok, errors = bulk_upsert(
            os_client, config.INDEX_CATALOG, docs, id_field="doc_id",
        )
        log.info("  ✓ indexed=%d  errors=%d", ok, errors)
        return 0 if errors == 0 else 1

    finally:
        try:
            cur.close()
        finally:
            conn.close()


if __name__ == "__main__":
    sys.exit(index_all())
