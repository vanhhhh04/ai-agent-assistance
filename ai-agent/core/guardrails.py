"""
core/guardrails.py — Security & safety layer.

Every SQL produced by the SQL Writer Agent passes through validate_sql()
before it ever reaches the warehouse. If validation fails, the SQL is
rejected and a clean error is bubbled back to the user; the LLM may be
re-prompted with the failure reason.

Checks:
  1. Forbidden keywords        — DDL / DML must never run from a chat agent
  2. Single-statement only     — block ;-injection that smuggles a second query
  3. Referenced tables exist   — using either Hive catalog or the in-memory
                                  schema fallback. Catches column-name hallucinations
                                  *partially* (only at the table level — full column
                                  validation is left to the database itself, since
                                  parsing arbitrary column references in SQL is brittle)
  4. Join count cap            — prevents pathological N-way joins
  5. LIMIT on non-aggregations — protects the warehouse from full table scans
  6. PII column denial         — if the query selects a PII-tagged column from
                                  the catalog, gate behind a flag (today: warn)

Why a separate module instead of inlining in sql_writer.py?
  - Multiple agents (SQL Writer, future Python-Notebook agent, future ad-hoc
    explainer) should all run the same validators
  - Guardrails are security-critical — easier to audit when isolated
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

log = logging.getLogger("guardrails")

# ─────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────
FORBIDDEN_KEYWORDS = [
    "DELETE", "UPDATE", "INSERT", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
    "MERGE", "REPLACE", "RENAME", "COMMENT ON",
]
MAX_JOINS = 6
MAX_UNION = 3


# ─────────────────────────────────────────────────────────────
#  Result type
# ─────────────────────────────────────────────────────────────
@dataclass
class ValidationResult:
    valid: bool
    error: str | None = None
    warnings: list[str] | None = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


# ─────────────────────────────────────────────────────────────
#  Public API
# ─────────────────────────────────────────────────────────────
def validate_sql(
    sql: str,
    *,
    known_tables: set[str] | None = None,
    pii_columns: set[str] | None = None,
    allow_pii: bool = False,
) -> ValidationResult:
    """
    `known_tables` — set of canonical (lowercase, no db prefix) table names.
    `pii_columns`  — "table.column" strings flagged PII in the catalog.
    """
    if not sql or not sql.strip():
        return ValidationResult(False, "Empty SQL")

    cleaned = _strip_comments(sql).strip().rstrip(";")
    upper = cleaned.upper()
    warnings: list[str] = []

    # 1. Reject everything that isn't a SELECT/WITH
    head = upper.lstrip()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
        return ValidationResult(False, "Only SELECT / WITH statements are allowed")

    # 2. Forbidden keywords (word-boundary match — "SELECT" containing "SELECT" is fine)
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", upper):
            return ValidationResult(False, f"Forbidden keyword: {kw}")

    # 3. Single-statement only — block ;-injection
    if ";" in cleaned:
        return ValidationResult(False, "Multiple statements are not allowed (semicolon detected)")

    # 4. Join count cap
    join_count = len(re.findall(r"\bJOIN\b", upper))
    if join_count > MAX_JOINS:
        return ValidationResult(False, f"Too many JOINs ({join_count}); max is {MAX_JOINS}")

    union_count = len(re.findall(r"\bUNION\b", upper))
    if union_count > MAX_UNION:
        return ValidationResult(False, f"Too many UNIONs ({union_count}); max is {MAX_UNION}")

    # 5. Table existence check (best-effort — catalog may be partial)
    if known_tables:
        referenced = _extract_tables(cleaned)
        unknown = [t for t in referenced if t not in known_tables]
        if unknown:
            return ValidationResult(
                False,
                f"Unknown table(s): {', '.join(unknown)}. Available: {', '.join(sorted(known_tables)[:10])}…",
            )

    # 6. LIMIT enforcement (skip if aggregating — SUM/COUNT/AVG return one row anyway)
    is_aggregated = bool(re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", upper))
    has_group_by = "GROUP BY" in upper
    has_limit = "LIMIT" in upper
    if not (is_aggregated or has_group_by or has_limit):
        return ValidationResult(
            False,
            "Query must include LIMIT (or aggregate via SUM/COUNT/AVG/GROUP BY). "
            "This protects the warehouse from full table scans.",
        )

    # 7. PII access gate (warn-only by default)
    if pii_columns and not allow_pii:
        accessed_pii = _detect_pii_access(cleaned, pii_columns)
        if accessed_pii:
            warnings.append(
                f"Query touches PII columns: {', '.join(accessed_pii)}. "
                "Allow only with explicit clearance.",
            )

    return ValidationResult(True, None, warnings)


# ─────────────────────────────────────────────────────────────
#  Internals
# ─────────────────────────────────────────────────────────────
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_comments(sql: str) -> str:
    return _BLOCK_COMMENT.sub("", _LINE_COMMENT.sub("", sql))


_TABLE_RE = re.compile(r"(?:FROM|JOIN)\s+([\w\.]+)", re.IGNORECASE)


def _extract_tables(sql: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for m in _TABLE_RE.finditer(sql):
        bare = m.group(1).lower().split(".")[-1]
        if bare in ("lateral", "unnest"):
            continue
        if bare not in seen:
            seen.add(bare)
            out.append(bare)
    return out


def _detect_pii_access(sql: str, pii_columns: set[str]) -> list[str]:
    """Loose match — true positive if "<table>.<col>" or bare "<col>" appears."""
    upper = sql.upper()
    flagged: list[str] = []
    for full in pii_columns:
        col = full.split(".")[-1]
        if col.upper() in upper:
            flagged.append(full)
    return flagged
