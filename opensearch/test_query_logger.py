"""
Smoke test for query_logger:
  1. Write a fake successful query
  2. Write a fake failed query
  3. Update one with thumbs_up
  4. Search for them by nl_question
  5. Cleanup (delete the test docs)

Run:  python -m opensearch.test_query_logger
"""

from __future__ import annotations

import sys
import time

from . import config
from .client import get_client
from .query_logger import log_query, update_feedback


def main() -> int:
    client = get_client()

    print("[1/5] Logging a successful query …")
    qid_ok = log_query(
        nl_question="doanh thu tháng 4/2026 theo brand",
        generated_sql="SELECT brand, SUM(item_total) FROM gold.fact_sales WHERE order_year=2026 AND order_month=4 GROUP BY brand",
        status="success",
        tables_used=["gold.fact_sales"],
        columns_used=["brand", "item_total", "order_year", "order_month"],
        row_count=42,
        exec_ms=850,
        user_id="test_user",
        session_id="smoke_session_1",
        model="claude-opus-4-7",
        retrieval_top_k={"catalog": 5, "docs": 3, "query_log": 0},
    )
    assert qid_ok, "log_query should return a query_id"
    print(f"      query_id = {qid_ok}")

    print("[2/5] Logging a failed query …")
    qid_fail = log_query(
        nl_question="xóa hết bảng sales",
        generated_sql="DROP TABLE gold.fact_sales",
        status="guardrail_fail",
        error_message="DDL operations not allowed",
        user_id="test_user",
        session_id="smoke_session_1",
        model="claude-opus-4-7",
    )
    assert qid_fail, "log_query should return a query_id"
    print(f"      query_id = {qid_fail}")

    print("[3/5] Updating success doc with thumbs_up …")
    ok = update_feedback(qid_ok, thumbs_up=True, feedback_text="đúng rồi")
    assert ok, "update_feedback should succeed"
    print(f"      updated = {ok}")

    # Refresh index so search sees the new docs
    client.indices.refresh(index=config.INDEX_QUERY_LOG)

    print("[4/5] Searching by nl_question 'doanh thu' …")
    res = client.search(
        index=config.INDEX_QUERY_LOG,
        body={
            "size": 5,
            "query": {"match": {"nl_question": "doanh thu"}},
            "_source": ["query_id", "nl_question", "status", "user_thumbs_up"],
        },
    )
    hits = res["hits"]["hits"]
    print(f"      hits = {len(hits)}")
    for h in hits:
        s = h["_source"]
        print(f"        score={h['_score']:.2f}  status={s['status']}  thumbs_up={s.get('user_thumbs_up')}  q='{s['nl_question']}'")

    found = any(h["_id"] == qid_ok for h in hits)
    assert found, "the success doc should be in search results"

    print("[5/5] Cleanup test docs …")
    for qid in (qid_ok, qid_fail):
        client.delete(index=config.INDEX_QUERY_LOG, id=qid, refresh=True)
        print(f"      deleted {qid}")

    print("\n[OK] all smoke tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
