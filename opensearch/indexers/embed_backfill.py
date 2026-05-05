"""
Embedding backfill — populate the `embedding` field on docs that don't have one.

Scans each index for docs missing `embedding`, builds the text-to-embed from
fields that depend on doc_type, batches into the embedder, then bulk updates.

Idempotent: docs that already have an embedding are skipped (unless --force).

Usage:
  python -m opensearch.indexers.embed_backfill                  # all indices
  python -m opensearch.indexers.embed_backfill --index table_docs
  python -m opensearch.indexers.embed_backfill --force          # re-embed all
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable

from opensearchpy.helpers import scan, bulk

from .. import config
from ..client import get_client, ping
from ..embedder import embed

log = logging.getLogger("embed_backfill")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

# Default text fields per index — what we concat to feed the embedder
TEXT_BUILDERS = {
    config.INDEX_CATALOG: lambda src: " | ".join(filter(None, [
        src.get("table_name"),
        src.get("column_name"),
        src.get("description"),
        src.get("synonyms"),
        " ".join(src.get("sample_values") or []) if isinstance(src.get("sample_values"), list) else None,
    ])),
    config.INDEX_TABLE_DOCS: lambda src: " | ".join(filter(None, [
        src.get("title"),
        src.get("table_name"),
        src.get("content"),
    ])),
    config.INDEX_QUERY_LOG: lambda src: src.get("nl_question") or "",
}


def fetch_docs(client, index: str, force: bool) -> Iterable[dict]:
    """
    Stream all docs in an index. If force=False, skip docs that already have
    a non-null embedding (filter at OpenSearch level).
    """
    if force:
        query = {"query": {"match_all": {}}}
    else:
        # Find docs where `embedding` field is missing or empty
        query = {
            "query": {
                "bool": {
                    "must_not": [{"exists": {"field": "embedding"}}],
                }
            }
        }
    yield from scan(client, index=index, query=query, _source=True, size=200)


def backfill_index(client, index: str, force: bool) -> tuple[int, int]:
    """Returns (n_embedded, n_errors)."""
    builder = TEXT_BUILDERS[index]
    docs = list(fetch_docs(client, index, force))
    if not docs:
        log.info("  %s: 0 docs to embed", index)
        return 0, 0

    texts = [builder(d["_source"]) for d in docs]
    log.info("  %s: %d docs → embedding …", index, len(docs))
    vectors = embed(texts)

    actions = (
        {
            "_op_type": "update",
            "_index":   index,
            "_id":      d["_id"],
            "doc":      {"embedding": v},
        }
        for d, v in zip(docs, vectors)
    )
    success, errors = bulk(client, actions, raise_on_error=False, refresh=True)
    n_err = len(errors) if isinstance(errors, list) else int(errors or 0)
    log.info("  %s: embedded=%d errors=%d", index, success, n_err)
    return success, n_err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", choices=list(TEXT_BUILDERS.keys()),
                    help="Embed only one index (default: all 3)")
    ap.add_argument("--force", action="store_true",
                    help="Re-embed even docs that already have an embedding")
    args = ap.parse_args()

    client = get_client()
    if not ping(client):
        log.error("OpenSearch not reachable at %s", config.OPENSEARCH_URL)
        return 1

    indices = [args.index] if args.index else list(TEXT_BUILDERS.keys())
    total_ok, total_err = 0, 0
    for idx in indices:
        ok, err = backfill_index(client, idx, args.force)
        total_ok += ok; total_err += err

    log.info("Done. total embedded=%d errors=%d", total_ok, total_err)
    return 0 if total_err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
