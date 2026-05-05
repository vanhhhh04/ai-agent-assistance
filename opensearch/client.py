"""
OpenSearch — shared client + bulk helper.

Wraps opensearch-py so indexers don't repeat connection boilerplate.
"""

from __future__ import annotations

from typing import Iterable

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

from . import config


def get_client() -> OpenSearch:
    """
    Return an OpenSearch client configured from config.OPENSEARCH_URL.

    Single connection per process is fine for our scale (a few indexers
    that bulk-write a few hundred docs and exit).
    """
    auth = None
    if config.OPENSEARCH_USER and config.OPENSEARCH_PASSWORD:
        auth = (config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD)

    return OpenSearch(
        hosts=[config.OPENSEARCH_URL],
        http_auth=auth,
        verify_certs=False,
        ssl_show_warn=False,
        request_timeout=30,
    )


def bulk_upsert(
    client: OpenSearch,
    index: str,
    docs: Iterable[dict],
    id_field: str,
) -> tuple[int, int]:
    """
    Bulk index docs with explicit _id taken from `id_field`.
    Uses index action (not create) so re-runs overwrite cleanly — idempotent.

    Returns (success_count, error_count).
    """
    actions = (
        {
            "_op_type": "index",
            "_index":   index,
            "_id":      doc[id_field],
            "_source":  doc,
        }
        for doc in docs
    )
    success, errors = bulk(client, actions, raise_on_error=False, refresh=True)
    return success, len(errors) if isinstance(errors, list) else errors


def ping(client: OpenSearch) -> bool:
    """True if the cluster is reachable. Used by indexers as a preflight check."""
    try:
        return client.ping()
    except Exception:
        return False
