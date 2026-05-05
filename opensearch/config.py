"""
OpenSearch — shared config for Finch retrieval layer.

URL/credentials read from env so the same module works:
  - On the host (running indexers from a dev shell)
  - Inside a container on the `dataplatform` network (where the host is "opensearch")

Defaults assume local dev with security disabled (Part 1 setup).
"""

import os

# Inside the docker network, the service is reachable as http://opensearch:9200
# From the host, it's exposed on http://localhost:9200
OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://localhost:9200")

# Optional auth — security plugin is disabled in dev, so these are usually empty
OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER")     or None
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD") or None

# Index names — single source of truth for indexers and the agent
INDEX_CATALOG    = "finch_catalog"
INDEX_TABLE_DOCS = "table_docs"
INDEX_QUERY_LOG  = "query_log"

# Embedding dimension — must match what the mapping declared in Part 1
EMBEDDING_DIM = 768

# HiveServer2 — used by catalog_indexer to discover Gold tables
HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DB   = os.getenv("HIVE_DB",   "gold")

# Maximum distinct sample values to record per column
SAMPLE_VALUES_LIMIT = int(os.getenv("SAMPLE_VALUES_LIMIT", "20"))
