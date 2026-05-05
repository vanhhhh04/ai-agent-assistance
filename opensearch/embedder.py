"""
Embedding service — encode text into 768-d vectors for kNN search.

Model: paraphrase-multilingual-mpnet-base-v2
  - 768 dimensions (matches the kNN field declared in Part 1 mappings)
  - Multilingual including Vietnamese
  - ~420 MB on disk, runs CPU OK (~50ms/text after warmup)

Lazy-loaded on first call: indexers and the agent module import this without
paying the model load cost up front. In-memory cache de-duplicates identical
text across calls in the same process.

Usage:
    from opensearch.embedder import embed, embed_one

    vec  = embed_one("Doanh thu theo brand")        # list[float], len 768
    vecs = embed(["question 1", "question 2"])      # list[list[float]]
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from typing import Iterable

log = logging.getLogger("embedder")

MODEL_NAME = os.getenv(
    "EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-mpnet-base-v2",
)

# Singleton model + lock for thread-safe lazy load
_model = None
_model_lock = threading.Lock()

# In-memory cache: sha1(text) -> vector
_cache: dict[str, list[float]] = {}
_CACHE_MAX = int(os.getenv("EMBEDDING_CACHE_MAX", "10000"))


def _load_model():
    """Load the sentence-transformers model. Idempotent + thread-safe."""
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is not None:
            return _model
        log.info("Loading embedding model %s …", MODEL_NAME)
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(MODEL_NAME)
        log.info("  model loaded (dim=%d)", _model.get_sentence_embedding_dimension())
    return _model


def _key(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def embed(texts: Iterable[str]) -> list[list[float]]:
    """
    Batch embed. Cache hits are returned directly; cache misses are computed
    in one batch (fewer torch invocations = faster).
    """
    texts_list = list(texts)
    if not texts_list:
        return []

    # 1. Look up cache
    keys = [_key(t) for t in texts_list]
    cached = [_cache.get(k) for k in keys]
    miss_idx = [i for i, v in enumerate(cached) if v is None]

    # 2. Compute misses in one batch
    if miss_idx:
        model = _load_model()
        batch = [texts_list[i] for i in miss_idx]
        # normalize_embeddings=True so cosine ≡ dot product (faster on OpenSearch)
        vecs = model.encode(
            batch,
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        for idx, vec in zip(miss_idx, vecs):
            v = vec.tolist()
            cached[idx] = v
            if len(_cache) < _CACHE_MAX:
                _cache[keys[idx]] = v

    return cached  # type: ignore[return-value]


def embed_one(text: str) -> list[float]:
    return embed([text])[0]


def warmup() -> None:
    """Force model load now (so subsequent calls are fast)."""
    embed(["warmup"])


def cache_size() -> int:
    return len(_cache)


def clear_cache() -> None:
    _cache.clear()


def get_dimension() -> int:
    """Returns the embedding dimension (loads the model if needed)."""
    return _load_model().get_sentence_embedding_dimension()
