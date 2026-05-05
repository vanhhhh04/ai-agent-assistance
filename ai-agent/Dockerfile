# DataFinch — AI Agent runtime image
# Slim Python 3.11 + system libs for sasl/cyrus (pyhive) + git (sentence-transformers cache).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/cache/huggingface \
    SENTENCE_TRANSFORMERS_HOME=/cache/sentence-transformers

# System dependencies:
#   - build-essential, gcc : compile asyncpg + thrift native bits
#   - libssl-dev, libffi-dev : python crypto deps used by some transitive packages
#   - curl : healthcheck
# NOTE: libsasl2-dev intentionally omitted — we connect to HiveServer2 with
# auth=NOSASL, which doesn't need libsasl2. If you switch to LDAP/Kerberos,
# add libsasl2-dev back AND install `pure-sasl` (pure Python, no compile).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libssl-dev \
        libffi-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better cache layering)
COPY ai-agent/requirements.txt /app/ai-agent/requirements.txt
RUN pip install -r /app/ai-agent/requirements.txt

# Pre-download the sentence-transformers model so first request isn't slow.
# Failure is non-fatal — container will download on first use instead.
RUN python -c "from sentence_transformers import SentenceTransformer; \
    SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')" \
    || echo "[warn] embedder pre-download failed; first request will be slower"

# Copy source — both ai-agent/ and opensearch/ packages
# (semantic_layer.py imports from the opensearch package next door)
COPY ai-agent/ /app/ai-agent/
COPY opensearch/ /app/opensearch/

WORKDIR /app/ai-agent

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:8000/api/health/ping || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
