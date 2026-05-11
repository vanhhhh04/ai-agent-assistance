"""
DataFinch — AI Data Assistant Backend.

Multi-agent NL → SQL → DataWarehouse system inspired by Uber's Finch.

Architecture (mapped to Finch components):
  - Generative AI Gateway     → core/llm_gateway.py
  - Supervisor Agent          → agents/supervisor.py
  - Data Retrieval Agents     → agents/retrieval/{hive,postgres}_agent.py
  - Semantic Layer (OpenSearch dataset metadata) → core/semantic_layer.py
  - Security/Guardrails       → core/guardrails.py
  - Schema Registry           → core/schema_cache.py (loaded at startup)

Entry point: `uvicorn main:app --reload --port 8000`
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from core.schema_cache import cache as schema_cache
from core.semantic_layer import semantic_layer
from core.settings import settings
from routers import health, query, schema

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("datafinch")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("DataFinch starting...")

    # 1. Warm the embedder model (so the first query isn't slow)
    await semantic_layer.warmup()

    # 2. Load both backend schemas into memory
    await schema_cache.load()

    log.info(
        "DataFinch ready · supervisor=%s · sql_writer=%s",
        settings.llm_model_supervisor, settings.llm_model_sql_writer,
    )
    yield
    log.info("DataFinch shutting down")


app = FastAPI(
    title="DataFinch — AI Data Assistant",
    description="Multi-agent NL→SQL system inspired by Uber Finch",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api/health", tags=["Health"])
app.include_router(schema.router, prefix="/api/schema", tags=["Schema"])
app.include_router(query.router,  prefix="/api/query",  tags=["Query"])


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/")


@app.get("/ai-data-assistant.jsx", include_in_schema=False)
async def serve_jsx():
    return FileResponse("ai-data-assistant.jsx", media_type="application/javascript")


# Serve static UI — mounted last so API routes take priority
app.mount("/ui", StaticFiles(directory="static", html=True), name="ui")
