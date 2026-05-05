"""
Table-docs indexer — Markdown files → table_docs index.

Walks `opensearch/docs/*.md`, parses each as YAML frontmatter + markdown body,
splits long bodies into sections (by `## `), upserts into table_docs.

Idempotent: doc_id derived from `source_path + section_index` so re-runs replace
in place. Run after editing markdown.

Usage:
  python -m opensearch.indexers.docs_indexer
  python -m opensearch.indexers.docs_indexer  --docs-dir path/to/dir
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import frontmatter

from .. import config
from ..client import get_client, bulk_upsert, ping
from ..embedder import embed

log = logging.getLogger("docs_indexer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

DOCS_DIR_DEFAULT = Path(__file__).resolve().parent.parent / "docs"


# ─────────────────────────────────────────────────────────────
#  Section splitter — split markdown by `## ` headings
# ─────────────────────────────────────────────────────────────
SECTION_RE = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)


def split_sections(body: str) -> list[tuple[str, str]]:
    """
    Split a markdown body into (section_title, section_text) tuples.
    The text BEFORE the first `## ` becomes the "overview" section.
    Sections shorter than 40 chars are merged into the next one.
    """
    matches = list(SECTION_RE.finditer(body))
    if not matches:
        return [("overview", body.strip())]

    sections: list[tuple[str, str]] = []
    head = body[:matches[0].start()].strip()
    if head:
        sections.append(("overview", head))

    for i, m in enumerate(matches):
        title = m.group(1).strip()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        text = body[start:end].strip()
        if text:
            sections.append((title, text))
    return sections


# ─────────────────────────────────────────────────────────────
#  Git SHA — for traceability
# ─────────────────────────────────────────────────────────────
def current_git_sha() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True, timeout=5,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ─────────────────────────────────────────────────────────────
#  Doc builder
# ─────────────────────────────────────────────────────────────
def build_docs(md_path: Path, git_sha: str) -> list[dict]:
    """One markdown file → 1+ docs (one per `## ` section)."""
    post = frontmatter.load(md_path)
    fm = post.metadata
    body = post.content

    table_name = fm.get("table_name") or md_path.stem
    base_section = fm.get("section", "overview")
    tags = fm.get("tags", []) or []
    if isinstance(tags, str):
        tags = [tags]

    sections = split_sections(body)
    docs: list[dict] = []
    for i, (title, text) in enumerate(sections):
        # Use a content hash so editing a section produces a new doc_id ONLY if
        # the section title or path changed — avoids stale duplicates.
        section_slug = re.sub(r"\W+", "_", title.lower()).strip("_") or f"sec{i}"
        doc_id = f"{md_path.name}::{section_slug}"
        docs.append({
            "doc_id":      doc_id,
            "table_name":  table_name,
            "section":     base_section if i == 0 else section_slug,
            "title":       title,
            "content":     text,
            "examples":    "\n".join(re.findall(r"```sql\s+(.*?)\s+```", text, re.DOTALL)),
            "tags":        tags,
            "source_path": str(md_path.relative_to(md_path.parent.parent.parent)),
            "git_sha":     git_sha,
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
        })
    return docs


# ─────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────
def index_all(docs_dir: Path) -> int:
    if not docs_dir.exists():
        log.error("Docs dir not found: %s", docs_dir)
        return 1

    md_files = sorted(docs_dir.glob("*.md"))
    if not md_files:
        log.warning("No *.md in %s — nothing to index", docs_dir)
        return 0

    os_client = get_client()
    if not ping(os_client):
        log.error("OpenSearch not reachable at %s", config.OPENSEARCH_URL)
        return 1

    sha = current_git_sha()
    log.info("git_sha=%s  files=%d", sha, len(md_files))

    all_docs: list[dict] = []
    for md in md_files:
        docs = build_docs(md, sha)
        log.info("  %s  →  %d sections", md.name, len(docs))
        all_docs.extend(docs)

    # Compute embeddings — text built same way as embed_backfill.TEXT_BUILDERS
    log.info("Embedding %d docs …", len(all_docs))
    texts = [
        " | ".join(filter(None, [d.get("title"), d.get("table_name"), d.get("content")]))
        for d in all_docs
    ]
    vectors = embed(texts)
    for d, v in zip(all_docs, vectors):
        d["embedding"] = v

    log.info("Bulk upserting %d docs into %s …", len(all_docs), config.INDEX_TABLE_DOCS)
    ok, errors = bulk_upsert(
        os_client, config.INDEX_TABLE_DOCS, all_docs, id_field="doc_id",
    )
    log.info("  ✓ indexed=%d  errors=%d", ok, errors)
    return 0 if errors == 0 else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs-dir", type=Path, default=DOCS_DIR_DEFAULT)
    args = ap.parse_args()
    sys.exit(index_all(args.docs_dir))


if __name__ == "__main__":
    main()
