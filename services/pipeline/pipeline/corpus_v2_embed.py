"""Generate versioned embeddings for a hydrated corpus-v2 snapshot."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.embedding_persistence import upsert_work_embeddings
from pipeline.embedding_provider import (
    DEFAULT_OPENAI_EMBEDDING_MODEL,
    EXPECTED_EMBEDDING_DIMENSIONS,
    EmbeddingProvider,
    openai_embedding_provider_from_env,
)
from pipeline.openalex_text import clean_openalex_text

OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
TEXT_SOURCE = "title_abstract"
PROVIDER = "openai"
SKIP_COUNT = 0
FAIL_COUNT = 0


class CorpusV2EmbedError(RuntimeError):
    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _WorkRow:
    work_id: int
    title: str
    abstract: str | None
    work_type: str
    language: str


@dataclass(frozen=True)
class _PreparedEmbedding:
    work_id: int
    text: str


def run_corpus_v2_embed(
    *,
    snapshot_version: str,
    embedding_version: str,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
    model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    batch_size: int = 32,
    replace: bool = False,
    provider: EmbeddingProvider | None = None,
) -> dict[str, Any]:
    snapshot_version = (snapshot_version or "").strip()
    embedding_version = (embedding_version or "").strip()
    model = (model or "").strip() or DEFAULT_OPENAI_EMBEDDING_MODEL
    if not snapshot_version:
        raise CorpusV2EmbedError("--snapshot-version is required and must not be blank", code=2)
    if not embedding_version:
        raise CorpusV2EmbedError("--embedding-version is required and must not be blank", code=2)
    if embedding_version.casefold().startswith("v1"):
        raise CorpusV2EmbedError("refusing to reuse a v1 embedding_version for corpus-v2 embedding", code=2)
    if batch_size <= 0:
        raise CorpusV2EmbedError("--batch-size must be positive", code=2)

    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, autocommit=False) as conn:
        _assert_snapshot_exists(conn, snapshot_version)
        works = _load_target_works(conn, snapshot_version=snapshot_version)
        if not works:
            raise CorpusV2EmbedError(f"snapshot has no included works: {snapshot_version}", code=2)
        blockers = _readiness_blockers(works)
        if any(value > 0 for value in blockers.values()):
            raise CorpusV2EmbedError(_format_blocker_message(blockers), code=2)
        existing_total = _count_embedding_version_rows(conn, embedding_version=embedding_version)
        existing_target = _count_target_embedding_version_rows(
            conn,
            snapshot_version=snapshot_version,
            embedding_version=embedding_version,
        )
        if existing_total > 0 and not replace:
            raise CorpusV2EmbedError(
                f"embedding_version already exists: {embedding_version}; pass --replace to overwrite target snapshot rows",
                code=2,
            )
        if replace and existing_total > existing_target:
            raise CorpusV2EmbedError(
                "embedding_version already exists outside the target snapshot; choose a new embedding_version",
                code=2,
            )
        if replace and existing_target > 0:
            _delete_target_embeddings(
                conn,
                snapshot_version=snapshot_version,
                embedding_version=embedding_version,
            )
            conn.commit()

    active_provider = provider or _openai_provider_for_corpus_v2(model=model)
    prepared = [
        _PreparedEmbedding(work_id=row.work_id, text=build_corpus_v2_embedding_text(row.title, row.abstract))
        for row in works
    ]
    text_sha256 = input_text_sha256(prepared)
    rows_written = 0
    batches_committed = 0
    embedding_dimension = _expected_dimension(active_provider)

    with psycopg.connect(dsn, autocommit=False) as conn:
        try:
            for batch in _batched(prepared, batch_size):
                vectors = active_provider.embed_texts([item.text for item in batch])
                dimension = _validate_vectors(vectors, expected_dimension=_expected_dimension(active_provider))
                embedding_dimension = dimension
                upsert_work_embeddings(
                    conn,
                    embedding_version=embedding_version,
                    rows=[(item.work_id, vector) for item, vector in zip(batch, vectors)],
                )
                conn.commit()
                rows_written += len(batch)
                batches_committed += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            raise CorpusV2EmbedError(f"corpus-v2 embedding failed: {exc}", code=1) from exc

    summary = {
        "snapshot_version": snapshot_version,
        "embedding_version": embedding_version,
        "works_considered_count": len(works),
        "embedding_ready_count": len(prepared),
        "embedded_count": rows_written,
        "skipped_count": SKIP_COUNT,
        "failed_count": FAIL_COUNT,
        "embedding_dimension": embedding_dimension,
        "model": model,
        "provider": PROVIDER,
        "text_source": TEXT_SOURCE,
        "input_text_count": len(prepared),
        "input_text_sha256": text_sha256,
        "batches_committed": batches_committed,
        "replaced_existing_count": existing_target if replace else 0,
        "warnings": [
            "This is an ML artifact generation step only; no clustering, ranking, or bridge validation was run.",
            "Old/new corpus metrics are not same-pool comparable.",
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(render_embedding_summary_markdown(summary), encoding="utf-8", newline="\n")
    return summary


def build_corpus_v2_embedding_text(title: str, abstract: str | None) -> str:
    clean_title = clean_openalex_text(title)
    clean_abstract = clean_openalex_text(abstract)
    if not clean_title:
        raise ValueError("Embedding text requires a non-empty title.")
    if not clean_abstract:
        raise ValueError("Corpus-v2 embedding text requires a non-empty abstract.")
    return f"Title: {clean_title}\n\nAbstract: {clean_abstract}"


def input_text_sha256(rows: Sequence[_PreparedEmbedding]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(str(row.work_id).encode("utf-8"))
        digest.update(b"\0")
        digest.update(row.text.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def render_embedding_summary_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Corpus v2 embedding coverage summary",
        "",
        "This is an ML artifact generation step for title+abstract embeddings.",
        "",
        f"- **snapshot_version:** `{summary.get('snapshot_version')}`",
        f"- **embedding_version:** `{summary.get('embedding_version')}`",
        f"- **works_considered_count:** `{summary.get('works_considered_count')}`",
        f"- **embedding_ready_count:** `{summary.get('embedding_ready_count')}`",
        f"- **embedded_count:** `{summary.get('embedded_count')}`",
        f"- **skipped_count:** `{summary.get('skipped_count')}`",
        f"- **failed_count:** `{summary.get('failed_count')}`",
        f"- **embedding_dimension:** `{summary.get('embedding_dimension')}`",
        f"- **provider:** `{summary.get('provider')}`",
        f"- **model:** `{summary.get('model')}`",
        f"- **text_source:** `{summary.get('text_source')}`",
        f"- **input_text_count:** `{summary.get('input_text_count')}`",
        f"- **input_text_sha256:** `{summary.get('input_text_sha256')}`",
        "",
        "## Caveats",
        "",
        "- This is not clustering.",
        "- This is not ranking.",
        "- This is not bridge validation.",
        "- Old/new corpus metrics are not same-pool comparable.",
        "",
        "## Warnings",
        "",
    ]
    warnings = list(summary.get("warnings") or [])
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def _openai_provider_for_corpus_v2(*, model: str) -> EmbeddingProvider:
    if not os.environ.get(OPENAI_API_KEY_ENV):
        raise CorpusV2EmbedError(
            f"{OPENAI_API_KEY_ENV} is required for corpus-v2-embed; set it in the environment before running",
            code=2,
        )
    return openai_embedding_provider_from_env(model=model)


def _assert_snapshot_exists(conn: psycopg.Connection, snapshot_version: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    if row is None:
        raise CorpusV2EmbedError(f"snapshot_version not found: {snapshot_version}", code=2)


def _load_target_works(conn: psycopg.Connection, *, snapshot_version: str) -> list[_WorkRow]:
    rows = conn.execute(
        """
        SELECT id, title, abstract, type, language
        FROM works
        WHERE corpus_snapshot_version = %s
          AND inclusion_status = 'included'
        ORDER BY id
        """,
        (snapshot_version,),
    ).fetchall()
    return [
        _WorkRow(
            work_id=int(row[0]),
            title=str(row[1] or ""),
            abstract=row[2] if isinstance(row[2], str) else None,
            work_type=str(row[3] or ""),
            language=str(row[4] or ""),
        )
        for row in rows
    ]


def _readiness_blockers(rows: Sequence[_WorkRow]) -> dict[str, int]:
    return {
        "missing_title_count": sum(1 for row in rows if not clean_openalex_text(row.title)),
        "missing_abstract_count": sum(1 for row in rows if not clean_openalex_text(row.abstract)),
        "unknown_type_count": sum(1 for row in rows if _is_unknown_type(row.work_type)),
        "missing_language_count": sum(1 for row in rows if not clean_openalex_text(row.language)),
    }


def _format_blocker_message(blockers: Mapping[str, int]) -> str:
    active = {key: value for key, value in blockers.items() if value > 0}
    parts = ", ".join(f"{key}={value}" for key, value in sorted(active.items()))
    return f"snapshot is not embedding-ready; {parts}"


def _is_unknown_type(value: str) -> bool:
    raw = (value or "").strip().casefold()
    return (not raw) or raw == "unknown"


def _count_embedding_version_rows(conn: psycopg.Connection, *, embedding_version: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM embeddings WHERE embedding_version = %s",
        (embedding_version,),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _count_target_embedding_version_rows(
    conn: psycopg.Connection,
    *,
    snapshot_version: str,
    embedding_version: str,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM embeddings e
        JOIN works w ON w.id = e.work_id
        WHERE e.embedding_version = %s
          AND w.corpus_snapshot_version = %s
        """,
        (embedding_version, snapshot_version),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _delete_target_embeddings(
    conn: psycopg.Connection,
    *,
    snapshot_version: str,
    embedding_version: str,
) -> None:
    conn.execute(
        """
        DELETE FROM embeddings e
        USING works w
        WHERE e.work_id = w.id
          AND e.embedding_version = %s
          AND w.corpus_snapshot_version = %s
        """,
        (embedding_version, snapshot_version),
    )


def _batched(rows: Sequence[_PreparedEmbedding], batch_size: int) -> list[list[_PreparedEmbedding]]:
    return [list(rows[index : index + batch_size]) for index in range(0, len(rows), batch_size)]


def _expected_dimension(provider: EmbeddingProvider) -> int | None:
    raw = getattr(provider, "expected_dimensions", None)
    if isinstance(raw, int) and raw > 0:
        return raw
    return EXPECTED_EMBEDDING_DIMENSIONS if provider.__class__.__name__ == "OpenAIEmbeddingProvider" else None


def _validate_vectors(vectors: Sequence[Sequence[float]], *, expected_dimension: int | None) -> int:
    if not vectors:
        raise RuntimeError("embedding provider returned no vectors for a non-empty batch")
    dimension = len(vectors[0])
    if dimension <= 0:
        raise RuntimeError("embedding provider returned an empty vector")
    for vector in vectors:
        if len(vector) != dimension:
            raise RuntimeError("embedding provider returned inconsistent vector dimensions")
    if expected_dimension is not None and dimension != expected_dimension:
        raise RuntimeError(f"embedding dimension mismatch: expected {expected_dimension}, got {dimension}")
    return dimension


__all__ = [
    "CorpusV2EmbedError",
    "build_corpus_v2_embedding_text",
    "input_text_sha256",
    "render_embedding_summary_markdown",
    "run_corpus_v2_embed",
]
