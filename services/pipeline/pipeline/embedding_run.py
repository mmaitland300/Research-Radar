from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.embedding_persistence import (
    EmbeddingCandidate,
    count_included_works_for_snapshot,
    count_missing_embedding_candidates,
    latest_corpus_snapshot_version_with_works,
    list_embedding_candidates,
    upsert_work_embeddings,
)
from pipeline.embedding_provider import (
    DEFAULT_OPENAI_EMBEDDING_MODEL,
    EmbeddingProvider,
    openai_embedding_provider_from_env,
)


@dataclass(frozen=True)
class EmbeddingRunSummary:
    corpus_snapshot_version: str
    embedding_version: str
    model: str
    total_included_works: int
    already_embedded_works: int
    missing_embedding_works: int
    candidate_works: int
    rows_written: int
    batch_count: int


def build_work_embedding_text(title: str, abstract: str | None) -> str:
    clean_title = title.strip()
    if not clean_title:
        raise ValueError("Embedding text requires a non-empty title.")

    clean_abstract = (abstract or "").strip()
    if not clean_abstract:
        return f"Title: {clean_title}"
    return f"Title: {clean_title}\n\nAbstract: {clean_abstract}"


def _batched(candidates: Sequence[EmbeddingCandidate], batch_size: int) -> list[list[EmbeddingCandidate]]:
    return [list(candidates[index : index + batch_size]) for index in range(0, len(candidates), batch_size)]


def execute_embedding_run(
    *,
    database_url: str | None = None,
    embedding_version: str,
    corpus_snapshot_version: str | None = None,
    model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    batch_size: int = 32,
    limit: int | None = None,
    provider: EmbeddingProvider | None = None,
) -> EmbeddingRunSummary:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided.")

    dsn = database_url or database_url_from_env()
    active_provider = provider or openai_embedding_provider_from_env(model=model)

    with psycopg.connect(dsn, autocommit=False) as conn:
        snapshot = corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
        if snapshot is None:
            raise RuntimeError(
                "No corpus_snapshot_version with included works found. "
                "Pass --corpus-snapshot-version or ingest data first."
            )
        total_included_works = count_included_works_for_snapshot(conn, snapshot)
        if total_included_works <= 0:
            raise RuntimeError(
                f"No included works for corpus_snapshot_version={snapshot!r}; nothing to embed."
            )
        missing_embedding_works = count_missing_embedding_candidates(
            conn,
            corpus_snapshot_version=snapshot,
            embedding_version=embedding_version,
        )
        candidates = list_embedding_candidates(
            conn,
            corpus_snapshot_version=snapshot,
            embedding_version=embedding_version,
            limit=limit,
        )

    if not candidates:
        return EmbeddingRunSummary(
            corpus_snapshot_version=snapshot,
            embedding_version=embedding_version,
            model=model,
            total_included_works=total_included_works,
            already_embedded_works=total_included_works - missing_embedding_works,
            missing_embedding_works=missing_embedding_works,
            candidate_works=0,
            rows_written=0,
            batch_count=0,
        )

    rows_written = 0
    batch_count = 0
    with psycopg.connect(dsn, autocommit=False) as conn:
        for batch in _batched(candidates, batch_size):
            texts = [build_work_embedding_text(candidate.title, candidate.abstract) for candidate in batch]
            vectors = active_provider.embed_texts(texts)
            if len(vectors) != len(batch):
                raise RuntimeError(
                    "Embedding provider returned a vector count mismatch: "
                    f"expected {len(batch)}, got {len(vectors)}."
                )
            upsert_work_embeddings(
                conn,
                embedding_version=embedding_version,
                rows=[(candidate.work_id, vector) for candidate, vector in zip(batch, vectors)],
            )
            conn.commit()
            rows_written += len(batch)
            batch_count += 1

    return EmbeddingRunSummary(
        corpus_snapshot_version=snapshot,
        embedding_version=embedding_version,
        model=model,
        total_included_works=total_included_works,
        already_embedded_works=total_included_works - missing_embedding_works,
        missing_embedding_works=missing_embedding_works,
        candidate_works=len(candidates),
        rows_written=rows_written,
        batch_count=batch_count,
    )
