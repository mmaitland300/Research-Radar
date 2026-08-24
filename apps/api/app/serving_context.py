"""Resolve one coherent ranking, snapshot, embedding, and scorer context."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import database_url_from_env
from app.public_release_repo import (
    MaterializedRunContext,
    PublicReleasePromotion,
    fetch_latest_public_release_promotion,
    fetch_succeeded_materialized_run,
    inspect_public_release_serveability,
)

ServingContextSource = Literal[
    "explicit_run",
    "explicit_constraints",
    "active_promotion",
    "legacy_fallback",
]


class ServingContextError(RuntimeError):
    """Base error for serving-context resolution."""


class ServingContextNotFoundError(ServingContextError):
    """No succeeded materialized run matched the requested context."""

    def __init__(
        self,
        *,
        ranking_run_id: str | None = None,
        corpus_snapshot_version: str | None = None,
        ranking_version: str | None = None,
    ) -> None:
        self.ranking_run_id = ranking_run_id
        self.corpus_snapshot_version = corpus_snapshot_version
        self.ranking_version = ranking_version
        selectors = {
            key: value
            for key, value in (
                ("ranking_run_id", ranking_run_id),
                ("corpus_snapshot_version", corpus_snapshot_version),
                ("ranking_version", ranking_version),
            )
            if value is not None
        }
        detail = ", ".join(f"{key}={value!r}" for key, value in selectors.items())
        super().__init__(
            f"No succeeded materialized ranking run matched {detail}."
            if detail
            else "No succeeded materialized ranking run is available."
        )


class ServingContextUnavailableError(ServingContextError):
    """The configured public release exists but cannot be served safely."""

    def __init__(
        self,
        promotion: PublicReleasePromotion,
        *,
        failures: tuple[str, ...],
    ) -> None:
        self.promotion_id = promotion.promotion_id
        self.ranking_run_id = promotion.run.ranking_run_id
        self.failures = failures
        detail = ", ".join(failures) if failures else "unknown validation failure"
        super().__init__(
            "Active public release "
            f"{promotion.promotion_id} ({promotion.run.ranking_run_id}) is unavailable: {detail}."
        )


@dataclass(frozen=True)
class ServingContext:
    """A resolved immutable run plus how it was selected."""

    run: MaterializedRunContext
    source: ServingContextSource
    promotion_id: int | None = None

    @property
    def ranking_run_id(self) -> str:
        return self.run.ranking_run_id

    @property
    def ranking_version(self) -> str:
        return self.run.ranking_version

    @property
    def corpus_snapshot_version(self) -> str:
        return self.run.corpus_snapshot_version

    @property
    def embedding_version(self) -> str:
        return self.run.embedding_version

    @property
    def is_active_promotion(self) -> bool:
        return self.source == "active_promotion"


def _selector(value: str | None) -> str | None:
    selected = (value or "").strip()
    return selected or None


def _latest_constrained_run(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str | None,
    ranking_version: str | None,
) -> MaterializedRunContext | None:
    predicates = ["status = 'succeeded'"]
    params: list[str] = []
    if corpus_snapshot_version is not None:
        predicates.append("corpus_snapshot_version = %s")
        params.append(corpus_snapshot_version)
    if ranking_version is not None:
        predicates.append("ranking_version = %s")
        params.append(ranking_version)
    row = conn.execute(
        f"""
        SELECT ranking_run_id
        FROM ranking_runs
        WHERE {' AND '.join(predicates)}
        ORDER BY finished_at DESC NULLS LAST, started_at DESC, ranking_run_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    return fetch_succeeded_materialized_run(
        conn,
        ranking_run_id=str(row["ranking_run_id"]),
    )


def _legacy_fallback_run(
    conn: psycopg.Connection,
) -> MaterializedRunContext | None:
    snapshot_row = conn.execute(
        """
        SELECT ssv.source_snapshot_version
        FROM source_snapshot_versions ssv
        WHERE EXISTS (
            SELECT 1
            FROM works w
            WHERE w.corpus_snapshot_version = ssv.source_snapshot_version
              AND w.inclusion_status = 'included'
        )
        ORDER BY ssv.created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if snapshot_row is None:
        return None
    return _latest_constrained_run(
        conn,
        corpus_snapshot_version=str(snapshot_row["source_snapshot_version"]),
        ranking_version=None,
    )


def resolve_serving_context(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str | None = None,
    corpus_snapshot_version: str | None = None,
    ranking_version: str | None = None,
) -> ServingContext:
    """Resolve selectors with exact-run precedence and a fail-closed public pointer."""
    run_id = _selector(ranking_run_id)
    snapshot = _selector(corpus_snapshot_version)
    version = _selector(ranking_version)

    if run_id is not None:
        run = fetch_succeeded_materialized_run(conn, ranking_run_id=run_id)
        if run is None:
            raise ServingContextNotFoundError(ranking_run_id=run_id)
        return ServingContext(run=run, source="explicit_run")

    if snapshot is not None or version is not None:
        run = _latest_constrained_run(
            conn,
            corpus_snapshot_version=snapshot,
            ranking_version=version,
        )
        if run is None:
            raise ServingContextNotFoundError(
                corpus_snapshot_version=snapshot,
                ranking_version=version,
            )
        return ServingContext(run=run, source="explicit_constraints")

    promotion = fetch_latest_public_release_promotion(conn)
    if promotion is not None:
        diagnostics = inspect_public_release_serveability(conn, promotion)
        if not diagnostics.serveable:
            raise ServingContextUnavailableError(
                promotion,
                failures=diagnostics.failures,
            )
        return ServingContext(
            run=promotion.run,
            source="active_promotion",
            promotion_id=promotion.promotion_id,
        )

    run = _legacy_fallback_run(conn)
    if run is None:
        raise ServingContextNotFoundError()
    return ServingContext(run=run, source="legacy_fallback")


def load_serving_context(
    *,
    ranking_run_id: str | None = None,
    corpus_snapshot_version: str | None = None,
    ranking_version: str | None = None,
) -> ServingContext:
    """Open the API database connection and resolve one serving context."""
    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        return resolve_serving_context(
            conn,
            ranking_run_id=ranking_run_id,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_version=ranking_version,
        )


__all__ = [
    "ServingContext",
    "ServingContextError",
    "ServingContextNotFoundError",
    "ServingContextSource",
    "ServingContextUnavailableError",
    "load_serving_context",
    "resolve_serving_context",
]
