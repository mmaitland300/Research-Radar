"""Validate and atomically promote one exact ranking run for public serving."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.clustering_persistence import (
    count_included_missing_cluster_assignment,
    require_successful_clustering_run,
)
from pipeline.embedding_persistence import count_missing_embedding_candidates
from pipeline.public_release_persistence import (
    PublicReleasePromotionRow,
    RankingRunForPromotion,
    ScoreCoverage,
    acquire_public_release_advisory_lock,
    append_public_release_promotion,
    count_cluster_assignments_outside_membership,
    fetch_active_public_release_promotion,
    fetch_ranking_run_for_promotion,
    fetch_score_coverage,
)
from pipeline.snapshot_membership import count_included_memberships

EXPECTED_FAMILIES: tuple[str, ...] = ("emerging", "bridge", "undercited")
PROMOTED_BY = "pipeline-cli"
NON_SERVING_EMBEDDING_VERSIONS = frozenset({"none", "none-v0"})


class PublicReleasePromotionError(RuntimeError):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class PublicReleasePromotionResult:
    status: str
    changed: bool
    dry_run: bool
    promotion_id: int | None
    promoted_at: datetime | None
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    membership_count: int
    rows_by_family: dict[str, int]
    cluster_version: str | None
    active_ranking_run_id_before: str | None


def _json_object(raw: Any, *, label: str) -> dict[str, Any]:
    parsed = raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PublicReleasePromotionError(f"{label} is not valid JSON") from exc
    if not isinstance(parsed, Mapping):
        raise PublicReleasePromotionError(f"{label} must be a JSON object")
    return dict(parsed)


def _required_nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PublicReleasePromotionError(f"{label} must be a non-negative integer")
    return value


def _validate_run_state(run: RankingRunForPromotion, *, requested_run_id: str) -> None:
    if run.ranking_run_id != requested_run_id:
        raise PublicReleasePromotionError("database returned a different ranking_run_id")
    if run.status != "succeeded":
        raise PublicReleasePromotionError(
            f"ranking run {requested_run_id!r} is not succeeded (status={run.status!r})"
        )
    if run.finished_at is None:
        raise PublicReleasePromotionError(
            f"ranking run {requested_run_id!r} has no finished_at timestamp"
        )
    if run.error_message is not None:
        raise PublicReleasePromotionError(
            f"ranking run {requested_run_id!r} has a recorded error"
        )
    if not run.ranking_version.strip():
        raise PublicReleasePromotionError("ranking run has a blank ranking_version")
    if not run.corpus_snapshot_version.strip():
        raise PublicReleasePromotionError("ranking run has a blank corpus_snapshot_version")
    embedding_version = run.embedding_version.strip()
    if not embedding_version or embedding_version.lower() in NON_SERVING_EMBEDDING_VERSIONS:
        raise PublicReleasePromotionError(
            "ranking run must reference a non-placeholder embedding_version"
        )


def _validate_config(
    raw_config: Any,
    *,
    corpus_snapshot_version: str,
) -> tuple[dict[str, Any], Mapping[str, Any] | None]:
    config = _json_object(raw_config, label="ranking run config_json")
    if not config:
        raise PublicReleasePromotionError("ranking run config_json must not be empty")

    raw_families = config.get("families_written")
    if not isinstance(raw_families, Sequence) or isinstance(raw_families, (str, bytes)):
        raise PublicReleasePromotionError("config_json.families_written must be an array")
    families = [str(family) for family in raw_families]
    if len(families) != len(EXPECTED_FAMILIES) or set(families) != set(EXPECTED_FAMILIES):
        raise PublicReleasePromotionError(
            "config_json.families_written must contain emerging, bridge, and undercited exactly once"
        )

    selection_scope = config.get("selection_scope")
    if not isinstance(selection_scope, Mapping):
        raise PublicReleasePromotionError("config_json.selection_scope must be an object")
    if selection_scope.get("type") != "included_works":
        raise PublicReleasePromotionError(
            "config_json.selection_scope.type must be 'included_works'"
        )
    if selection_scope.get("corpus_snapshot_version") != corpus_snapshot_version:
        raise PublicReleasePromotionError(
            "config_json.selection_scope corpus snapshot does not match the ranking run"
        )

    clustering_artifact = config.get("clustering_artifact")
    if clustering_artifact is not None and not isinstance(clustering_artifact, Mapping):
        raise PublicReleasePromotionError("config_json.clustering_artifact must be null or an object")
    return config, clustering_artifact


def _validate_score_coverage(coverage: ScoreCoverage, *, membership_count: int) -> None:
    if coverage.outside_membership_count:
        raise PublicReleasePromotionError(
            "ranking run has "
            f"{coverage.outside_membership_count} score rows outside snapshot membership"
        )
    if coverage.emerging_count != membership_count:
        raise PublicReleasePromotionError(
            "emerging score coverage is incomplete: "
            f"expected {membership_count}, found {coverage.emerging_count}"
        )
    if coverage.bridge_count != membership_count:
        raise PublicReleasePromotionError(
            "bridge score coverage is incomplete: "
            f"expected {membership_count}, found {coverage.bridge_count}"
        )
    if coverage.undercited_count <= 0:
        raise PublicReleasePromotionError("undercited score coverage must be a nonempty subset")
    if coverage.undercited_count > membership_count:
        raise PublicReleasePromotionError(
            "undercited score coverage exceeds snapshot membership"
        )
    expected_total = sum(coverage.rows_by_family.values())
    if coverage.total_count != expected_total:
        raise PublicReleasePromotionError("ranking run contains an unexpected recommendation family")


def _validate_counts(
    raw_counts: Any,
    *,
    membership_count: int,
    coverage: ScoreCoverage,
) -> None:
    counts = _json_object(raw_counts, label="ranking run counts_json")
    total_candidates = _required_nonnegative_int(
        counts.get("total_candidate_works"),
        label="counts_json.total_candidate_works",
    )
    if total_candidates != membership_count:
        raise PublicReleasePromotionError(
            "counts_json.total_candidate_works does not match snapshot membership"
        )

    total_rows = _required_nonnegative_int(
        counts.get("total_rows_written"),
        label="counts_json.total_rows_written",
    )
    if total_rows != coverage.total_count:
        raise PublicReleasePromotionError(
            "counts_json.total_rows_written does not match persisted ranking rows"
        )

    raw_rows_by_family = counts.get("rows_by_family")
    if not isinstance(raw_rows_by_family, Mapping):
        raise PublicReleasePromotionError("counts_json.rows_by_family must be an object")
    if set(raw_rows_by_family) != set(EXPECTED_FAMILIES):
        raise PublicReleasePromotionError(
            "counts_json.rows_by_family must contain exactly emerging, bridge, and undercited"
        )
    for family, actual in coverage.rows_by_family.items():
        expected = _required_nonnegative_int(
            raw_rows_by_family.get(family),
            label=f"counts_json.rows_by_family.{family}",
        )
        if expected != actual:
            raise PublicReleasePromotionError(
                f"counts_json.rows_by_family.{family} does not match persisted ranking rows"
            )

    for optional_name in ("rows_null_semantic", "rows_null_bridge"):
        if optional_name not in counts:
            continue
        value = _required_nonnegative_int(
            counts[optional_name], label=f"counts_json.{optional_name}"
        )
        if value > total_rows:
            raise PublicReleasePromotionError(
                f"counts_json.{optional_name} exceeds total_rows_written"
            )


def _validate_clustering(
    conn: psycopg.Connection,
    *,
    clustering_artifact: Mapping[str, Any] | None,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> str | None:
    if clustering_artifact is None:
        return None

    cluster_version = str(clustering_artifact.get("cluster_version") or "").strip()
    if not cluster_version:
        raise PublicReleasePromotionError("clustering_artifact.cluster_version is required")
    if clustering_artifact.get("corpus_snapshot_version") != corpus_snapshot_version:
        raise PublicReleasePromotionError(
            "clustering_artifact corpus snapshot does not match the ranking run"
        )
    if clustering_artifact.get("embedding_version") != embedding_version:
        raise PublicReleasePromotionError(
            "clustering_artifact embedding version does not match the ranking run"
        )
    try:
        require_successful_clustering_run(
            conn,
            cluster_version=cluster_version,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
        )
    except RuntimeError as exc:
        raise PublicReleasePromotionError(str(exc)) from exc

    missing = count_included_missing_cluster_assignment(
        conn,
        corpus_snapshot_version=corpus_snapshot_version,
        cluster_version=cluster_version,
    )
    if missing:
        raise PublicReleasePromotionError(
            f"clustering artifact is missing {missing} snapshot assignments"
        )
    outside = count_cluster_assignments_outside_membership(
        conn,
        corpus_snapshot_version=corpus_snapshot_version,
        cluster_version=cluster_version,
    )
    if outside:
        raise PublicReleasePromotionError(
            f"clustering artifact has {outside} assignments outside snapshot membership"
        )
    return cluster_version


def _result(
    *,
    status: str,
    changed: bool,
    dry_run: bool,
    promotion: PublicReleasePromotionRow | None,
    run: RankingRunForPromotion,
    membership_count: int,
    coverage: ScoreCoverage,
    cluster_version: str | None,
    active_before: PublicReleasePromotionRow | None,
) -> PublicReleasePromotionResult:
    return PublicReleasePromotionResult(
        status=status,
        changed=changed,
        dry_run=dry_run,
        promotion_id=promotion.promotion_id if promotion is not None else None,
        promoted_at=promotion.promoted_at if promotion is not None else None,
        ranking_run_id=run.ranking_run_id,
        ranking_version=run.ranking_version,
        corpus_snapshot_version=run.corpus_snapshot_version,
        embedding_version=run.embedding_version,
        membership_count=membership_count,
        rows_by_family=coverage.rows_by_family,
        cluster_version=cluster_version,
        active_ranking_run_id_before=(
            active_before.ranking_run_id if active_before is not None else None
        ),
    )


def promote_public_release(
    *,
    ranking_run_id: str,
    database_url: str | None = None,
    dry_run: bool = False,
) -> PublicReleasePromotionResult:
    """Revalidate one exact run and append it as the public release when requested."""
    requested_run_id = (ranking_run_id or "").strip()
    if not requested_run_id:
        raise PublicReleasePromotionError("--ranking-run-id is required and must not be blank")

    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, autocommit=False) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        acquire_public_release_advisory_lock(conn)
        run = fetch_ranking_run_for_promotion(conn, ranking_run_id=requested_run_id)
        if run is None:
            raise PublicReleasePromotionError(f"ranking run not found: {requested_run_id}")
        _validate_run_state(run, requested_run_id=requested_run_id)

        membership_count = count_included_memberships(
            conn, snapshot_version=run.corpus_snapshot_version
        )
        if membership_count <= 0:
            raise PublicReleasePromotionError("ranking run snapshot has no included memberships")

        missing_embeddings = count_missing_embedding_candidates(
            conn,
            corpus_snapshot_version=run.corpus_snapshot_version,
            embedding_version=run.embedding_version,
        )
        if missing_embeddings:
            raise PublicReleasePromotionError(
                f"ranking run snapshot is missing {missing_embeddings} embeddings"
            )

        _config, clustering_artifact = _validate_config(
            run.config_json,
            corpus_snapshot_version=run.corpus_snapshot_version,
        )
        coverage = fetch_score_coverage(
            conn,
            ranking_run_id=run.ranking_run_id,
            corpus_snapshot_version=run.corpus_snapshot_version,
        )
        _validate_score_coverage(coverage, membership_count=membership_count)
        _validate_counts(
            run.counts_json,
            membership_count=membership_count,
            coverage=coverage,
        )
        cluster_version = _validate_clustering(
            conn,
            clustering_artifact=clustering_artifact,
            corpus_snapshot_version=run.corpus_snapshot_version,
            embedding_version=run.embedding_version,
        )

        active_before = fetch_active_public_release_promotion(conn)
        if active_before is not None and active_before.ranking_run_id == run.ranking_run_id:
            return _result(
                status="already-active",
                changed=False,
                dry_run=dry_run,
                promotion=active_before,
                run=run,
                membership_count=membership_count,
                coverage=coverage,
                cluster_version=cluster_version,
                active_before=active_before,
            )
        if dry_run:
            return _result(
                status="validated",
                changed=False,
                dry_run=True,
                promotion=None,
                run=run,
                membership_count=membership_count,
                coverage=coverage,
                cluster_version=cluster_version,
                active_before=active_before,
            )

        promotion = append_public_release_promotion(
            conn,
            ranking_run_id=run.ranking_run_id,
            promoted_by=PROMOTED_BY,
            note=None,
        )
        return _result(
            status="promoted",
            changed=True,
            dry_run=False,
            promotion=promotion,
            run=run,
            membership_count=membership_count,
            coverage=coverage,
            cluster_version=cluster_version,
            active_before=active_before,
        )


__all__ = [
    "EXPECTED_FAMILIES",
    "PROMOTED_BY",
    "PublicReleasePromotionError",
    "PublicReleasePromotionResult",
    "promote_public_release",
]
