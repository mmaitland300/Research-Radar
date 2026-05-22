"""Run the second shadow-generalization product-candidate ranking.

This command validates the second shadow-generalization snapshot embedding
chain, delegates eval-only ranking materialization to existing ranking-run
machinery, and emits an audit artifact. It writes only ranking_runs and
paper_scores through that machinery. It does not hydrate, embed, generate
learned probabilities, execute scorers, import labels, or authorize
shadow/production.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import psycopg

from pipeline.config import RankingRun
from pipeline.embedding_persistence import count_included_works_for_snapshot, count_missing_embedding_candidates
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_second_candidate_plan_ingest import (
    EXPECTED_SELECTED_TOTAL,
    MLShadowScorerSecondCandidatePlanIngestError,
    _database_url_from_env,
    assert_local_database_url,
)
from pipeline.ranking_run import execute_ranking_run
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_product_candidate_ranking"
ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-product-candidate-ranking-v1"
DEFAULT_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
DEFAULT_EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
DEFAULT_RANKING_VERSION = "shadow-generalization-product-candidate-ranking-v1"
DEFAULT_FAMILY = "emerging"
DISALLOWED_RANKING_RUN_ID = "rank-9f4b2a2084"

EMBEDDINGS_ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
HYDRATION_ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_hydration"
HYDRATION_VERSION = "ml-shadow-scorer-v1-second-snapshot-hydration-v1"
INGEST_ARTIFACT_TYPE = "ml_shadow_scorer_second_candidate_plan_ingest"
INGEST_VERSION = "ml-shadow-scorer-v1-second-candidate-plan-ingest-v1"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"

ALLOWED_WRITE_TABLES = ("ranking_runs", "paper_scores")
BRIDGE_WEIGHT_FOR_FAMILY_BRIDGE = 0.0

CAVEATS = (
    "Eval-only product-candidate ranking materialization for the second shadow-generalization source.",
    "Ranking materializes heuristic final_score paths only; no learned audit_embedding_probability_work is generated.",
    "paper_scores are discovery inputs, not confirmatory validation or shadow execution.",
    "The new ranking_run_id must remain distinct from rank-9f4b2a2084.",
    "No hydration, snapshot/work/raw writes, embeddings, scorer execution, label ingest, online shadow, API/web, or production/default change.",
    "No shadow or production authorization.",
)


class MLShadowScorerSecondProductCandidateRankingError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondProductCandidateRankingError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondProductCandidateRankingError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondProductCandidateRankingError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerSecondProductCandidateRankingError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return dict(assert_local_database_url(database_url))
    except MLShadowScorerSecondCandidatePlanIngestError as exc:
        raise MLShadowScorerSecondProductCandidateRankingError(str(exc), code=exc.code) from exc


def _validate_embeddings(
    payload: Mapping[str, Any],
    *,
    snapshot_version: str | None,
    embedding_version: str,
) -> str:
    metadata = _metadata(payload, name="second-snapshot-embeddings")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLShadowScorerSecondProductCandidateRankingError("embeddings metadata.artifact_type mismatch")
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLShadowScorerSecondProductCandidateRankingError(
            "embeddings artifact_version must be ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
        )
    if _get(payload, "embedding_result.status") != "succeeded":
        raise MLShadowScorerSecondProductCandidateRankingError("embedding_result.status must be succeeded")
    if _get(payload, "embedding_result.full_snapshot_embedding_coverage") is not True:
        raise MLShadowScorerSecondProductCandidateRankingError("full_snapshot_embedding_coverage must be true")
    if _get(payload, "embedding_result.cluster_required_before_ranking") is not False:
        raise MLShadowScorerSecondProductCandidateRankingError("cluster_required_before_ranking must be false")
    if _get(payload, "embedding_result.recommended_next_stage") != "run_second_shadow_generalization_product_candidate_ranking_v1":
        raise MLShadowScorerSecondProductCandidateRankingError(
            "embedding_result.recommended_next_stage must be run_second_shadow_generalization_product_candidate_ranking_v1"
        )
    top_next = payload.get("recommended_next_stage")
    if top_next is not None and top_next != "run_second_shadow_generalization_product_candidate_ranking_v1":
        raise MLShadowScorerSecondProductCandidateRankingError(
            "top-level recommended_next_stage must be run_second_shadow_generalization_product_candidate_ranking_v1"
        )
    observed_snapshot = metadata.get("snapshot_version")
    if not isinstance(observed_snapshot, str) or not observed_snapshot.strip():
        raise MLShadowScorerSecondProductCandidateRankingError("embeddings metadata.snapshot_version is missing")
    if snapshot_version and snapshot_version != observed_snapshot:
        raise MLShadowScorerSecondProductCandidateRankingError(
            f"snapshot-version {snapshot_version!r} does not match embeddings artifact {observed_snapshot!r}"
        )
    observed_embedding = metadata.get("embedding_version")
    if observed_embedding != embedding_version:
        raise MLShadowScorerSecondProductCandidateRankingError(
            f"embedding-version {embedding_version!r} does not match embeddings artifact {observed_embedding!r}"
        )
    if _get(payload, "coverage.snapshot_work_count") != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondProductCandidateRankingError("coverage.snapshot_work_count must be 528")
    if _get(payload, "coverage.embedded_work_count") != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondProductCandidateRankingError("coverage.embedded_work_count must be 528")
    if _get(payload, "coverage.missing_embedding_count") != 0:
        raise MLShadowScorerSecondProductCandidateRankingError("coverage.missing_embedding_count must be 0")
    if _get(payload, "sql_write_report.ranking_runs_written") is not False:
        raise MLShadowScorerSecondProductCandidateRankingError("embeddings artifact must not have written ranking_runs")
    if _get(payload, "sql_write_report.paper_scores_written") is not False:
        raise MLShadowScorerSecondProductCandidateRankingError("embeddings artifact must not have written paper_scores")
    return observed_snapshot


def _validate_hydration(payload: Mapping[str, Any], *, snapshot_version: str) -> None:
    metadata = _metadata(payload, name="second-snapshot-hydration")
    if metadata.get("artifact_type") != HYDRATION_ARTIFACT_TYPE:
        raise MLShadowScorerSecondProductCandidateRankingError("hydration metadata.artifact_type mismatch")
    if metadata.get("hydration_version") != HYDRATION_VERSION:
        raise MLShadowScorerSecondProductCandidateRankingError(
            "hydration_version must be ml-shadow-scorer-v1-second-snapshot-hydration-v1"
        )
    if _get(payload, "hydration_result.status") != "succeeded":
        raise MLShadowScorerSecondProductCandidateRankingError("hydration_result.status must be succeeded")
    if _get(payload, "hydration_result.snapshot_embedding_ready") is not True:
        raise MLShadowScorerSecondProductCandidateRankingError("hydration snapshot_embedding_ready must be true")
    if metadata.get("snapshot_version") != snapshot_version:
        raise MLShadowScorerSecondProductCandidateRankingError("hydration metadata.snapshot_version does not match snapshot")


def _validate_ingest(payload: Mapping[str, Any], *, snapshot_version: str) -> int:
    metadata = _metadata(payload, name="second-candidate-plan-ingest")
    if metadata.get("artifact_type") != INGEST_ARTIFACT_TYPE:
        raise MLShadowScorerSecondProductCandidateRankingError("ingest metadata.artifact_type mismatch")
    if metadata.get("ingest_version") != INGEST_VERSION:
        raise MLShadowScorerSecondProductCandidateRankingError(
            "ingest_version must be ml-shadow-scorer-v1-second-candidate-plan-ingest-v1"
        )
    if _get(payload, "ingest_result.status") != "succeeded":
        raise MLShadowScorerSecondProductCandidateRankingError("ingest_result.status must be succeeded")
    if _get(payload, "snapshot.source_snapshot_version") != snapshot_version:
        raise MLShadowScorerSecondProductCandidateRankingError("ingest snapshot.source_snapshot_version does not match snapshot")
    if _get(payload, "snapshot.shadow_generalization_candidate_source") is not True:
        raise MLShadowScorerSecondProductCandidateRankingError("ingest snapshot must be shadow_generalization_candidate_source")
    count = _get(payload, "ingest_result.snapshot_work_count")
    if count != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondProductCandidateRankingError("ingest_result.snapshot_work_count must be 528")
    return int(count)


def _validate_generalization_plan(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="generalization-audit-plan")
    if metadata.get("artifact_type") != GENERALIZATION_PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondProductCandidateRankingError("generalization audit plan artifact_type mismatch")
    if metadata.get("plan_version") != GENERALIZATION_PLAN_VERSION:
        raise MLShadowScorerSecondProductCandidateRankingError(
            "generalization audit plan_version must be ml-shadow-scorer-v1-generalization-audit-v1"
        )
    if payload.get("generalization_audit_plan_defined") is not True:
        raise MLShadowScorerSecondProductCandidateRankingError("generalization audit plan must be defined")
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondProductCandidateRankingError("runtime implementation must remain unauthorized")


def _validate_policy(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLShadowScorerSecondProductCandidateRankingError("fresh surface policy artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLShadowScorerSecondProductCandidateRankingError(
            "fresh surface policy_version must be ml-fresh-eval-surface-policy-hybrid-v1"
        )


def _validate_snapshot_and_embeddings(
    conn: Any,
    *,
    snapshot_version: str,
    embedding_version: str,
    expected_work_count: int,
) -> dict[str, int]:
    exists = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    if exists is None:
        raise MLShadowScorerSecondProductCandidateRankingError(
            f"snapshot_version not found in local Postgres: {snapshot_version}"
        )
    snapshot_count = count_included_works_for_snapshot(conn, snapshot_version)
    if snapshot_count != expected_work_count:
        raise MLShadowScorerSecondProductCandidateRankingError(
            f"snapshot work count mismatch for {snapshot_version}: expected {expected_work_count}, found {snapshot_count}"
        )
    missing_embeddings = count_missing_embedding_candidates(
        conn,
        corpus_snapshot_version=snapshot_version,
        embedding_version=embedding_version,
    )
    if missing_embeddings != 0:
        raise MLShadowScorerSecondProductCandidateRankingError(
            f"missing {missing_embeddings} embedding rows for snapshot {snapshot_version} and version {embedding_version}"
        )
    return {
        "snapshot_work_count": snapshot_count,
        "missing_embedding_count": missing_embeddings,
    }


def _paper_scores_by_family(conn: Any, *, ranking_run_id: str) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT recommendation_family, COUNT(*)
        FROM paper_scores
        WHERE ranking_run_id = %s
        GROUP BY recommendation_family
        ORDER BY recommendation_family
        """,
        (ranking_run_id,),
    ).fetchall()
    return {str(row[0]): int(row[1] or 0) for row in rows}


def _run_ranking(
    *,
    database_url: str,
    ranking_version: str,
    snapshot_version: str,
    embedding_version: str,
) -> RankingRun:
    return execute_ranking_run(
        database_url=database_url,
        ranking_version=ranking_version,
        corpus_snapshot_version=snapshot_version,
        embedding_version=embedding_version,
        cluster_version=None,
        bridge_weight_for_bridge_family=BRIDGE_WEIGHT_FOR_FAMILY_BRIDGE,
        note=(
            "Eval-only shadow generalization product-candidate ranking; "
            "no production/default pin."
        ),
    )


def _handoff_commands(*, ranking_run_id: str | None, family: str) -> dict[str, str | None]:
    if not ranking_run_id:
        return {
            "generalization_second_surface": None,
            "note": "Dry run only: run the live ranking first, then rerun second-surface discovery.",
        }
    return {
        "generalization_second_surface": (
            "py -m pipeline.cli ml-shadow-scorer-generalization-second-surface "
            "--generalization-audit-plan ../../docs/audit/ml-shadow-scorer-v1-generalization-audit-v1.json "
            "--online-shadow-policy ../../docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json "
            "--fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json "
            "--label-dataset ../../docs/audit/ml-label-dataset-v10.json "
            "--conflict-policy ../../docs/audit/ml-label-conflict-policy.md "
            "--offline-production-candidate-scoring-v3 ../../docs/audit/ml-offline-production-candidate-scoring-v3.json "
            "--first-validated-surface ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.json "
            f"--family {family} "
            "--output ../../docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.json "
            "--markdown-output ../../docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.md"
        ),
        "note": (
            "Discovery has no --ranking-run-id; identify this source by corpus_snapshot_version, "
            "ranking_version, and candidate_pool_work_set_sha256 distinct from the first validated surface."
        ),
    }


def _artifact_from_run(
    *,
    artifact_version: str,
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    snapshot_version: str,
    embedding_version: str,
    ranking_version: str,
    family: str,
    dry_run: bool,
    validation_counts: Mapping[str, int],
    run: RankingRun | None,
    paper_scores_by_family: Mapping[str, int],
) -> dict[str, Any]:
    ranking_run_id = run.ranking_run_id if run is not None else None
    if ranking_run_id == DISALLOWED_RANKING_RUN_ID:
        raise MLShadowScorerSecondProductCandidateRankingError(
            f"ranking_run_id must differ from {DISALLOWED_RANKING_RUN_ID}"
        )
    run_counts = run.counts if run is not None else None
    paper_scores_written = int(sum(paper_scores_by_family.values()))
    total_candidate_works = int(
        run_counts.total_candidate_works if run_counts is not None else validation_counts.get("snapshot_work_count", 0)
    )
    status = "dry_run_validated" if dry_run else (run.status if run is not None else "failed")
    next_stage = (
        "rerun_second_shadow_generalization_surface_discovery_v1"
        if status == "succeeded" and paper_scores_written > 0
        else "run_live_second_shadow_generalization_product_candidate_ranking_v1"
    )
    affected = (
        {
            "ranking_runs": 2,
            "paper_scores": paper_scores_written,
        }
        if not dry_run
        else {}
    )
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "artifact_version": artifact_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "snapshot_version": snapshot_version,
            "embedding_version": embedding_version,
            "ranking_version": ranking_version,
            "family": family,
            "eval_only": True,
            "shadow_generalization_eval_only": True,
            "dry_run": dry_run,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "caveats": list(CAVEATS),
        },
        "ranking_result": {
            "status": status,
            "ranking_run_id": ranking_run_id,
            "ranking_version": ranking_version,
            "corpus_snapshot_version": snapshot_version,
            "embedding_version": embedding_version,
            "cluster_version": None,
            "total_candidate_works": total_candidate_works,
            "paper_scores_written_count": paper_scores_written,
            "paper_scores_by_family": dict(paper_scores_by_family),
            "emerging_family_work_count": int(paper_scores_by_family.get("emerging", 0)),
            "bridge_weight_for_family_bridge": BRIDGE_WEIGHT_FOR_FAMILY_BRIDGE,
            "recommended_next_stage": next_stage,
        },
        "sql_write_report": {
            "writes_enabled": not dry_run,
            "allowed_tables": list(ALLOWED_WRITE_TABLES) if not dry_run else [],
            "affected_row_counts": affected,
            "ranking_runs_written": bool((not dry_run) and status == "succeeded"),
            "paper_scores_written": bool((not dry_run) and status == "succeeded" and paper_scores_written > 0),
            "source_snapshot_versions_written": False,
            "ingest_runs_written": False,
            "raw_openalex_works_written": False,
            "works_modified": False,
            "embeddings_modified": False,
            "production_tables_modified": False,
        },
        "candidate_source_handoff": {
            "ranking_run_id": ranking_run_id,
            "family": family,
            "snapshot_version": snapshot_version,
            "ranking_version": ranking_version,
            "embedding_version": embedding_version,
            "commands": _handoff_commands(ranking_run_id=ranking_run_id, family=family),
            "note": (
                "Rerun second-surface discovery and confirm this source has candidate_pool_work_set_sha256 "
                f"distinct from the first validated surface and ranking_run_id != {DISALLOWED_RANKING_RUN_ID}."
            ),
        },
        "blocked_actions": [
            "hydration",
            "snapshot_or_work_row_write",
            "embedding_generation",
            "learned_probability_generation",
            "scorer_execution",
            "label_ingest",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "missing_second_fresh_candidate_source": False,
            "missing_second_surface_embedding_coverage": False,
            "missing_second_surface_ranking_run": not (status == "succeeded" and ranking_run_id is not None),
            "missing_second_surface_learned_probability_coverage": True,
            "missing_generalization_audit_on_second_surface": True,
            "missing_generalization_audit_gates": True,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "recommended_next_stage": next_stage,
        "caveats": list(CAVEATS),
    }


def build_ml_shadow_scorer_second_product_candidate_ranking_payload(
    *,
    second_snapshot_embeddings_path: Path,
    second_snapshot_hydration_path: Path,
    second_candidate_plan_ingest_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    snapshot_version: str | None = DEFAULT_SNAPSHOT_VERSION,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    ranking_version: str = DEFAULT_RANKING_VERSION,
    family: str = DEFAULT_FAMILY,
    database_url: str | None = None,
    dry_run: bool = False,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    embeddings_path = Path(second_snapshot_embeddings_path).resolve()
    hydration_path = Path(second_snapshot_hydration_path).resolve()
    ingest_path = Path(second_candidate_plan_ingest_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    embeddings_payload = _load_json_object(embeddings_path)
    hydration_payload = _load_json_object(hydration_path)
    ingest_payload = _load_json_object(ingest_path)
    audit_plan_payload = _load_json_object(audit_plan_path)
    policy_payload = _load_json_object(policy_path)

    embedding_label = (embedding_version or "").strip() or DEFAULT_EMBEDDING_VERSION
    observed_snapshot = _validate_embeddings(
        embeddings_payload,
        snapshot_version=snapshot_version,
        embedding_version=embedding_label,
    )
    resolved_snapshot = (snapshot_version or observed_snapshot).strip()
    _validate_hydration(hydration_payload, snapshot_version=resolved_snapshot)
    expected_work_count = _validate_ingest(ingest_payload, snapshot_version=resolved_snapshot)
    _validate_generalization_plan(audit_plan_payload)
    _validate_policy(policy_payload)

    inputs = [
        _input_record("second_snapshot_embeddings", embeddings_path, repo_root=root),
        _input_record("second_snapshot_hydration", hydration_path, repo_root=root),
        _input_record("second_candidate_plan_ingest", ingest_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
    ]
    dsn = database_url or _database_url_from_env()
    database_summary = _assert_local_database_url(dsn)
    with psycopg.connect(dsn, autocommit=False) as conn:
        validation_counts = _validate_snapshot_and_embeddings(
            conn,
            snapshot_version=resolved_snapshot,
            embedding_version=embedding_label,
            expected_work_count=expected_work_count,
        )

    resolved_ranking = (ranking_version or DEFAULT_RANKING_VERSION).strip()
    resolved_family = (family or DEFAULT_FAMILY).strip()
    run: RankingRun | None = None
    paper_scores: dict[str, int] = {}
    if not dry_run:
        try:
            run = _run_ranking(
                database_url=dsn,
                ranking_version=resolved_ranking,
                snapshot_version=resolved_snapshot,
                embedding_version=embedding_label,
            )
        except Exception as exc:
            raise MLShadowScorerSecondProductCandidateRankingError(
                f"second shadow-generalization product-candidate ranking failed: {exc}",
                code=1,
            ) from exc
        with psycopg.connect(dsn, autocommit=False) as conn:
            paper_scores = _paper_scores_by_family(conn, ranking_run_id=run.ranking_run_id)
            if not paper_scores:
                raise MLShadowScorerSecondProductCandidateRankingError(
                    f"ranking_run_id {run.ranking_run_id!r} wrote no paper_scores rows"
                )
    return _artifact_from_run(
        artifact_version=artifact_version,
        inputs=inputs,
        database_summary=database_summary,
        snapshot_version=resolved_snapshot,
        embedding_version=embedding_label,
        ranking_version=resolved_ranking,
        family=resolved_family,
        dry_run=dry_run,
        validation_counts=validation_counts,
        run=run,
        paper_scores_by_family=paper_scores,
    )


def markdown_from_ml_shadow_scorer_second_product_candidate_ranking(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    result = payload["ranking_result"]
    write_report = payload["sql_write_report"]
    handoff = payload["candidate_source_handoff"]
    commands = handoff.get("commands") if isinstance(handoff, Mapping) else {}
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# Second Shadow-Generalization Product-Candidate Ranking ({metadata['artifact_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records an eval-only product-candidate ranking run for the second shadow-generalization source. It materializes ranking_runs and paper_scores only; it does not generate learned probabilities, execute the shadow scorer, or authorize shadow/production.",
        "",
        f"- **Status:** `{result['status']}`",
        f"- **Ranking run ID:** `{result['ranking_run_id']}`",
        f"- **Snapshot version:** `{result['corpus_snapshot_version']}`",
        f"- **Embedding version:** `{result['embedding_version']}`",
        f"- **Ranking version:** `{result['ranking_version']}`",
        f"- **Cluster version:** `{result['cluster_version']}`",
        f"- **Total candidate works:** {result['total_candidate_works']}",
        f"- **Emerging family work count:** {result['emerging_family_work_count']}",
        f"- **paper_scores written:** {result['paper_scores_written_count']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        "",
        "## Paper Scores By Family",
        "",
    ]
    by_family = result.get("paper_scores_by_family") or {}
    if by_family:
        lines.extend(f"- {family}: {count}" for family, count in sorted(by_family.items()))
    else:
        lines.append("- None written in dry run.")
    lines.extend(
        [
            "",
            "## DB Write Scope",
            "",
            f"- Writes enabled: {write_report['writes_enabled']}",
            f"- Allowed tables: {', '.join(write_report['allowed_tables']) if write_report['allowed_tables'] else 'none'}",
            f"- ranking_runs written: {write_report['ranking_runs_written']}",
            f"- paper_scores written: {write_report['paper_scores_written']}",
            f"- source_snapshot_versions written: {write_report['source_snapshot_versions_written']}",
            f"- ingest_runs written: {write_report['ingest_runs_written']}",
            f"- raw_openalex_works written: {write_report['raw_openalex_works_written']}",
            f"- works modified: {write_report['works_modified']}",
            f"- embeddings modified: {write_report['embeddings_modified']}",
            f"- production tables modified: {write_report['production_tables_modified']}",
            "",
            "## Second-Surface Discovery Handoff",
            "",
        ]
    )
    if isinstance(commands, Mapping) and commands.get("generalization_second_surface"):
        lines.extend(
            [
                "Rerun second-surface discovery:",
                "",
                f"```powershell\n{commands['generalization_second_surface']}\n```",
            ]
        )
    else:
        lines.append(str(commands.get("note") if isinstance(commands, Mapping) else "Dry run only."))
    lines.extend(
        [
            "",
            "## Remaining Blockers",
            "",
        ]
    )
    for key, value in blockers.items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Not Learned Probability / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_shadow_scorer_second_product_candidate_ranking(
    *,
    second_snapshot_embeddings_path: Path,
    second_snapshot_hydration_path: Path,
    second_candidate_plan_ingest_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    snapshot_version: str | None = DEFAULT_SNAPSHOT_VERSION,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    ranking_version: str = DEFAULT_RANKING_VERSION,
    family: str = DEFAULT_FAMILY,
    database_url: str | None = None,
    dry_run: bool = False,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_second_product_candidate_ranking_payload(
        second_snapshot_embeddings_path=second_snapshot_embeddings_path,
        second_snapshot_hydration_path=second_snapshot_hydration_path,
        second_candidate_plan_ingest_path=second_candidate_plan_ingest_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        snapshot_version=snapshot_version,
        embedding_version=embedding_version,
        ranking_version=ranking_version,
        family=family,
        database_url=database_url,
        dry_run=dry_run,
        artifact_version=artifact_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_product_candidate_ranking(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "ARTIFACT_VERSION",
    "DEFAULT_EMBEDDING_VERSION",
    "DEFAULT_RANKING_VERSION",
    "DEFAULT_SNAPSHOT_VERSION",
    "MLShadowScorerSecondProductCandidateRankingError",
    "build_ml_shadow_scorer_second_product_candidate_ranking_payload",
    "markdown_from_ml_shadow_scorer_second_product_candidate_ranking",
    "write_ml_shadow_scorer_second_product_candidate_ranking",
]
