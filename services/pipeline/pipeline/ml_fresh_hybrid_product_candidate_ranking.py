"""Fresh hybrid product-candidate ranking audit wrapper.

This command validates the fresh-hybrid snapshot embedding chain, delegates
ranking materialization to the existing ranking-run machinery, and emits an
eval-only audit artifact. It writes only ranking_runs and paper_scores through
that machinery. It does not hydrate, embed, cluster, score hybrids, import
labels, or authorize shadow/prod.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import psycopg

from pipeline.config import RankingRun
from pipeline.embedding_persistence import count_included_works_for_snapshot, count_missing_embedding_candidates
from pipeline.error_reporting import safe_exception_summary
from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
    MLFreshHybridCandidatePlanIngestError,
    _database_url_from_env,
    assert_local_database_url,
)
from pipeline.ml_label_dataset import sha256_file
from pipeline.ranking_run import execute_ranking_run
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_hybrid_product_candidate_ranking"
ARTIFACT_VERSION = "ml-fresh-hybrid-product-candidate-ranking-v1"
DEFAULT_SNAPSHOT_VERSION = "source-snapshot-fresh-hybrid-v1-20260518"
DEFAULT_EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
DEFAULT_RANKING_VERSION = "fresh-hybrid-product-candidate-ranking-v1"
DEFAULT_FAMILY = "emerging"

EMBEDDINGS_ARTIFACT_TYPE = "ml_fresh_hybrid_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-fresh-hybrid-snapshot-embeddings-v1"
HYDRATION_ARTIFACT_TYPE = "ml_fresh_hybrid_snapshot_hydration"
HYDRATION_VERSION = "ml-fresh-hybrid-snapshot-hydration-v1"
INGEST_ARTIFACT_TYPE = "ml_fresh_hybrid_candidate_plan_ingest"
INGEST_VERSION = "ml-fresh-hybrid-candidate-plan-ingest-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"

ALLOWED_WRITE_TABLES = ("ranking_runs", "paper_scores")
BRIDGE_WEIGHT_FOR_FAMILY_BRIDGE = 0.0

CAVEATS = (
    "Eval-only product-candidate ranking materialization for the fresh hybrid confirmation path.",
    "Ranking is delegated to existing execute_ranking_run machinery; no production/default ranking pin is changed.",
    "No new embeddings, hydration, clustering, hybrid validation, label import, API/web change, shadow, or production change.",
    "cluster_version is omitted because the upstream embedding artifact recorded cluster_required_before_ranking=false.",
    "paper_scores are source-discovery material for later fresh-surface materialization, not confirmatory validation.",
    "No shadow or production authorization.",
)


class MLFreshHybridProductCandidateRankingError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshHybridProductCandidateRankingError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshHybridProductCandidateRankingError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshHybridProductCandidateRankingError(f"{name} JSON missing metadata object")
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
        raise MLFreshHybridProductCandidateRankingError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return assert_local_database_url(database_url)
    except MLFreshHybridCandidatePlanIngestError as exc:
        raise MLFreshHybridProductCandidateRankingError(str(exc), code=exc.code) from exc


def _validate_embeddings(
    payload: Mapping[str, Any],
    *,
    snapshot_version: str | None,
    embedding_version: str,
) -> str:
    metadata = _metadata(payload, name="fresh-hybrid-snapshot-embeddings")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLFreshHybridProductCandidateRankingError("embeddings metadata.artifact_type mismatch")
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLFreshHybridProductCandidateRankingError(
            "embeddings artifact_version must be ml-fresh-hybrid-snapshot-embeddings-v1"
        )
    if _get(payload, "embedding_result.status") != "succeeded":
        raise MLFreshHybridProductCandidateRankingError("embedding_result.status must be succeeded")
    if _get(payload, "embedding_result.full_snapshot_embedding_coverage") is not True:
        raise MLFreshHybridProductCandidateRankingError("full_snapshot_embedding_coverage must be true")
    if _get(payload, "embedding_result.cluster_required_before_ranking") is not False:
        raise MLFreshHybridProductCandidateRankingError("cluster_required_before_ranking must be false")
    if _get(payload, "embedding_result.recommended_next_stage") != "run_fresh_hybrid_product_candidate_ranking_v1":
        raise MLFreshHybridProductCandidateRankingError(
            "embedding_result.recommended_next_stage must be run_fresh_hybrid_product_candidate_ranking_v1"
        )
    if _get(payload, "sql_write_report.ranking_runs_written") is not False:
        raise MLFreshHybridProductCandidateRankingError("embeddings artifact must not have written ranking_runs")
    if _get(payload, "sql_write_report.paper_scores_written") is not False:
        raise MLFreshHybridProductCandidateRankingError("embeddings artifact must not have written paper_scores")
    observed_snapshot = metadata.get("snapshot_version")
    if not isinstance(observed_snapshot, str) or not observed_snapshot.strip():
        raise MLFreshHybridProductCandidateRankingError("embeddings metadata.snapshot_version is missing")
    if snapshot_version and snapshot_version != observed_snapshot:
        raise MLFreshHybridProductCandidateRankingError(
            f"snapshot-version {snapshot_version!r} does not match embeddings artifact {observed_snapshot!r}"
        )
    observed_embedding = metadata.get("embedding_version")
    if observed_embedding != embedding_version:
        raise MLFreshHybridProductCandidateRankingError(
            f"embedding-version {embedding_version!r} does not match embeddings artifact {observed_embedding!r}"
        )
    return observed_snapshot


def _validate_hydration(payload: Mapping[str, Any], *, snapshot_version: str) -> None:
    metadata = _metadata(payload, name="fresh-hybrid-snapshot-hydration")
    if metadata.get("artifact_type") != HYDRATION_ARTIFACT_TYPE:
        raise MLFreshHybridProductCandidateRankingError("hydration metadata.artifact_type mismatch")
    if metadata.get("hydration_version") != HYDRATION_VERSION:
        raise MLFreshHybridProductCandidateRankingError("hydration_version must be ml-fresh-hybrid-snapshot-hydration-v1")
    if _get(payload, "hydration_result.status") != "succeeded":
        raise MLFreshHybridProductCandidateRankingError("hydration_result.status must be succeeded")
    if metadata.get("snapshot_version") != snapshot_version:
        raise MLFreshHybridProductCandidateRankingError("hydration metadata.snapshot_version does not match snapshot")


def _validate_ingest(payload: Mapping[str, Any], *, snapshot_version: str) -> int:
    metadata = _metadata(payload, name="fresh-hybrid-candidate-plan-ingest")
    if metadata.get("artifact_type") != INGEST_ARTIFACT_TYPE:
        raise MLFreshHybridProductCandidateRankingError("ingest metadata.artifact_type mismatch")
    if metadata.get("ingest_version") != INGEST_VERSION:
        raise MLFreshHybridProductCandidateRankingError("ingest_version must be ml-fresh-hybrid-candidate-plan-ingest-v1")
    if _get(payload, "ingest_result.status") != "succeeded":
        raise MLFreshHybridProductCandidateRankingError("ingest_result.status must be succeeded")
    if _get(payload, "snapshot.source_snapshot_version") != snapshot_version:
        raise MLFreshHybridProductCandidateRankingError("ingest snapshot.source_snapshot_version does not match snapshot")
    count = _get(payload, "ingest_result.snapshot_work_count")
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        raise MLFreshHybridProductCandidateRankingError("ingest_result.snapshot_work_count must be > 0")
    return count


def _validate_policy(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshHybridProductCandidateRankingError("fresh surface policy artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshHybridProductCandidateRankingError(
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
        raise MLFreshHybridProductCandidateRankingError(f"snapshot_version not found in local Postgres: {snapshot_version}")
    snapshot_count = count_included_works_for_snapshot(conn, snapshot_version)
    if snapshot_count != expected_work_count:
        raise MLFreshHybridProductCandidateRankingError(
            f"snapshot work count mismatch for {snapshot_version}: expected {expected_work_count}, found {snapshot_count}"
        )
    missing_embeddings = count_missing_embedding_candidates(
        conn,
        corpus_snapshot_version=snapshot_version,
        embedding_version=embedding_version,
    )
    if missing_embeddings != 0:
        raise MLFreshHybridProductCandidateRankingError(
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
            "Eval-only fresh hybrid confirmatory-path product-candidate ranking; "
            "no production/default pin, no API/web change, no shadow/prod authorization."
        ),
    )


def _handoff_commands(*, ranking_run_id: str | None, family: str) -> dict[str, str | None]:
    if not ranking_run_id:
        return {
            "fresh_product_candidate_ranking_source": None,
            "fresh_eval_surface_hybrid_materialize": None,
            "note": "Dry run only: run the ranking command before source discovery/materialization.",
        }
    return {
        "fresh_product_candidate_ranking_source": (
            "py -m pipeline.cli ml-fresh-product-candidate-ranking-source "
            "--fresh-eval-labeling-plan ../../docs/audit/ml-fresh-eval-labeling-plan-hybrid-v1.json "
            "--fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json "
            "--label-dataset ../../docs/audit/ml-label-dataset-v8.json "
            "--conflict-policy ../../docs/audit/ml-label-conflict-policy.md "
            f"--ranking-run-id {ranking_run_id} --family {family} "
            "--output ../../docs/audit/ml-fresh-product-candidate-ranking-source-v1.json "
            "--markdown-output ../../docs/audit/ml-fresh-product-candidate-ranking-source-v1.md"
        ),
        "fresh_eval_surface_hybrid_materialize": (
            "py -m pipeline.cli ml-fresh-eval-surface-hybrid-materialize "
            "--fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json "
            "--label-dataset ../../docs/audit/ml-label-dataset-v8.json "
            "--conflict-policy ../../docs/audit/ml-label-conflict-policy.md "
            f"--ranking-run-id {ranking_run_id} --family {family} "
            "--output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.json "
            "--markdown-output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.md"
        ),
        "note": "Use the pinned ranking_run_id so later discovery/materialization stays on this eval-only source.",
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
    run_counts = run.counts if run is not None else None
    paper_scores_written = int(sum(paper_scores_by_family.values()))
    total_candidate_works = int(
        run_counts.total_candidate_works if run_counts is not None else validation_counts.get("snapshot_work_count", 0)
    )
    status = "dry_run_validated" if dry_run else (run.status if run is not None else "failed")
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
            "dry_run": dry_run,
            "caveats": list(CAVEATS),
        },
        "ranking_result": {
            "status": status,
            "ranking_run_id": ranking_run_id,
            "ranking_version": ranking_version,
            "corpus_snapshot_version": snapshot_version,
            "embedding_version": embedding_version,
            "cluster_version": None,
            "bridge_weight_for_family_bridge": BRIDGE_WEIGHT_FOR_FAMILY_BRIDGE,
            "total_candidate_works": total_candidate_works,
            "paper_scores_written_count": paper_scores_written,
            "paper_scores_by_family": dict(paper_scores_by_family),
            "emerging_family_work_count": int(paper_scores_by_family.get("emerging", 0)),
            "recommended_next_stage": "rerun_fresh_product_candidate_ranking_source_after_ranking_v1",
        },
        "sql_write_report": {
            "writes_enabled": not dry_run,
            "allowed_tables": list(ALLOWED_WRITE_TABLES) if not dry_run else [],
            "affected_row_counts": affected,
            "ranking_runs_written": not dry_run,
            "paper_scores_written": not dry_run,
            "production_tables_modified": False,
            "works_modified": False,
            "embeddings_modified": False,
        },
        "candidate_source_handoff": {
            "ranking_run_id": ranking_run_id,
            "family": family,
            "snapshot_version": snapshot_version,
            "commands": _handoff_commands(ranking_run_id=ranking_run_id, family=family),
        },
        "blocked_actions": [
            "hybrid_validation",
            "shadow",
            "production_default",
            "api_web_change",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }


def build_ml_fresh_hybrid_product_candidate_ranking_payload(
    *,
    fresh_hybrid_snapshot_embeddings_path: Path,
    fresh_hybrid_snapshot_hydration_path: Path,
    fresh_hybrid_candidate_plan_ingest_path: Path,
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
    embeddings_path = Path(fresh_hybrid_snapshot_embeddings_path).resolve()
    hydration_path = Path(fresh_hybrid_snapshot_hydration_path).resolve()
    ingest_path = Path(fresh_hybrid_candidate_plan_ingest_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    embeddings_payload = _load_json_object(embeddings_path)
    hydration_payload = _load_json_object(hydration_path)
    ingest_payload = _load_json_object(ingest_path)
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
    _validate_policy(policy_payload)

    inputs = [
        _input_record("fresh_hybrid_snapshot_embeddings", embeddings_path, repo_root=root),
        _input_record("fresh_hybrid_snapshot_hydration", hydration_path, repo_root=root),
        _input_record("fresh_hybrid_candidate_plan_ingest", ingest_path, repo_root=root),
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

    run: RankingRun | None = None
    paper_scores: dict[str, int] = {}
    if not dry_run:
        try:
            run = _run_ranking(
                database_url=dsn,
                ranking_version=(ranking_version or DEFAULT_RANKING_VERSION).strip(),
                snapshot_version=resolved_snapshot,
                embedding_version=embedding_label,
            )
        except Exception as exc:
            raise MLFreshHybridProductCandidateRankingError(
                "fresh hybrid product-candidate ranking failed: "
                f"{safe_exception_summary(exc)}",
                code=1,
            ) from exc
        with psycopg.connect(dsn, autocommit=False) as conn:
            paper_scores = _paper_scores_by_family(conn, ranking_run_id=run.ranking_run_id)
            if not paper_scores:
                raise MLFreshHybridProductCandidateRankingError(
                    f"ranking_run_id {run.ranking_run_id!r} wrote no paper_scores rows"
                )
    return _artifact_from_run(
        artifact_version=artifact_version,
        inputs=inputs,
        database_summary=database_summary,
        snapshot_version=resolved_snapshot,
        embedding_version=embedding_label,
        ranking_version=(ranking_version or DEFAULT_RANKING_VERSION).strip(),
        family=(family or DEFAULT_FAMILY).strip(),
        dry_run=dry_run,
        validation_counts=validation_counts,
        run=run,
        paper_scores_by_family=paper_scores,
    )


def markdown_from_ml_fresh_hybrid_product_candidate_ranking(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    result = payload["ranking_result"]
    write_report = payload["sql_write_report"]
    handoff = payload["candidate_source_handoff"]
    commands = handoff.get("commands") if isinstance(handoff, Mapping) else {}
    lines = [
        f"# Fresh Hybrid Product-Candidate Ranking ({metadata['artifact_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records an eval-only product-candidate ranking run for the fresh hybrid confirmation path. It materializes ranking_runs and paper_scores only; it does not validate the hybrid scorer or authorize shadow/production.",
        "",
        f"- **Status:** `{result['status']}`",
        f"- **Ranking run ID:** `{result['ranking_run_id']}`",
        f"- **Snapshot version:** `{result['corpus_snapshot_version']}`",
        f"- **Embedding version:** `{result['embedding_version']}`",
        f"- **Ranking version:** `{result['ranking_version']}`",
        f"- **Cluster version:** `{result['cluster_version']}`",
        f"- **Total candidate works:** {result['total_candidate_works']}",
        f"- **paper_scores written:** {result['paper_scores_written_count']}",
        f"- **Recommended next stage:** `{result['recommended_next_stage']}`",
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
            f"- production tables modified: {write_report['production_tables_modified']}",
            f"- works modified: {write_report['works_modified']}",
            f"- embeddings modified: {write_report['embeddings_modified']}",
            "",
            "## Source Discovery And Materialize Handoff",
            "",
        ]
    )
    if isinstance(commands, Mapping) and commands.get("fresh_product_candidate_ranking_source"):
        lines.extend(
            [
                "Fresh source discovery:",
                "",
                f"```powershell\n{commands['fresh_product_candidate_ranking_source']}\n```",
                "",
                "Fresh surface materialization:",
                "",
                f"```powershell\n{commands['fresh_eval_surface_hybrid_materialize']}\n```",
            ]
        )
    else:
        lines.append(str(commands.get("note") if isinstance(commands, Mapping) else "Dry run only."))
    lines.extend(
        [
            "",
            "## Not Hybrid Validation / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_hybrid_product_candidate_ranking(
    *,
    fresh_hybrid_snapshot_embeddings_path: Path,
    fresh_hybrid_snapshot_hydration_path: Path,
    fresh_hybrid_candidate_plan_ingest_path: Path,
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
    payload = build_ml_fresh_hybrid_product_candidate_ranking_payload(
        fresh_hybrid_snapshot_embeddings_path=fresh_hybrid_snapshot_embeddings_path,
        fresh_hybrid_snapshot_hydration_path=fresh_hybrid_snapshot_hydration_path,
        fresh_hybrid_candidate_plan_ingest_path=fresh_hybrid_candidate_plan_ingest_path,
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
        markdown_from_ml_fresh_hybrid_product_candidate_ranking(payload),
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
    "MLFreshHybridProductCandidateRankingError",
    "build_ml_fresh_hybrid_product_candidate_ranking_payload",
    "markdown_from_ml_fresh_hybrid_product_candidate_ranking",
    "write_ml_fresh_hybrid_product_candidate_ranking",
]
