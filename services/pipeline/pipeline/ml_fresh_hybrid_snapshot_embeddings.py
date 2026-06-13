"""Fresh hybrid snapshot embedding audit wrapper.

This validates the fresh-hybrid hydration chain, delegates title+abstract
embedding generation to corpus-v2 embedding machinery, and emits a fresh-hybrid
audit artifact. It does not cluster, rank, write paper_scores, score hybrids,
import labels, or authorize shadow/prod.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg

from pipeline.corpus_v2_embed import (
    OPENAI_API_KEY_ENV,
    PROVIDER,
    TEXT_SOURCE,
    CorpusV2EmbedError,
    _load_target_works,
    run_corpus_v2_embed,
)
from pipeline.embedding_provider import DEFAULT_OPENAI_EMBEDDING_MODEL, EXPECTED_EMBEDDING_DIMENSIONS
from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
    MLFreshHybridCandidatePlanIngestError,
    _database_url_from_env,
    assert_local_database_url,
)
from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_hybrid_snapshot_embeddings"
ARTIFACT_VERSION = "ml-fresh-hybrid-snapshot-embeddings-v1"
DEFAULT_EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
HYDRATION_ARTIFACT_TYPE = "ml_fresh_hybrid_snapshot_hydration"
HYDRATION_VERSION = "ml-fresh-hybrid-snapshot-hydration-v1"
INGEST_ARTIFACT_TYPE = "ml_fresh_hybrid_candidate_plan_ingest"
INGEST_VERSION = "ml-fresh-hybrid-candidate-plan-ingest-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"

ALLOWED_WRITE_TABLES = ("embeddings",)

CAVEATS = (
    "Controlled embedding generation for one eval-only fresh hybrid snapshot only.",
    "Embeddings use existing corpus-v2 title+abstract text rules and persistence machinery.",
    "No clustering, ranking run, paper_scores, label import, hybrid scoring, shadow, or production changes.",
    "Snapshot remains eval-only / fresh hybrid validation candidate source; it is not a production/default corpus switch.",
    "Embedding coverage is a pipeline readiness artifact, not validation or ranking evidence.",
    "No shadow or production authorization.",
)


class MLFreshHybridSnapshotEmbeddingsError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _MockEmbeddingProvider:
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS

    def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            seed = hashlib.sha256(text.encode("utf-8")).digest()
            values = [float(seed[index % len(seed)]) / 255.0 for index in range(self.expected_dimensions)]
            vectors.append(values)
        return vectors


def _now_iso_z() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshHybridSnapshotEmbeddingsError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshHybridSnapshotEmbeddingsError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshHybridSnapshotEmbeddingsError(f"{name} JSON missing metadata object")
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
        raise MLFreshHybridSnapshotEmbeddingsError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return assert_local_database_url(database_url)
    except MLFreshHybridCandidatePlanIngestError as exc:
        raise MLFreshHybridSnapshotEmbeddingsError(str(exc), code=exc.code) from exc


def _validate_hydration(payload: Mapping[str, Any], *, snapshot_version: str | None) -> str:
    metadata = _metadata(payload, name="fresh-hybrid-snapshot-hydration")
    if metadata.get("artifact_type") != HYDRATION_ARTIFACT_TYPE:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration metadata.artifact_type mismatch")
    if metadata.get("hydration_version") != HYDRATION_VERSION:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration_version must be ml-fresh-hybrid-snapshot-hydration-v1")
    if _get(payload, "hydration_result.status") != "succeeded":
        raise MLFreshHybridSnapshotEmbeddingsError("hydration_result.status must be succeeded")
    if _get(payload, "hydration_result.snapshot_embedding_ready") is not True:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration_result.snapshot_embedding_ready must be true")
    if _get(payload, "hydration_result.recommended_next_stage") != "embed_fresh_hybrid_snapshot_v1":
        raise MLFreshHybridSnapshotEmbeddingsError(
            "hydration_result.recommended_next_stage must be embed_fresh_hybrid_snapshot_v1"
        )
    if _get(payload, "sql_write_report.ranking_runs_written") is not False:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration artifact must not have written ranking_runs")
    if _get(payload, "sql_write_report.paper_scores_written") is not False:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration artifact must not have written paper_scores")
    if _get(payload, "sql_write_report.production_tables_modified") is not False:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration artifact must not have modified production tables")
    observed_snapshot = metadata.get("snapshot_version")
    if not isinstance(observed_snapshot, str) or not observed_snapshot.strip():
        raise MLFreshHybridSnapshotEmbeddingsError("hydration metadata.snapshot_version is missing")
    if snapshot_version and snapshot_version != observed_snapshot:
        raise MLFreshHybridSnapshotEmbeddingsError(
            f"snapshot-version {snapshot_version!r} does not match hydration artifact {observed_snapshot!r}"
        )
    return observed_snapshot


def _validate_ingest(payload: Mapping[str, Any], *, snapshot_version: str) -> None:
    metadata = _metadata(payload, name="fresh-hybrid-candidate-plan-ingest")
    if metadata.get("artifact_type") != INGEST_ARTIFACT_TYPE:
        raise MLFreshHybridSnapshotEmbeddingsError("ingest metadata.artifact_type mismatch")
    if metadata.get("ingest_version") != INGEST_VERSION:
        raise MLFreshHybridSnapshotEmbeddingsError("ingest_version must be ml-fresh-hybrid-candidate-plan-ingest-v1")
    if _get(payload, "snapshot.source_snapshot_version") != snapshot_version:
        raise MLFreshHybridSnapshotEmbeddingsError("ingest snapshot.source_snapshot_version does not match snapshot")


def _validate_policy(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshHybridSnapshotEmbeddingsError("fresh surface policy artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshHybridSnapshotEmbeddingsError("fresh surface policy_version must be ml-fresh-eval-surface-policy-hybrid-v1")


def _snapshot_work_count(conn: Any, *, snapshot_version: str) -> int:
    exists = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    if exists is None:
        raise MLFreshHybridSnapshotEmbeddingsError(f"snapshot_version not found in local Postgres: {snapshot_version}")
    return len(_load_target_works(conn, snapshot_version=snapshot_version))


def _embedding_coverage(conn: Any, *, snapshot_version: str, embedding_version: str) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT COUNT(*), COUNT(e.work_id)
        FROM works w
        JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = w.id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        LEFT JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        """,
        (snapshot_version, embedding_version),
    ).fetchone()
    snapshot_count = int(row[0] or 0) if row is not None else 0
    embedded = int(row[1] or 0) if row is not None else 0
    return {
        "snapshot_work_count": snapshot_count,
        "embedded_work_count": embedded,
        "missing_embedding_count": max(snapshot_count - embedded, 0),
    }


def _cluster_required_before_ranking() -> bool:
    return False


def _artifact_from_summary(
    *,
    artifact_version: str,
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    snapshot_version: str,
    embedding_version: str,
    mock_embeddings: bool,
    dry_run: bool,
    limit: int | None,
    hydration_payload: Mapping[str, Any],
    summary: Mapping[str, Any],
    coverage: Mapping[str, int],
) -> dict[str, Any]:
    written = int(summary.get("embedded_count") or 0)
    skipped = int(summary.get("skipped_count") or 0)
    failed = int(summary.get("failed_count") or 0)
    dimensions = int(summary.get("embedding_dimension") or (EXPECTED_EMBEDDING_DIMENSIONS if mock_embeddings else 0))
    cluster_required = _cluster_required_before_ranking()
    full_coverage = int(coverage.get("missing_embedding_count") or 0) == 0
    status = "dry_run_validated" if dry_run else str(summary.get("status") or "succeeded")
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "artifact_version": artifact_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "snapshot_version": snapshot_version,
            "embedding_version": embedding_version,
            "mock_embeddings": mock_embeddings,
            "dry_run": dry_run,
            "limit": limit,
            "caveats": list(CAVEATS),
        },
        "embedding_result": {
            "status": status,
            "works_considered_count": summary.get("works_considered_count"),
            "embedding_ready_count_from_hydration": _get(hydration_payload, "hydration_result.embedding_ready_count"),
            "embeddings_written_count": written,
            "embeddings_skipped_existing_count": skipped,
            "embeddings_failed_count": failed,
            "embedding_provider": summary.get("provider") or PROVIDER,
            "embedding_model": summary.get("model") or DEFAULT_OPENAI_EMBEDDING_MODEL,
            "embedding_dimensions": dimensions,
            "text_source": summary.get("text_source") or TEXT_SOURCE,
            "input_text_sha256": summary.get("input_text_sha256"),
            "full_snapshot_embedding_coverage": full_coverage,
            "cluster_required_before_ranking": cluster_required,
            "recommended_next_stage": (
                "cluster_fresh_hybrid_snapshot_v1"
                if cluster_required
                else "run_fresh_hybrid_product_candidate_ranking_v1"
            ),
            "warnings": list(summary.get("warnings") or []),
        },
        "coverage": {
            "snapshot_work_count": int(coverage.get("snapshot_work_count") or 0),
            "embedded_work_count": int(coverage.get("embedded_work_count") or 0),
            "missing_embedding_count": int(coverage.get("missing_embedding_count") or 0),
        },
        "sql_write_report": {
            "writes_enabled": not dry_run,
            "allowed_tables": list(ALLOWED_WRITE_TABLES) if not dry_run else [],
            "affected_row_counts": {"embeddings": written} if not dry_run else {},
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "production_tables_modified": False,
        },
        "blocked_actions": [
            "clustering" if cluster_required else "clustering_if_later_required",
            "ranking_run",
            "hybrid_validation",
            "shadow",
            "production_default",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }


def build_ml_fresh_hybrid_snapshot_embeddings_payload(
    *,
    fresh_hybrid_snapshot_hydration_path: Path,
    fresh_hybrid_candidate_plan_ingest_path: Path,
    fresh_surface_policy_path: Path,
    snapshot_version: str | None = None,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    database_url: str | None = None,
    mock_embeddings: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    hydration_path = Path(fresh_hybrid_snapshot_hydration_path).resolve()
    ingest_path = Path(fresh_hybrid_candidate_plan_ingest_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    hydration_payload = _load_json_object(hydration_path)
    ingest_payload = _load_json_object(ingest_path)
    policy_payload = _load_json_object(policy_path)
    observed_snapshot = _validate_hydration(hydration_payload, snapshot_version=snapshot_version)
    resolved_snapshot = (snapshot_version or observed_snapshot).strip()
    _validate_ingest(ingest_payload, snapshot_version=resolved_snapshot)
    _validate_policy(policy_payload)
    expected_count = int(_get(hydration_payload, "hydration_result.works_considered_count") or 0)
    if expected_count <= 0:
        raise MLFreshHybridSnapshotEmbeddingsError("hydration works_considered_count must be > 0")
    if limit is not None and limit <= 0:
        raise MLFreshHybridSnapshotEmbeddingsError("--limit must be positive when supplied")
    embedding_label = (embedding_version or "").strip() or DEFAULT_EMBEDDING_VERSION
    inputs = [
        _input_record("fresh_hybrid_snapshot_hydration", hydration_path, repo_root=root),
        _input_record("fresh_hybrid_candidate_plan_ingest", ingest_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
    ]
    dsn = database_url or _database_url_from_env()
    database_summary = _assert_local_database_url(dsn)
    if not dry_run and not mock_embeddings and not os.environ.get(OPENAI_API_KEY_ENV):
        raise MLFreshHybridSnapshotEmbeddingsError(
            f"{OPENAI_API_KEY_ENV} is required for live fresh hybrid embedding generation"
        )

    with psycopg.connect(dsn, autocommit=False) as conn:
        snapshot_count = _snapshot_work_count(conn, snapshot_version=resolved_snapshot)
        if snapshot_count != expected_count:
            raise MLFreshHybridSnapshotEmbeddingsError(
                f"snapshot work count mismatch for {resolved_snapshot}: expected {expected_count}, found {snapshot_count}"
            )
        before_coverage = _embedding_coverage(conn, snapshot_version=resolved_snapshot, embedding_version=embedding_label)

    if dry_run:
        considered = min(expected_count, limit) if limit is not None else expected_count
        summary = {
            "snapshot_version": resolved_snapshot,
            "embedding_version": embedding_label,
            "works_considered_count": considered,
            "embedded_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "embedding_dimension": EXPECTED_EMBEDDING_DIMENSIONS,
            "model": DEFAULT_OPENAI_EMBEDDING_MODEL,
            "provider": PROVIDER,
            "text_source": TEXT_SOURCE,
            "input_text_sha256": None,
            "warnings": ["Dry run only: no embedding API calls and no database writes were performed."],
        }
        coverage = before_coverage
    else:
        try:
            summary = run_corpus_v2_embed(
                snapshot_version=resolved_snapshot,
                embedding_version=embedding_label,
                output_path=Path(os.devnull),
                markdown_output_path=Path(os.devnull),
                database_url=dsn,
                model=DEFAULT_OPENAI_EMBEDDING_MODEL,
                batch_size=32,
                replace=False,
                limit=limit,
                provider=_MockEmbeddingProvider() if mock_embeddings else None,
            )
        except CorpusV2EmbedError as exc:
            raise MLFreshHybridSnapshotEmbeddingsError(str(exc), code=exc.code) from exc
        with psycopg.connect(dsn, autocommit=False) as conn:
            coverage = _embedding_coverage(conn, snapshot_version=resolved_snapshot, embedding_version=embedding_label)
    return _artifact_from_summary(
        artifact_version=artifact_version,
        inputs=inputs,
        database_summary=database_summary,
        snapshot_version=resolved_snapshot,
        embedding_version=embedding_label,
        mock_embeddings=mock_embeddings,
        dry_run=dry_run,
        limit=limit,
        hydration_payload=hydration_payload,
        summary=summary,
        coverage=coverage,
    )


def markdown_from_ml_fresh_hybrid_snapshot_embeddings(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    result = payload["embedding_result"]
    coverage = payload["coverage"]
    write_report = payload["sql_write_report"]
    lines = [
        f"# Fresh Hybrid Snapshot Embeddings ({metadata['artifact_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records title+abstract embedding generation for the eval-only fresh hybrid snapshot. It does not cluster, rank, write paper_scores, score hybrids, import labels, or authorize shadow/production.",
        "",
        f"- **Snapshot version:** `{metadata['snapshot_version']}`",
        f"- **Embedding version:** `{metadata['embedding_version']}`",
        f"- **Status:** `{result['status']}`",
        f"- **Mock embeddings:** {metadata['mock_embeddings']}",
        f"- **Dry run:** {metadata['dry_run']}",
        f"- **Works considered:** {result['works_considered_count']}",
        f"- **Embeddings written / skipped / failed:** {result['embeddings_written_count']} / {result['embeddings_skipped_existing_count']} / {result['embeddings_failed_count']}",
        f"- **Full snapshot embedding coverage:** {result['full_snapshot_embedding_coverage']}",
        f"- **Recommended next stage:** `{result['recommended_next_stage']}`",
        "",
        "## Model And Dimensions",
        "",
        f"- Provider: `{result['embedding_provider']}`",
        f"- Model: `{result['embedding_model']}`",
        f"- Dimensions: {result['embedding_dimensions']}",
        f"- Text source: `{result['text_source']}`",
        "",
        "## Coverage",
        "",
        f"- Snapshot work count: {coverage['snapshot_work_count']}",
        f"- Embedded work count: {coverage['embedded_work_count']}",
        f"- Missing embedding count: {coverage['missing_embedding_count']}",
        "",
        "## Cluster Before Ranking",
        "",
        f"- cluster_required_before_ranking: {result['cluster_required_before_ranking']}",
        "",
        "## DB Write Scope",
        "",
        f"- Writes enabled: {write_report['writes_enabled']}",
        f"- Allowed tables: {', '.join(write_report['allowed_tables']) if write_report['allowed_tables'] else 'none'}",
        f"- ranking_runs written: {write_report['ranking_runs_written']}",
        f"- paper_scores written: {write_report['paper_scores_written']}",
        f"- production tables modified: {write_report['production_tables_modified']}",
        "",
        "## Next Stage",
        "",
        result["recommended_next_stage"],
        "",
        "## Not Ranking Yet / Not Shadow / Not Production",
        "",
    ]
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_hybrid_snapshot_embeddings(
    *,
    fresh_hybrid_snapshot_hydration_path: Path,
    fresh_hybrid_candidate_plan_ingest_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    snapshot_version: str | None = None,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    database_url: str | None = None,
    mock_embeddings: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_fresh_hybrid_snapshot_embeddings_payload(
        fresh_hybrid_snapshot_hydration_path=fresh_hybrid_snapshot_hydration_path,
        fresh_hybrid_candidate_plan_ingest_path=fresh_hybrid_candidate_plan_ingest_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        snapshot_version=snapshot_version,
        embedding_version=embedding_version,
        database_url=database_url,
        mock_embeddings=mock_embeddings,
        dry_run=dry_run,
        limit=limit,
        artifact_version=artifact_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_hybrid_snapshot_embeddings(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "ARTIFACT_VERSION",
    "DEFAULT_EMBEDDING_VERSION",
    "MLFreshHybridSnapshotEmbeddingsError",
    "build_ml_fresh_hybrid_snapshot_embeddings_payload",
    "markdown_from_ml_fresh_hybrid_snapshot_embeddings",
    "write_ml_fresh_hybrid_snapshot_embeddings",
]
