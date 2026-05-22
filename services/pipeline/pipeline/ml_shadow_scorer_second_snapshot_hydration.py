"""Hydrate the second shadow-generalization source snapshot with OpenAlex metadata.

This audit wrapper validates the second candidate-plan ingest provenance,
checks the local eval-only snapshot, delegates metadata/text hydration to the
existing corpus-v2 OpenAlex hydrator, and emits a shadow-generalization audit
artifact. It does not embed, rank, write paper_scores, execute scorers, import
labels, or authorize shadow/production behavior.
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping

import psycopg

from pipeline.corpus_v2_hydrate_openalex import (
    _fetch_openalex_work_by_id,
    _is_defaulted_language,
    _is_embedding_ready,
    _is_unknown_type,
    _load_snapshot_works,
    run_corpus_v2_hydrate_openalex,
)
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_second_candidate_plan_ingest import (
    EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
    EXPECTED_SELECTED_TOTAL,
    MLShadowScorerSecondCandidatePlanIngestError,
    _database_url_from_env,
    assert_local_database_url,
)
from pipeline.openalex_client import OPENALEX_API_KEY_ENV, compute_contact_provenance, openalex_api_key_from_env
from pipeline.policy import CorpusPolicy
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_hydration"
HYDRATION_VERSION = "ml-shadow-scorer-v1-second-snapshot-hydration-v1"
INGEST_ARTIFACT_TYPE = "ml_shadow_scorer_second_candidate_plan_ingest"
INGEST_VERSION = "ml-shadow-scorer-v1-second-candidate-plan-ingest-v1"
PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_second_hybrid_candidate_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
DEFAULT_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
NEXT_STAGE_READY = "embed_second_shadow_generalization_snapshot_v1"
NEXT_STAGE_REPAIR = "repair_or_retry_second_shadow_generalization_snapshot_hydration"

ALLOWED_WRITE_TABLES = ("ingest_runs", "raw_openalex_works", "works")

CAVEATS = (
    "Metadata/text hydration only; not validation, ranking, scorer execution, shadow, or production.",
    "Snapshot remains eval-only / shadow_generalization_candidate_source.",
    "Overlap works are hydrated for traceability; confirmatory exclusion happens later.",
    "Embedding readiness is metadata readiness only, not embedding coverage or validation evidence.",
    "No online shadow, API/web, production default, or runtime implementation is authorized.",
)


class MLShadowScorerSecondSnapshotHydrationError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondSnapshotHydrationError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondSnapshotHydrationError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondSnapshotHydrationError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondSnapshotHydrationError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_ingest(payload: Mapping[str, Any], *, snapshot_version: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="second-candidate-plan-ingest")
    if metadata.get("artifact_type") != INGEST_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSnapshotHydrationError(
            "ingest metadata.artifact_type must be ml_shadow_scorer_second_candidate_plan_ingest"
        )
    if metadata.get("ingest_version") != INGEST_VERSION:
        raise MLShadowScorerSecondSnapshotHydrationError(
            "ingest_version must be ml-shadow-scorer-v1-second-candidate-plan-ingest-v1"
        )
    if _get(payload, "ingest_result.status") != "succeeded":
        raise MLShadowScorerSecondSnapshotHydrationError("ingest_result.status must be succeeded")
    if _get(payload, "ingest_result.recommended_next_stage") != "hydrate_second_shadow_generalization_snapshot_metadata_v1":
        raise MLShadowScorerSecondSnapshotHydrationError(
            "ingest_result.recommended_next_stage must be hydrate_second_shadow_generalization_snapshot_metadata_v1"
        )
    top_next = payload.get("recommended_next_stage")
    if top_next is not None and top_next != "hydrate_second_shadow_generalization_snapshot_metadata_v1":
        raise MLShadowScorerSecondSnapshotHydrationError(
            "top-level recommended_next_stage must be hydrate_second_shadow_generalization_snapshot_metadata_v1"
        )
    observed_snapshot = _get(payload, "snapshot.source_snapshot_version")
    if observed_snapshot != snapshot_version:
        raise MLShadowScorerSecondSnapshotHydrationError(
            f"snapshot-version {snapshot_version!r} does not match ingest artifact {observed_snapshot!r}"
        )
    if _get(payload, "snapshot.shadow_generalization_candidate_source") is not True:
        raise MLShadowScorerSecondSnapshotHydrationError("ingest snapshot must be shadow_generalization_candidate_source")
    if _get(payload, "ingest_result.snapshot_work_count") != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondSnapshotHydrationError("ingest_result.snapshot_work_count must be 528")
    if _get(payload, "candidate_plan_summary.selected_total") != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondSnapshotHydrationError("candidate_plan_summary.selected_total must be 528")
    if _get(payload, "candidate_plan_summary.planned_candidate_work_set_sha256") != EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256:
        raise MLShadowScorerSecondSnapshotHydrationError("planned candidate work-set SHA mismatch")
    if _get(payload, "sql_write_report.ranking_runs_written") is not False:
        raise MLShadowScorerSecondSnapshotHydrationError("ingest artifact must not have written ranking_runs")
    if _get(payload, "sql_write_report.paper_scores_written") is not False:
        raise MLShadowScorerSecondSnapshotHydrationError("ingest artifact must not have written paper_scores")
    if _get(payload, "sql_write_report.embeddings_written") is not False:
        raise MLShadowScorerSecondSnapshotHydrationError("ingest artifact must not have written embeddings")
    if _get(payload, "sql_write_report.production_tables_modified") is not False:
        raise MLShadowScorerSecondSnapshotHydrationError("ingest artifact must not have modified production tables")
    return metadata


def _validate_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="second-hybrid-candidate-plan")
    if metadata.get("artifact_type") != PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSnapshotHydrationError("candidate plan artifact_type mismatch")
    if metadata.get("plan_version") != PLAN_VERSION:
        raise MLShadowScorerSecondSnapshotHydrationError(
            "candidate plan_version must be ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1"
        )
    return metadata


def _validate_plan_input_sha(ingest_payload: Mapping[str, Any], *, actual_plan_sha: str) -> None:
    inputs = _get(ingest_payload, "metadata.inputs")
    if not isinstance(inputs, list):
        raise MLShadowScorerSecondSnapshotHydrationError("ingest metadata.inputs missing")
    for record in inputs:
        if isinstance(record, Mapping) and record.get("name") == "second_hybrid_candidate_plan":
            if record.get("sha256") != actual_plan_sha:
                raise MLShadowScorerSecondSnapshotHydrationError("second_hybrid_candidate_plan sha256 mismatch")
            return
    raise MLShadowScorerSecondSnapshotHydrationError("ingest metadata.inputs missing second_hybrid_candidate_plan record")


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-audit-plan")
    if metadata.get("artifact_type") != GENERALIZATION_PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSnapshotHydrationError("generalization audit plan artifact_type mismatch")
    if metadata.get("plan_version") != GENERALIZATION_PLAN_VERSION:
        raise MLShadowScorerSecondSnapshotHydrationError(
            "generalization audit plan_version must be ml-shadow-scorer-v1-generalization-audit-v1"
        )
    if payload.get("generalization_audit_plan_defined") is not True:
        raise MLShadowScorerSecondSnapshotHydrationError("generalization audit plan must be defined")
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondSnapshotHydrationError("runtime implementation must remain unauthorized")
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSnapshotHydrationError("fresh surface policy artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLShadowScorerSecondSnapshotHydrationError(
            "fresh surface policy_version must be ml-fresh-eval-surface-policy-hybrid-v1"
        )
    return metadata


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return assert_local_database_url(database_url)
    except MLShadowScorerSecondCandidatePlanIngestError as exc:
        raise MLShadowScorerSecondSnapshotHydrationError(str(exc), code=exc.code) from exc


def _snapshot_profile(conn: Any, *, snapshot_version: str, expected_work_count: int) -> dict[str, Any]:
    exists = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    if exists is None:
        raise MLShadowScorerSecondSnapshotHydrationError(f"snapshot_version not found in local Postgres: {snapshot_version}")
    rows = _load_snapshot_works(conn, snapshot_version=snapshot_version)
    if len(rows) != expected_work_count:
        raise MLShadowScorerSecondSnapshotHydrationError(
            f"snapshot work count mismatch for {snapshot_version}: expected {expected_work_count}, found {len(rows)}"
        )
    accepted_types = {t.casefold() for t in CorpusPolicy().include_document_types}
    embedding_ready = sum(1 for row in rows if _is_embedding_ready(row, accepted_types=accepted_types))
    return {
        "works_considered_count": len(rows),
        "abstract_count": sum(1 for row in rows if isinstance(row.abstract, str) and bool(row.abstract.strip())),
        "type_unknown_count": sum(1 for row in rows if _is_unknown_type(row.work_type)),
        "language_defaulted_count": sum(1 for row in rows if _is_defaulted_language(row.language)),
        "doi_present_count": sum(1 for row in rows if isinstance(row.doi, str) and bool(row.doi.strip())),
        "embedding_ready_count": embedding_ready,
        "embedding_blocked_count": len(rows) - embedding_ready,
        "snapshot_embedding_ready": embedding_ready == len(rows),
    }


def _mock_fetch_work(openalex_id: str) -> Mapping[str, Any]:
    token = str(openalex_id).rstrip("/").split("/")[-1] or "W0"
    return {
        "id": openalex_id,
        "title": f"Mock hydrated second shadow-generalization work {token}",
        "type": "article",
        "language": "en",
        "doi": f"10.5555/{token.casefold()}",
        "cited_by_count": 1,
        "publication_year": 2026,
        "publication_date": "2026-01-01",
        "updated_date": "2026-05-21",
        "abstract_inverted_index": {
            "music": [0],
            "information": [1],
            "retrieval": [2],
            "generalization": [3],
        },
    }


def _live_fetch_work_with_retries(openalex_id: str, *, attempts: int = 3) -> Mapping[str, Any] | None:
    for attempt in range(1, attempts + 1):
        try:
            return _fetch_openalex_work_by_id(openalex_id, mock_openalex=False)
        except Exception:
            if attempt >= attempts:
                return None
            time.sleep(min(float(attempt), 2.0))
    return None


@contextmanager
def _temporary_openalex_mailto(mailto: str | None) -> Iterator[None]:
    cleaned = (mailto or "").strip()
    if not cleaned:
        yield
        return
    sentinel = object()
    previous: object = os.environ.get("OPENALEX_MAILTO", sentinel)  # type: ignore[assignment]
    os.environ["OPENALEX_MAILTO"] = cleaned
    try:
        yield
    finally:
        if previous is sentinel:
            os.environ.pop("OPENALEX_MAILTO", None)
        else:
            os.environ["OPENALEX_MAILTO"] = str(previous)


def _artifact_from_summary(
    *,
    hydration_version: str,
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    snapshot_version: str,
    dry_run: bool,
    mock_openalex: bool,
    mailto: str | None,
    ingest_payload: Mapping[str, Any],
    plan_metadata: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    blocked = int(summary.get("embedding_blocked_count") or 0)
    ready = bool(summary.get("snapshot_embedding_ready"))
    status = "dry_run_validated" if dry_run else str(summary.get("status") or "succeeded")
    fetched = int(summary.get("fetched_count") or 0)
    updated = int(summary.get("updated_count") or 0)
    raw_payloads = int(summary.get("raw_payload_upserted_count") or 0)
    writes_enabled = not dry_run
    contact_mode, contact_provided = compute_contact_provenance(mailto_cli=mailto or "", mock_openalex=mock_openalex)
    recommended_next_stage = NEXT_STAGE_READY if ready else NEXT_STAGE_REPAIR
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "hydration_version": hydration_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "snapshot_version": snapshot_version,
            "dry_run": dry_run,
            "mock_openalex": mock_openalex,
            "openalex_contact_provenance": {
                "contact_mode": contact_mode,
                "contact_provided": contact_provided,
                "api_key_provided": bool(openalex_api_key_from_env()) if not mock_openalex else False,
                "raw_mailto_stored": False,
            },
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
        },
        "ingest_provenance": {
            "second_candidate_plan_ingest_version": _get(ingest_payload, "metadata.ingest_version"),
            "second_hybrid_candidate_plan_version": plan_metadata.get("plan_version"),
            "planned_candidate_work_set_sha256": _get(
                ingest_payload,
                "candidate_plan_summary.planned_candidate_work_set_sha256",
            ),
            "snapshot_work_count": _get(ingest_payload, "ingest_result.snapshot_work_count"),
            "shadow_generalization_candidate_source": _get(
                ingest_payload,
                "snapshot.shadow_generalization_candidate_source",
            ),
        },
        "hydration_result": {
            "status": status,
            "hydration_run_id": summary.get("hydration_run_id"),
            "works_considered_count": summary.get("works_considered_count"),
            "fetched_count": fetched,
            "updated_count": updated,
            "failed_count": int(summary.get("failed_count") or 0),
            "raw_payload_upserted_count": raw_payloads,
            "abstract_before_count": summary.get("abstract_before_count"),
            "abstract_after_count": summary.get("abstract_after_count"),
            "abstract_added_count": summary.get("abstract_added_count"),
            "type_unknown_before_count": summary.get("type_unknown_before_count"),
            "type_unknown_after_count": summary.get("type_unknown_after_count"),
            "language_defaulted_before_count": summary.get("language_defaulted_before_count"),
            "language_defaulted_after_count": summary.get("language_defaulted_after_count"),
            "doi_added_count": summary.get("doi_added_count"),
            "embedding_ready_count": summary.get("embedding_ready_count"),
            "embedding_blocked_count": blocked,
            "snapshot_embedding_ready": ready,
            "snapshot_embedding_ready_criteria": "embedding_blocked_count == 0 using corpus-v2 title+abstract+type+language readiness rules",
            "recommended_next_stage": recommended_next_stage,
            "warnings": list(summary.get("warnings") or []),
        },
        "sql_write_report": {
            "writes_enabled": writes_enabled,
            "allowed_tables": list(ALLOWED_WRITE_TABLES) if writes_enabled else [],
            "affected_row_counts": (
                {
                    "ingest_runs": 2,
                    "raw_openalex_works": raw_payloads,
                    "works_updated": updated,
                }
                if writes_enabled
                else {}
            ),
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "embeddings_written": False,
            "production_tables_modified": False,
        },
        "blocked_actions": [
            "ranking_run_creation",
            "paper_scores_write",
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
            "missing_second_surface_ranking_run": True,
            "missing_second_surface_embedding_coverage": True,
            "missing_second_surface_learned_probability_coverage": True,
            "missing_generalization_audit_on_second_surface": True,
            "missing_generalization_audit_gates": True,
            "missing_online_shadow_implementation_disabled_by_default": True,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "recommended_next_stage": recommended_next_stage,
        "caveats": list(CAVEATS),
    }


def build_ml_shadow_scorer_second_snapshot_hydration_payload(
    *,
    second_candidate_plan_ingest_path: Path,
    second_hybrid_candidate_plan_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    snapshot_version: str | None = None,
    database_url: str | None = None,
    mailto: str | None = None,
    mock_openalex: bool = False,
    dry_run: bool = False,
    hydration_version: str = HYDRATION_VERSION,
    repo_root: Path | None = None,
    run_hydrator: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    ingest_path = Path(second_candidate_plan_ingest_path).resolve()
    plan_path = Path(second_hybrid_candidate_plan_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    resolved_snapshot = (snapshot_version or DEFAULT_SNAPSHOT_VERSION).strip()

    ingest_payload = _load_json_object(ingest_path)
    plan_payload = _load_json_object(plan_path)
    audit_plan_payload = _load_json_object(audit_plan_path)
    policy_payload = _load_json_object(policy_path)

    _validate_ingest(ingest_payload, snapshot_version=resolved_snapshot)
    plan_sha = sha256_file(plan_path)
    _validate_plan_input_sha(ingest_payload, actual_plan_sha=plan_sha)
    plan_metadata = _validate_plan(plan_payload)
    _validate_generalization_plan(audit_plan_payload)
    _validate_policy(policy_payload)
    expected_work_count = int(_get(ingest_payload, "ingest_result.snapshot_work_count") or 0)
    inputs = [
        _input_record("second_candidate_plan_ingest", ingest_path, repo_root=root),
        _input_record("second_hybrid_candidate_plan", plan_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
    ]
    dsn = database_url or _database_url_from_env()
    database_summary = _assert_local_database_url(dsn)
    if not dry_run and not mock_openalex and not openalex_api_key_from_env():
        raise MLShadowScorerSecondSnapshotHydrationError(
            f"live hydration requires {OPENALEX_API_KEY_ENV} because corpus-v2 hydration requires API-key auth; "
            "--mailto alone is not sufficient"
        )

    with psycopg.connect(dsn, autocommit=False) as conn:
        before = _snapshot_profile(conn, snapshot_version=resolved_snapshot, expected_work_count=expected_work_count)

    if dry_run:
        summary = {
            "snapshot_version": resolved_snapshot,
            "hydration_run_id": None,
            "works_considered_count": before["works_considered_count"],
            "fetched_count": 0,
            "updated_count": 0,
            "failed_count": 0,
            "raw_payload_upserted_count": 0,
            "abstract_before_count": before["abstract_count"],
            "abstract_after_count": before["abstract_count"],
            "abstract_added_count": 0,
            "type_unknown_before_count": before["type_unknown_count"],
            "type_unknown_after_count": before["type_unknown_count"],
            "language_defaulted_before_count": before["language_defaulted_count"],
            "language_defaulted_after_count": before["language_defaulted_count"],
            "doi_added_count": 0,
            "embedding_ready_count": before["embedding_ready_count"],
            "embedding_blocked_count": before["embedding_blocked_count"],
            "snapshot_embedding_ready": before["snapshot_embedding_ready"],
            "warnings": ["Dry run only: no OpenAlex calls and no database writes were performed."],
        }
    else:
        if not run_hydrator:
            raise MLShadowScorerSecondSnapshotHydrationError("internal: run_hydrator=false is only useful with dry_run")
        with _temporary_openalex_mailto(mailto):
            summary = run_corpus_v2_hydrate_openalex(
                snapshot_version=resolved_snapshot,
                output_path=Path(os.devnull),
                markdown_output_path=Path(os.devnull),
                database_url=dsn,
                mock_openalex=mock_openalex,
                fetch_work=_mock_fetch_work if mock_openalex else _live_fetch_work_with_retries,
            )
    return _artifact_from_summary(
        hydration_version=hydration_version,
        inputs=inputs,
        database_summary=database_summary,
        snapshot_version=resolved_snapshot,
        dry_run=dry_run,
        mock_openalex=mock_openalex,
        mailto=mailto,
        ingest_payload=ingest_payload,
        plan_metadata=plan_metadata,
        summary=summary,
    )


def markdown_from_ml_shadow_scorer_second_snapshot_hydration(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    result = payload["hydration_result"]
    provenance = payload["ingest_provenance"]
    write_report = payload["sql_write_report"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# Second Shadow-Generalization Snapshot Hydration ({metadata['hydration_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records metadata/text hydration for the second shadow-generalization source snapshot. It prepares the snapshot for a later embedding step only; it does not rank, score, import labels, or authorize shadow/production.",
        "",
        f"- **Snapshot version:** `{metadata['snapshot_version']}`",
        f"- **Status:** `{result['status']}`",
        f"- **Mock OpenAlex:** {metadata['mock_openalex']}",
        f"- **Dry run:** {metadata['dry_run']}",
        f"- **Works considered:** {result['works_considered_count']}",
        f"- **Fetched / updated / failed:** {result['fetched_count']} / {result['updated_count']} / {result['failed_count']}",
        f"- **Snapshot embedding ready:** {result['snapshot_embedding_ready']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        "",
        "## Ingest Provenance",
        "",
        f"- Ingest version: `{provenance['second_candidate_plan_ingest_version']}`",
        f"- Candidate plan version: `{provenance['second_hybrid_candidate_plan_version']}`",
        f"- Candidate work-set SHA: `{provenance['planned_candidate_work_set_sha256']}`",
        f"- Snapshot work count: {provenance['snapshot_work_count']}",
        f"- Shadow-generalization candidate source: {provenance['shadow_generalization_candidate_source']}",
        "",
        "## Hydration Counts",
        "",
        f"- Raw payloads upserted: {result['raw_payload_upserted_count']}",
        f"- DOI added: {result['doi_added_count']}",
        "",
        "## Before/After Text Readiness",
        "",
        f"- Abstracts: {result['abstract_before_count']} -> {result['abstract_after_count']} (+{result['abstract_added_count']})",
        f"- Unknown/default type: {result['type_unknown_before_count']} -> {result['type_unknown_after_count']}",
        f"- Defaulted language: {result['language_defaulted_before_count']} -> {result['language_defaulted_after_count']}",
        "",
        "## Embedding Readiness",
        "",
        f"- Embedding ready: {result['embedding_ready_count']}",
        f"- Embedding blocked: {result['embedding_blocked_count']}",
        f"- Criterion: {result['snapshot_embedding_ready_criteria']}",
        "",
        "## DB Write Scope",
        "",
        f"- Writes enabled: {write_report['writes_enabled']}",
        f"- Allowed tables: {', '.join(write_report['allowed_tables']) if write_report['allowed_tables'] else 'none'}",
        f"- ranking_runs written: {write_report['ranking_runs_written']}",
        f"- paper_scores written: {write_report['paper_scores_written']}",
        f"- embeddings written: {write_report['embeddings_written']}",
        f"- production tables modified: {write_report['production_tables_modified']}",
        "",
        "## Remaining Blockers",
        "",
    ]
    for key, value in blockers.items():
        lines.append(f"- `{key}`: {value}")
    warnings = list(result.get("warnings") or [])
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- None")
    lines.extend(
        [
            "",
            "## Not Ranking / Not Embeddings Yet / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_shadow_scorer_second_snapshot_hydration(
    *,
    second_candidate_plan_ingest_path: Path,
    second_hybrid_candidate_plan_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    snapshot_version: str | None = None,
    database_url: str | None = None,
    mailto: str | None = None,
    mock_openalex: bool = False,
    dry_run: bool = False,
    hydration_version: str = HYDRATION_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_second_snapshot_hydration_payload(
        second_candidate_plan_ingest_path=second_candidate_plan_ingest_path,
        second_hybrid_candidate_plan_path=second_hybrid_candidate_plan_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        snapshot_version=snapshot_version,
        database_url=database_url,
        mailto=mailto,
        mock_openalex=mock_openalex,
        dry_run=dry_run,
        hydration_version=hydration_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_snapshot_hydration(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "HYDRATION_VERSION",
    "MLShadowScorerSecondSnapshotHydrationError",
    "build_ml_shadow_scorer_second_snapshot_hydration_payload",
    "markdown_from_ml_shadow_scorer_second_snapshot_hydration",
    "write_ml_shadow_scorer_second_snapshot_hydration",
]
