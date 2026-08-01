"""Ingest the second shadow-generalization candidate plan as a source snapshot.

This controlled local-Postgres write imports only the committed second hybrid
candidate plan rows into an eval-only source snapshot. It reuses the corpus-v2
candidate-plan ingest helpers and does not call OpenAlex, create embeddings,
rank, score, import labels, or authorize shadow/production behavior.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import psycopg

from pipeline.config import IngestRun, SourceSnapshotVersion
from pipeline.corpus_v2_ingest_from_plan import (
    CandidatePlanDocument,
    _derive_policy_identity,
    _ingest_plan_candidates,
    _mark_ingest_failed,
    _metadata_warnings,
    _register_snapshot_and_ingest_run,
    _update_ingest_run_final,
    validate_candidate_plan,
)
from pipeline.error_reporting import safe_exception_summary
from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
    _database_url_from_env,
    assert_local_database_url as _assert_fresh_local_database_url,
)
from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_candidate_plan_ingest"
INGEST_VERSION = "ml-shadow-scorer-v1-second-candidate-plan-ingest-v1"
PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_second_hybrid_candidate_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
OLD_EVAL_WORK_SET_SHA256 = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
EXPECTED_SELECTED_TOTAL = 528
EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256 = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
DEFAULT_INGEST_MODE = "snapshot-import"
SECOND_SHADOW_GENERALIZATION_INGEST_INTENT = "second_shadow_generalization_candidate_plan_snapshot_import"
NEXT_STAGE = "hydrate_second_shadow_generalization_snapshot_metadata_v1"

ALLOWED_WRITE_TABLES = (
    "source_snapshot_versions",
    "ingest_runs",
    "raw_openalex_works",
    "works",
)

CAVEATS = (
    "Snapshot ingest only; no OpenAlex enrichment, embeddings, ranking, scorer execution, shadow, or production changes.",
    "All 528 planned works are ingested, including old-217 and first-surface overlaps for audit traceability.",
    "Overlap tags are preserved from the plan; confirmatory eligibility is enforced later during materialization/gates.",
    "Underpowered-source overlap in the plan is preview-limited; this ingest does not claim full 59-work overlap.",
    "No online shadow, API/web, production default, or runtime implementation is authorized.",
)


class MLShadowScorerSecondCandidatePlanIngestError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _SecondPlanDocument:
    path: Path
    sha256: str
    payload: Mapping[str, Any]
    corpus_v2_payload: Mapping[str, Any]


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_snapshot_version() -> str:
    return f"source-snapshot-shadow-generalization-v1-{datetime.now(UTC).strftime('%Y%m%d')}"


def assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return dict(_assert_fresh_local_database_url(database_url))
    except Exception as exc:
        raise MLShadowScorerSecondCandidatePlanIngestError(str(exc)) from exc


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondCandidatePlanIngestError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondCandidatePlanIngestError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondCandidatePlanIngestError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondCandidatePlanIngestError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"fresh-surface-policy metadata.artifact_type must be {POLICY_ARTIFACT_TYPE}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"fresh-surface-policy metadata.policy_version must be {POLICY_VERSION}"
        )
    if metadata.get("disallowed_eval_work_set_sha256") != OLD_EVAL_WORK_SET_SHA256:
        raise MLShadowScorerSecondCandidatePlanIngestError("fresh-surface-policy old-217 SHA mismatch")
    return metadata


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-audit-plan")
    if metadata.get("artifact_type") != GENERALIZATION_PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"generalization-audit-plan metadata.artifact_type must be {GENERALIZATION_PLAN_ARTIFACT_TYPE}"
        )
    if metadata.get("plan_version") != GENERALIZATION_PLAN_VERSION:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"generalization-audit-plan metadata.plan_version must be {GENERALIZATION_PLAN_VERSION}"
        )
    if payload.get("generalization_audit_plan_defined") is not True:
        raise MLShadowScorerSecondCandidatePlanIngestError("generalization audit plan must be defined")
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondCandidatePlanIngestError("runtime implementation must remain unauthorized")
    return metadata


def _validate_second_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="second-hybrid-candidate-plan")
    if metadata.get("artifact_type") != PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"second candidate plan metadata.artifact_type must be {PLAN_ARTIFACT_TYPE}"
        )
    if metadata.get("plan_version") != PLAN_VERSION:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"second candidate plan metadata.plan_version must be {PLAN_VERSION}"
        )
    if _get(payload, "candidate_selection.candidate_threshold_plausibly_met") is not True:
        raise MLShadowScorerSecondCandidatePlanIngestError("candidate_threshold_plausibly_met must be true")
    if _get(payload, "readiness_estimate.expected_next_stage") != "ingest_second_hybrid_candidate_plan_as_snapshot_v1":
        raise MLShadowScorerSecondCandidatePlanIngestError(
            "readiness_estimate.expected_next_stage must be ingest_second_hybrid_candidate_plan_as_snapshot_v1"
        )
    if payload.get("recommended_next_stage") != "ingest_second_hybrid_candidate_plan_as_snapshot_v1":
        raise MLShadowScorerSecondCandidatePlanIngestError(
            "recommended_next_stage must be ingest_second_hybrid_candidate_plan_as_snapshot_v1"
        )
    selected_total = _get(payload, "candidate_selection.selected_total")
    selected_candidates = _get(payload, "candidate_selection.selected_candidates")
    if selected_total != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"candidate_selection.selected_total must be {EXPECTED_SELECTED_TOTAL}"
        )
    if not isinstance(selected_candidates, list) or len(selected_candidates) != EXPECTED_SELECTED_TOTAL:
        raise MLShadowScorerSecondCandidatePlanIngestError("selected_candidates must contain 528 rows")
    readiness_sha = _get(payload, "readiness_estimate.planned_candidate_work_set_sha256")
    selection_sha = _get(payload, "candidate_selection.planned_candidate_work_set_sha256")
    if readiness_sha != EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256:
        raise MLShadowScorerSecondCandidatePlanIngestError("readiness_estimate planned candidate SHA mismatch")
    if selection_sha != EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256:
        raise MLShadowScorerSecondCandidatePlanIngestError("candidate_selection planned candidate SHA mismatch")
    eligible = _get(payload, "readiness_estimate.estimated_confirmatory_eligible_after_exclusions")
    if isinstance(eligible, bool) or not isinstance(eligible, int) or eligible < 100:
        raise MLShadowScorerSecondCandidatePlanIngestError("estimated confirmatory eligible count must be >= 100")
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondCandidatePlanIngestError("runtime implementation must remain unauthorized")
    if payload.get("online_shadow_execution_enabled") is not False:
        raise MLShadowScorerSecondCandidatePlanIngestError("online shadow execution must remain disabled")
    if payload.get("shadow_scoring_allowed") is not False or payload.get("production_default_allowed") is not False:
        raise MLShadowScorerSecondCandidatePlanIngestError("shadow/prod must remain blocked")
    return metadata


def _candidate_to_corpus_v2_row(row: Mapping[str, Any]) -> dict[str, Any]:
    openalex_id = row.get("openalex_id")
    if not isinstance(openalex_id, str) or not openalex_id.strip():
        canonical = row.get("canonical_openalex_work_id")
        openalex_id = f"https://openalex.org/{canonical}" if canonical else None
    return {
        "openalex_id": openalex_id,
        "doi": row.get("doi"),
        "title": row.get("title"),
        "year": row.get("year"),
        "citation_count": row.get("citation_count"),
        "source_display_name": row.get("source_display_name"),
        "source_slug": row.get("source_slug"),
        "bucket_id": row.get("bucket_id"),
        "inclusion_reason": row.get("inclusion_reason"),
        "matched_terms": row.get("matched_terms") or [],
        "exclusion_reason": row.get("exclusion_reason"),
        "type": row.get("type"),
        "language": row.get("language"),
        "abstract": row.get("abstract"),
        "publication_date": row.get("publication_date"),
        "updated_date": row.get("updated_date"),
        "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
        "old_217_overlap": row.get("old_217_overlap"),
        "first_validated_surface_overlap": row.get("first_validated_surface_overlap"),
        "underpowered_source_overlap": row.get("underpowered_source_overlap"),
        "underpowered_overlap_basis": row.get("underpowered_overlap_basis"),
        "confirmatory_metric_candidate_after_exclusions": row.get("confirmatory_metric_candidate_after_exclusions"),
        "confirmatory_metric_candidate": row.get("confirmatory_metric_candidate_after_exclusions"),
        "negative_or_borderline_candidate": row.get("negative_or_borderline_candidate"),
        "label_used_for_selection": row.get("label_used_for_selection"),
    }


def _corpus_v2_plan_from_second_plan(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _metadata(payload, name="second-hybrid-candidate-plan")
    contact = metadata.get("openalex_contact_provenance")
    if not isinstance(contact, Mapping):
        contact = {}
    selected_candidates = [
        _candidate_to_corpus_v2_row(row)
        for row in (_get(payload, "candidate_selection.selected_candidates") or [])
        if isinstance(row, Mapping)
    ]
    selected_total = int(_get(payload, "candidate_selection.selected_total") or len(selected_candidates))
    caveats = list(payload.get("caveats") or metadata.get("caveats") or [])
    caveats.append("Dry-run candidate plan: no Postgres/database writes occurred before this ingest command.")
    caveats.append("Second shadow-generalization ingest converts committed plan rows only; no live OpenAlex enrichment.")
    plan = {
        "generated_at": metadata.get("generated_at"),
        "contact_provided": bool(contact.get("contact_provided")),
        "contact_mode": contact.get("contact_mode") or "none",
        "api_key_provided": bool(contact.get("api_key_provided")),
        "auth_mode": contact.get("auth_mode") or "no_key",
        "policy_reference": {
            "name": "shadow-scorer-second-hybrid-candidate-plan",
            "policy_hash": str(metadata.get("plan_version") or PLAN_VERSION),
        },
        "target_min": int(_get(payload, "planning_context.target_min") or 1),
        "target_max": int(_get(payload, "planning_context.target_max") or max(selected_total, 1)),
        "selected_total": selected_total,
        "bucket_summaries": _get(payload, "bucket_summary.by_bucket") or [],
        "selected_candidates": selected_candidates,
        "caveats": caveats,
    }
    validate_candidate_plan(plan)
    return plan


def _load_second_plan_document(path: Path) -> _SecondPlanDocument:
    raw = path.read_bytes()
    sha = sha256_file(path)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise MLShadowScorerSecondCandidatePlanIngestError(f"second candidate plan is not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise MLShadowScorerSecondCandidatePlanIngestError("second candidate plan JSON must be an object")
    _validate_second_plan(payload)
    corpus_v2_payload = _corpus_v2_plan_from_second_plan(payload)
    return _SecondPlanDocument(path=path, sha256=sha, payload=payload, corpus_v2_payload=corpus_v2_payload)


def _snapshot_exists(conn: Any, snapshot_version: str) -> bool:
    row = conn.execute(
        "SELECT source_snapshot_version FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    return row is not None


def _affected_row_counts_from_summary(summary: Mapping[str, Any]) -> dict[str, int]:
    written_work_rows = int(summary.get("inserted_count") or 0) + int(summary.get("updated_count") or 0)
    return {
        "source_snapshot_versions": 1,
        "ingest_runs": 2,
        "raw_openalex_works": written_work_rows,
        "works_inserted_or_updated": written_work_rows,
    }


def _run_ingest_with_conn(conn: Any, *, plan_doc: _SecondPlanDocument, snapshot_version: str) -> dict[str, Any]:
    if _snapshot_exists(conn, snapshot_version):
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"snapshot_version already exists: {snapshot_version}; v1 has no idempotent re-run mode"
        )
    policy_name, policy_hash = _derive_policy_identity(plan_doc.corpus_v2_payload, plan_doc.sha256)
    snapshot = SourceSnapshotVersion(
        source_snapshot_version=snapshot_version,
        policy_name=policy_name,
        policy_hash=policy_hash,
        ingest_mode=DEFAULT_INGEST_MODE,
        created_at=datetime.now(UTC),
        note=(
            "Second shadow-generalization candidate plan ingest; eval_only=true; "
            "shadow_generalization_candidate_source=true; "
            f"second_hybrid_candidate_plan_sha256={plan_doc.sha256}"
        ),
    )
    corpus_doc = CandidatePlanDocument(path=plan_doc.path, sha256=plan_doc.sha256, payload=plan_doc.corpus_v2_payload)
    warnings = _metadata_warnings(list(plan_doc.corpus_v2_payload["selected_candidates"]))
    ingest_run = IngestRun.start(
        snapshot=snapshot,
        config={
            "ingest_mode": DEFAULT_INGEST_MODE,
            "second_shadow_generalization_ingest_intent": SECOND_SHADOW_GENERALIZATION_INGEST_INTENT,
            "eval_only": True,
            "shadow_generalization_candidate_source": True,
            "candidate_plan_path": str(plan_doc.path),
            "second_hybrid_candidate_plan_sha256": plan_doc.sha256,
            "planned_candidate_work_set_sha256": _get(
                plan_doc.payload,
                "readiness_estimate.planned_candidate_work_set_sha256",
            ),
            "selected_total": int(plan_doc.corpus_v2_payload["selected_total"]),
            "overlap_tags_preserved": [
                "old_217_overlap",
                "first_validated_surface_overlap",
                "underpowered_source_overlap",
                "confirmatory_metric_candidate_after_exclusions",
                "negative_or_borderline_candidate",
                "underpowered_overlap_basis",
            ],
            "openalex_network_calls": "not_run",
            "production_default_changed": False,
            "warnings": warnings,
        },
    )
    _register_snapshot_and_ingest_run(conn, snapshot, ingest_run)
    conn.commit()
    try:
        with conn.transaction():
            summary = _ingest_plan_candidates(
                conn,
                plan_doc=corpus_doc,
                snapshot=snapshot,
                ingest_run=ingest_run,
                warnings=warnings,
            )
            summary["snapshot_version"] = snapshot.source_snapshot_version
            summary["ingest_run_id"] = ingest_run.ingest_run_id
            summary["next_step"] = NEXT_STAGE
            summary["openalex_enrichment"] = "not_run"
            _update_ingest_run_final(
                conn,
                ingest_run.ingest_run_id,
                status="succeeded",
                counts=summary,
                error_message=None,
            )
        conn.commit()
    except Exception as exc:
        _mark_ingest_failed(conn, ingest_run.ingest_run_id, safe_exception_summary(exc))
        raise
    return summary


def _count_selected_rows(plan_payload: Mapping[str, Any], field: str) -> int:
    rows = _get(plan_payload, "candidate_selection.selected_candidates") or []
    return sum(1 for row in rows if isinstance(row, Mapping) and row.get(field) is True)


def _artifact_from_summary(
    *,
    plan_doc: _SecondPlanDocument,
    policy_metadata: Mapping[str, Any],
    generalization_plan_metadata: Mapping[str, Any],
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    snapshot_version: str,
    ingest_version: str,
    dry_run: bool,
    summary: Mapping[str, Any],
    affected_row_counts: Mapping[str, int],
) -> dict[str, Any]:
    selected_total = int(_get(plan_doc.payload, "candidate_selection.selected_total") or 0)
    inserted = int(summary.get("inserted_count") or 0)
    updated = int(summary.get("updated_count") or 0)
    skipped = int(summary.get("skipped_existing_count") or 0)
    failed = int(summary.get("failed_count") or 0)
    status = "dry_run_validated" if dry_run else "succeeded"
    writes_enabled = not dry_run
    underpowered_tagged = _count_selected_rows(plan_doc.payload, "underpowered_source_overlap")
    confirmatory_count = _count_selected_rows(plan_doc.payload, "confirmatory_metric_candidate_after_exclusions")
    missing_second_source = not (not dry_run and status == "succeeded")
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "ingest_version": ingest_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "dry_run": dry_run,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
        },
        "snapshot": {
            "source_snapshot_version": snapshot_version,
            "ingest_mode": DEFAULT_INGEST_MODE,
            "eval_only": True,
            "shadow_generalization_candidate_source": True,
            "second_shadow_generalization_ingest_intent": SECOND_SHADOW_GENERALIZATION_INGEST_INTENT,
            "production_default_changed": False,
        },
        "candidate_plan_summary": {
            "selected_total": selected_total,
            "planned_candidate_work_set_sha256": _get(
                plan_doc.payload,
                "readiness_estimate.planned_candidate_work_set_sha256",
            ),
            "estimated_confirmatory_eligible_after_exclusions": _get(
                plan_doc.payload,
                "readiness_estimate.estimated_confirmatory_eligible_after_exclusions",
            ),
            "estimated_overlap_with_old_217": _get(plan_doc.payload, "readiness_estimate.estimated_overlap_with_old_217"),
            "estimated_overlap_with_first_validated_surface": _get(
                plan_doc.payload,
                "readiness_estimate.estimated_overlap_with_first_validated_surface",
            ),
            "borderline_or_negative_selected_count": _get(
                plan_doc.payload,
                "bucket_summary.rollups.borderline_or_negative_candidate.selected_count",
            ),
            "full_underpowered_overlap_available": _get(
                plan_doc.payload,
                "readiness_estimate.full_underpowered_overlap_available",
            ),
            "underpowered_source_overlap_preview_count": _get(
                plan_doc.payload,
                "readiness_estimate.underpowered_source_overlap_preview_count",
            ),
            "fresh_surface_policy_version": policy_metadata.get("policy_version"),
            "generalization_plan_version": generalization_plan_metadata.get("plan_version"),
        },
        "overlap_summary": {
            "old_217_overlap_count_in_plan": _get(plan_doc.payload, "readiness_estimate.estimated_overlap_with_old_217"),
            "first_validated_surface_overlap_count_in_plan": _get(
                plan_doc.payload,
                "readiness_estimate.estimated_overlap_with_first_validated_surface",
            ),
            "underpowered_preview_overlap_count_in_plan": underpowered_tagged,
            "confirmatory_metric_candidate_after_exclusions_count_in_plan": confirmatory_count,
            "note": "Overlaps are ingested into the snapshot for audit traceability; confirmatory eligibility is enforced later.",
        },
        "ingest_result": {
            "status": status,
            "inserted_count": inserted,
            "updated_count": updated,
            "skipped_existing_count": skipped,
            "failed_count": failed,
            "snapshot_work_count": inserted + updated,
            "selected_total": selected_total,
            "planned_candidate_count": selected_total,
            "recommended_next_stage": NEXT_STAGE,
            "warnings": list(summary.get("warnings") or []),
        },
        "sql_write_report": {
            "writes_enabled": writes_enabled,
            "allowed_tables": list(ALLOWED_WRITE_TABLES) if writes_enabled else [],
            "affected_row_counts": dict(affected_row_counts) if writes_enabled else {},
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "embeddings_written": False,
            "production_tables_modified": False,
        },
        "blocked_actions": [
            "openalex_network_fetch",
            "embedding_generation",
            "ranking_run_creation",
            "paper_scores_write",
            "learned_probability_generation",
            "scorer_execution",
            "label_ingest",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "missing_second_fresh_candidate_source": missing_second_source,
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
        "recommended_next_stage": NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def build_ml_shadow_scorer_second_candidate_plan_ingest_payload(
    *,
    second_hybrid_candidate_plan_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    snapshot_version: str,
    database_url: str | None = None,
    ingest_version: str = INGEST_VERSION,
    dry_run: bool = False,
    repo_root: Path | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(second_hybrid_candidate_plan_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    resolved_snapshot = (snapshot_version or "").strip() or _default_snapshot_version()
    if not resolved_snapshot:
        raise MLShadowScorerSecondCandidatePlanIngestError("snapshot-version must not be blank")

    plan_doc = _load_second_plan_document(plan_path)
    audit_plan_payload = _load_json_object(audit_plan_path)
    policy_payload = _load_json_object(policy_path)
    generalization_plan_metadata = _validate_generalization_plan(audit_plan_payload)
    policy_metadata = _validate_policy(policy_payload)
    inputs = [
        _input_record("second_hybrid_candidate_plan", plan_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
    ]
    database_summary = assert_local_database_url(database_url) if database_url else {
        "database_target_redacted": None,
        "local_database_url_confirmed": None,
    }

    if dry_run:
        summary = {
            "inserted_count": 0,
            "updated_count": 0,
            "skipped_existing_count": 0,
            "failed_count": 0,
            "warnings": _metadata_warnings(list(plan_doc.corpus_v2_payload["selected_candidates"])),
        }
        return _artifact_from_summary(
            plan_doc=plan_doc,
            policy_metadata=policy_metadata,
            generalization_plan_metadata=generalization_plan_metadata,
            inputs=inputs,
            database_summary=database_summary,
            snapshot_version=resolved_snapshot,
            ingest_version=ingest_version,
            dry_run=True,
            summary=summary,
            affected_row_counts={},
        )

    if conn is None:
        raise MLShadowScorerSecondCandidatePlanIngestError("internal: conn is required when dry_run is false")
    try:
        summary = _run_ingest_with_conn(conn, plan_doc=plan_doc, snapshot_version=resolved_snapshot)
    except MLShadowScorerSecondCandidatePlanIngestError:
        raise
    except Exception as exc:
        raise MLShadowScorerSecondCandidatePlanIngestError(
            f"second candidate plan ingest failed: {safe_exception_summary(exc)}",
            code=1,
        ) from exc
    return _artifact_from_summary(
        plan_doc=plan_doc,
        policy_metadata=policy_metadata,
        generalization_plan_metadata=generalization_plan_metadata,
        inputs=inputs,
        database_summary=database_summary,
        snapshot_version=resolved_snapshot,
        ingest_version=ingest_version,
        dry_run=False,
        summary=summary,
        affected_row_counts=_affected_row_counts_from_summary(summary),
    )


def markdown_from_ml_shadow_scorer_second_candidate_plan_ingest(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    snapshot = payload["snapshot"]
    plan = payload["candidate_plan_summary"]
    overlap = payload["overlap_summary"]
    result = payload["ingest_result"]
    write_report = payload["sql_write_report"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# Second Shadow-Generalization Candidate Plan Ingest ({metadata['ingest_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records the controlled ingest of the committed second hybrid candidate plan into a local eval-only source snapshot. It does not hydrate from OpenAlex, create embeddings, rank, score, import labels, or authorize shadow/production.",
        "",
        f"- **Dry run:** {metadata['dry_run']}",
        f"- **Status:** `{result['status']}`",
        f"- **Snapshot version:** `{snapshot['source_snapshot_version']}`",
        f"- **Selected total:** {plan['selected_total']}",
        f"- **Snapshot work count:** {result['snapshot_work_count']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        "",
        "## Candidate Plan Summary",
        "",
        f"- Planned candidate work-set SHA: `{plan['planned_candidate_work_set_sha256']}`",
        f"- Estimated confirmatory eligible after exclusions: {plan['estimated_confirmatory_eligible_after_exclusions']}",
        f"- Estimated old-217 overlap: {plan['estimated_overlap_with_old_217']}",
        f"- Estimated first-surface overlap: {plan['estimated_overlap_with_first_validated_surface']}",
        f"- Borderline/negative selected: {plan['borderline_or_negative_selected_count']}",
        f"- Full underpowered overlap available: {plan['full_underpowered_overlap_available']}",
        f"- Underpowered preview count: {plan['underpowered_source_overlap_preview_count']}",
        "",
        "## Overlap Tags Preserved",
        "",
        f"- Old-217 overlap rows in plan: {overlap['old_217_overlap_count_in_plan']}",
        f"- First validated surface overlap rows in plan: {overlap['first_validated_surface_overlap_count_in_plan']}",
        f"- Underpowered preview-tagged rows in plan: {overlap['underpowered_preview_overlap_count_in_plan']}",
        f"- Confirmatory-after-exclusions rows in plan: {overlap['confirmatory_metric_candidate_after_exclusions_count_in_plan']}",
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
        "## Counts",
        "",
        f"- Inserted: {result['inserted_count']}",
        f"- Updated: {result['updated_count']}",
        f"- Skipped existing/duplicates: {result['skipped_existing_count']}",
        f"- Failed: {result['failed_count']}",
        "",
        "## Remaining Blockers",
        "",
    ]
    for key, value in blockers.items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Not Hydration / Not Ranking / Not Embeddings / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_shadow_scorer_second_candidate_plan_ingest(
    *,
    second_hybrid_candidate_plan_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    snapshot_version: str | None = None,
    database_url: str | None = None,
    ingest_version: str = INGEST_VERSION,
    dry_run: bool = False,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    resolved_snapshot = (snapshot_version or "").strip() or _default_snapshot_version()
    dsn = database_url or _database_url_from_env()
    assert_local_database_url(dsn)
    if dry_run:
        payload = build_ml_shadow_scorer_second_candidate_plan_ingest_payload(
            second_hybrid_candidate_plan_path=second_hybrid_candidate_plan_path,
            generalization_audit_plan_path=generalization_audit_plan_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            snapshot_version=resolved_snapshot,
            database_url=dsn,
            ingest_version=ingest_version,
            dry_run=True,
            repo_root=repo_root,
        )
    else:
        with psycopg.connect(dsn, autocommit=False) as conn:
            payload = build_ml_shadow_scorer_second_candidate_plan_ingest_payload(
                second_hybrid_candidate_plan_path=second_hybrid_candidate_plan_path,
                generalization_audit_plan_path=generalization_audit_plan_path,
                fresh_surface_policy_path=fresh_surface_policy_path,
                snapshot_version=resolved_snapshot,
                database_url=dsn,
                ingest_version=ingest_version,
                dry_run=False,
                repo_root=repo_root,
                conn=conn,
            )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_candidate_plan_ingest(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "INGEST_VERSION",
    "MLShadowScorerSecondCandidatePlanIngestError",
    "assert_local_database_url",
    "build_ml_shadow_scorer_second_candidate_plan_ingest_payload",
    "markdown_from_ml_shadow_scorer_second_candidate_plan_ingest",
    "write_ml_shadow_scorer_second_candidate_plan_ingest",
]
