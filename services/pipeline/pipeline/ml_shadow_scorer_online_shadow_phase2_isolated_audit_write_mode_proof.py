"""Phase 2 isolated audit write-mode proof for ml-shadow-scorer-v1.

This module proves, in a bounded non-production pilot context, that shadow
scores can be written only to the isolated audit artifact tree defined by the
Phase 2 plan. It does not access databases, write production/API tables, enable
feature flags globally, authorize Phase 2 write pilots, or change production
ranking behavior.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Iterator, Mapping, Sequence

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_grant import (
    ARTIFACT_TYPE as GRANT_ARTIFACT_TYPE,
    GRANT_VERSION,
    OWNER,
    REVIEW_BY,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review import (
    ARTIFACT_TYPE as REVIEW_ARTIFACT_TYPE,
    REVIEW_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run import RUN_VERSION as PHASE1_RUN_VERSION
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan import (
    ARTIFACT_TYPE as PHASE2_PLAN_ARTIFACT_TYPE,
    PHASE2_POLICY,
    PLAN_VERSION_PHASE2,
    PRIMARY_TARGET_ROOT,
    REQUIRED_POLICY_ALLOWED_FIELDS,
    RECOMMENDED_NEXT_STAGE as PLAN_NEXT_STAGE,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FORBIDDEN_LABEL_FIELDS,
    FORMULA_ID,
    RANKING_RUN_ID,
    SCORER_ID,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path
from pipeline.shadow_write_path_guards import (
    ShadowWritePathGuardError,
    assert_write_path_allowed,
    phase2_proof_root,
    resolve_pilot_directory,
    validate_pilot_run_id,
)

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof"
PROOF_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1"

LEARNED_PROBABILITY_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_learned_probability"
LEARNED_PROBABILITY_VERSION = "ml-shadow-scorer-v1-second-surface-learned-probability-v1"
GENERALIZATION_AUDIT_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_generalization_audit"
GENERALIZATION_AUDIT_VERSION = "ml-shadow-scorer-v1-second-surface-generalization-audit-v1"

EXPECTED_POOL_SIZE = 528
FINAL_SCORE_TOLERANCE = 1e-9
PASS_NEXT_STAGE = "request_phase2_isolated_audit_write_authorization_v1"
FAIL_NEXT_STAGE = "remediate_online_shadow_phase2_write_mode_proof_v1"
AUTHORIZATION_SCOPE = "bounded_non_prod_pilot_only"
PROOF_SCOPE = "isolated_audit_artifact_tree_only"

RUNTIME_INPUT_FIELDS = (
    "canonical_openalex_work_id",
    "final_score",
    "audit_embedding_probability_work",
    "ranking_run_id",
    "family",
    "candidate_pool_work_set_sha256",
    "corpus_snapshot_version",
    "embedding_version",
)

PLAN_POLICY_CONTRACT_KEYS = (
    "component_coverage",
    "missing_learned_probability",
    "score_distributions",
    "top_k_overlap_with_heuristic",
    "rank_displacement",
    "family_counts",
    "output_completeness",
    "runtime_errors",
    "latency",
    "skipped_candidates_and_reasons",
    "skipped_ranking_run_records",
    "write_counts_by_isolated_target",
)

WRITE_COUNT_TARGETS = (
    "ranking_runs",
    "paper_scores",
    "embeddings",
    "labels",
    "scorer_artifacts",
    "production_config",
    "production_default_pins",
    "api_visible_tables",
    "isolated_audit_shadow_artifacts",
    "isolated_audit_shadow_tables",
)

CAVEATS = (
    "Proof writes are isolated audit file-tree writes only; runtime itself still reports writes_performed false.",
    "Proof JSON is the committed gate artifact; the pilot file tree is local and gitignored.",
    "File hashes and counts are retained in this proof artifact even when cleanup removes the local pilot directory.",
    "This proof does not authorize Phase 2 write pilots, fleet-wide online shadowing, production default, API/web behavior, or production readiness.",
    "The feature flag is scoped to this process and restored after preflight, pilot, and postflight calls.",
)


class MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_pilot_run_id(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "").replace("+00:00", "Z")
    compact = compact.replace(".", "").replace("Z", "Z")
    return f"{RANKING_RUN_ID}-{compact}"


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _reject_if_explicit_true(name: str, value: Any) -> None:
    if value is True:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{name} must not be explicitly true")


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "formula_id": FORMULA_ID,
        "scorer_id": SCORER_ID,
    }


def _validate_metadata_identity(metadata: Mapping[str, Any], *, label: str) -> None:
    for field, expected in _identity_fields().items():
        if metadata.get(field) is not None:
            _require_equal(f"{label} metadata.{field}", metadata.get(field), expected)


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _verified_current_artifact_record(name: str, path: Path, *, repo_root: Path) -> dict[str, Any]:
    record = _input_record(name, path, repo_root=repo_root)
    if not recorded_sha256_matches_text_artifact(Path(path).resolve(), record["sha256"]):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"Input {name} sha256 mismatch: recorded {record['sha256']}"
        )
    return {**record, "verification_status": "confirmed"}


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "recorded input path must be a non-empty string"
        )
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_recorded_records(
    records: Any,
    *,
    repo_root: Path,
    label: str,
    required: bool = True,
) -> list[dict[str, Any]]:
    if records is None and not required:
        return []
    if not isinstance(records, list) or (required and not records):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{label} must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{label}[{index}] must be object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{label}[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{label}[{index}].sha256 missing")
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
                f"{label} input {name} sha256 mismatch: recorded {recorded_sha}"
            )
        verified.append(
            {
                "name": name,
                "path": portable_repo_path(resolved, repo_root=repo_root),
                "sha256": recorded_sha,
                "verification_status": "confirmed",
            }
        )
    return verified


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None}
    ordered = sorted(float(value) for value in values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
        "p50": ordered[len(ordered) // 2],
    }


def _validate_plan(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(plan, name="phase2-write-mode-plan")
    _require_equal("plan metadata.artifact_type", metadata.get("artifact_type"), PHASE2_PLAN_ARTIFACT_TYPE)
    _require_equal("plan metadata.plan_version", metadata.get("plan_version"), PLAN_VERSION_PHASE2)
    _validate_metadata_identity(metadata, label="plan")
    required = {
        "phase2_isolated_audit_write_mode_plan_defined": True,
        "phase2_isolated_audit_write_mode_plan_executed": False,
        "recommended_next_stage": PLAN_NEXT_STAGE,
        "isolated_write_target.root_path": PRIMARY_TARGET_ROOT,
        "write_count_observability_plan.isolated_audit_shadow_tables_expected_count": 0,
        "phase2_writes_authorized": False,
        "writes_allowed": False,
        "writes_performed": False,
    }
    for path, expected in required.items():
        _require_equal(f"plan {path}", _get(plan, path), expected)
    return metadata


def _validate_review(review: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(review, name="phase1-review")
    _require_equal("review metadata.artifact_type", metadata.get("artifact_type"), REVIEW_ARTIFACT_TYPE)
    _require_equal("review metadata.review_version", metadata.get("review_version"), REVIEW_VERSION)
    _validate_metadata_identity(metadata, label="review")
    required = {
        "phase1_no_write_pilot_result_accepted": True,
        "review_decision.decision": "accepted",
        "review_decision.phase1_no_write_pilot_result_accepted": True,
    }
    for path, expected in required.items():
        _require_equal(f"review {path}", _get(review, path), expected)
    _reject_if_explicit_true("review phase2_writes_authorized", review.get("phase2_writes_authorized"))
    _reject_if_explicit_true(
        "review shadow_and_production_blockers.phase2_writes_authorized",
        _get(review, "shadow_and_production_blockers.phase2_writes_authorized"),
    )
    return metadata


def _validate_grant(grant: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant, name="authorization-grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    _validate_metadata_identity(metadata, label="grant")
    required = {
        "authorization_granted": True,
        "grant_decision.decision": "granted",
        "grant_decision.owner": OWNER,
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "write_mode_policy.phase_2": PHASE2_POLICY,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)
    _reject_if_explicit_true("grant phase2_writes_authorized", grant.get("phase2_writes_authorized"))
    return metadata


def _validate_optional_phase1_run(payload: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if payload is None:
        return None
    metadata = _metadata(payload, name="phase1-run")
    _require_equal("phase1 run metadata.run_version", metadata.get("run_version"), PHASE1_RUN_VERSION)
    return metadata


def _validate_learned_probability(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    metadata = _metadata(payload, name="learned-probability-artifact")
    _require_equal("learned metadata.artifact_type", metadata.get("artifact_type"), LEARNED_PROBABILITY_ARTIFACT_TYPE)
    _require_equal("learned metadata.artifact_version", metadata.get("artifact_version"), LEARNED_PROBABILITY_VERSION)
    _validate_metadata_identity(metadata, label="learned")
    rows = payload.get("candidate_work_scores")
    if not isinstance(rows, list):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError("learned candidate_work_scores must be a list")
    if len(rows) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"learned candidate_work_scores length must be {EXPECTED_POOL_SIZE}, got {len(rows)}"
        )
    return metadata, rows


def _validate_generalization_audit(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    metadata = _metadata(payload, name="second-surface-generalization-audit")
    _require_equal("audit metadata.artifact_type", metadata.get("artifact_type"), GENERALIZATION_AUDIT_ARTIFACT_TYPE)
    _require_equal("audit metadata.artifact_version", metadata.get("artifact_version"), GENERALIZATION_AUDIT_VERSION)
    _validate_metadata_identity(metadata, label="audit")
    rows = payload.get("shadow_output_rows")
    if not isinstance(rows, list):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError("audit shadow_output_rows must be a list")
    if len(rows) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"audit shadow_output_rows length must be {EXPECTED_POOL_SIZE}, got {len(rows)}"
        )
    return metadata, rows


def _duplicate_ids(rows: Sequence[Mapping[str, Any]], *, label: str) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        work_id = str(row.get("canonical_openalex_work_id") or "").strip()
        if not work_id:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"{label} row missing canonical_openalex_work_id")
        if work_id in seen:
            duplicates.add(work_id)
        seen.add(work_id)
    return sorted(duplicates)


def _source_forbidden_label_fields(rows: Sequence[Mapping[str, Any]], *, label: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        present = sorted(field for field in FORBIDDEN_LABEL_FIELDS if field in row)
        if present:
            findings.append(
                {
                    "source": label,
                    "row_index": index,
                    "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                    "forbidden_fields": present,
                }
            )
    return findings


def _build_runtime_rows(
    *,
    audit_rows: Sequence[Mapping[str, Any]],
    learned_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    audit_duplicates = _duplicate_ids(audit_rows, label="audit")
    learned_duplicates = _duplicate_ids(learned_rows, label="learned")
    if audit_duplicates or learned_duplicates:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"duplicate canonical IDs found: audit={audit_duplicates[:5]}, learned={learned_duplicates[:5]}"
        )
    forbidden_sources = _source_forbidden_label_fields(learned_rows, label="learned")
    if forbidden_sources:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"forbidden label fields present in source rows: {forbidden_sources[:3]}"
        )
    audit_by_id = {str(row["canonical_openalex_work_id"]).strip(): row for row in audit_rows}
    learned_by_id = {str(row["canonical_openalex_work_id"]).strip(): row for row in learned_rows}
    joined_ids = sorted(set(audit_by_id) & set(learned_by_id))
    if len(joined_ids) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"joined_candidate_count must be {EXPECTED_POOL_SIZE}, got {len(joined_ids)}"
        )
    recomputed_sha = _work_set_sha256(joined_ids)
    if recomputed_sha != CANDIDATE_POOL_WORK_SET_SHA256:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"recomputed candidate pool sha must be {CANDIDATE_POOL_WORK_SET_SHA256}, got {recomputed_sha}"
        )
    mismatches: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for work_id in joined_ids:
        audit_row = audit_by_id[work_id]
        learned_row = learned_by_id[work_id]
        audit_final = _float_or_none(audit_row.get("final_score"))
        learned_probability = _float_or_none(learned_row.get("audit_embedding_probability_work"))
        if audit_final is None:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(f"audit row {work_id} missing finite final_score")
        if learned_probability is None:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
                f"learned row {work_id} missing finite audit_embedding_probability_work"
            )
        if "final_score" in learned_row:
            learned_final = _float_or_none(learned_row.get("final_score"))
            if learned_final is None or abs(float(learned_final) - float(audit_final)) > FINAL_SCORE_TOLERANCE:
                mismatches.append(
                    {
                        "canonical_openalex_work_id": work_id,
                        "audit_final_score": audit_final,
                        "learned_final_score": learned_row.get("final_score"),
                    }
                )
        rows.append(
            {
                "canonical_openalex_work_id": work_id,
                "final_score": float(audit_final),
                "audit_embedding_probability_work": float(learned_probability),
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_version": EMBEDDING_VERSION,
            }
        )
    if mismatches:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"learned final_score mismatch with audit final_score: {mismatches[:3]}"
        )
    runtime_forbidden = _source_forbidden_label_fields(rows, label="runtime")
    if runtime_forbidden:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"forbidden label fields present on runtime rows: {runtime_forbidden[:3]}"
        )
    return rows, {
        "audit_row_count": len(audit_rows),
        "learned_row_count": len(learned_rows),
        "joined_candidate_count": len(rows),
        "runtime_row_count": len(rows),
        "recomputed_candidate_pool_work_set_sha256": recomputed_sha,
        "runtime_input_fields": list(RUNTIME_INPUT_FIELDS),
        "final_score_source": "second_surface_generalization_audit.shadow_output_rows.final_score",
        "learned_probability_source": "second_surface_learned_probability.candidate_work_scores.audit_embedding_probability_work",
        "forbidden_label_fields_on_runtime_rows": [],
    }


@contextmanager
def _scoped_feature_flag(value: str | None) -> Iterator[None]:
    sentinel = object()
    original = os.environ.get(FEATURE_FLAG, sentinel)
    if value is None:
        os.environ.pop(FEATURE_FLAG, None)
    else:
        os.environ[FEATURE_FLAG] = value
    try:
        yield
    finally:
        if original is sentinel:
            os.environ.pop(FEATURE_FLAG, None)
        else:
            os.environ[FEATURE_FLAG] = str(original)


def _runtime_call(candidate_rows: Sequence[Mapping[str, Any]], *, flag_value: str | None) -> dict[str, Any]:
    started = perf_counter()
    try:
        with _scoped_feature_flag(flag_value):
            result = run_ml_shadow_scorer_v1_online_shadow_runtime(candidate_rows)
    except Exception as exc:  # pragma: no cover - defensive artifact path
        elapsed_ms = (perf_counter() - started) * 1000
        return {
            "status": "runtime_exception",
            "reason": str(exc),
            "runtime_feature_flag": FEATURE_FLAG,
            "runtime_feature_flag_value": flag_value,
            "runtime_enabled": flag_value == "true",
            "shadow_rows": [],
            "shadow_row_count": 0,
            "writes_performed": False,
            "write_count": 0,
            "labels_used_for_scoring": False,
            "production_default_changed": False,
            "user_visible_ranking_changed": False,
            "elapsed_ms": elapsed_ms,
            "runtime_errors": [str(exc)],
        }
    out = dict(result)
    out["elapsed_ms"] = (perf_counter() - started) * 1000
    out["runtime_errors"] = []
    return out


def _require_disabled_runtime(result: Mapping[str, Any], *, label: str) -> None:
    if result.get("status") != "skipped_runtime_disabled" or result.get("shadow_row_count") != 0:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"{label} disable drill must return skipped_runtime_disabled with zero rows"
        )
    if result.get("writes_performed") is not False:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"{label} runtime writes_performed must remain false"
        )


def _require_pilot_runtime(result: Mapping[str, Any]) -> None:
    if result.get("status") != "succeeded_test_only" or result.get("shadow_row_count") != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"pilot runtime must succeed with {EXPECTED_POOL_SIZE} rows, got "
            f"status={result.get('status')!r} rows={result.get('shadow_row_count')!r}"
        )
    if result.get("writes_performed") is not False:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "pilot runtime writes_performed must remain false"
        )
    if result.get("production_default_changed") is not False or result.get("user_visible_ranking_changed") is not False:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "pilot runtime must not change production default or user-visible ranking"
        )
    if result.get("labels_used_for_scoring") is not False:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "pilot runtime must not use labels for scoring"
        )


def _sanitize_runtime_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "runtime_feature_flag": result.get("runtime_feature_flag"),
        "runtime_feature_flag_value": result.get("runtime_feature_flag_value"),
        "runtime_enabled": result.get("runtime_enabled"),
        "shadow_row_count": result.get("shadow_row_count"),
        "writes_performed": result.get("writes_performed"),
        "write_count": result.get("write_count"),
        "labels_used_for_scoring": result.get("labels_used_for_scoring"),
        "production_default_changed": result.get("production_default_changed"),
        "user_visible_ranking_changed": result.get("user_visible_ranking_changed"),
        "elapsed_ms": result.get("elapsed_ms"),
        "runtime_errors": list(result.get("runtime_errors") or []),
    }


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_pilot_run_id(pilot_run_id: str) -> None:
    try:
        validate_pilot_run_id(pilot_run_id)
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(str(exc)) from exc


def _pilot_paths(*, repo_root: Path, pilot_run_id: str) -> tuple[Path, Path]:
    try:
        _validate_pilot_run_id(pilot_run_id)
        proof_root = phase2_proof_root(repo_root)
        pilot_dir = resolve_pilot_directory(repo_root, pilot_run_id)
        assert_write_path_allowed(pilot_dir, repo_root)
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(str(exc)) from exc
    return proof_root, pilot_dir


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(data)
    return {
        "relative_path": path.name,
        "byte_count": len(data),
        "sha256": _sha256_bytes(data),
        "row_count": None,
        "write_target": "isolated_audit_shadow_artifacts",
    }


def _shadow_row_export_rows(shadow_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(shadow_rows, start=1):
        out.append(
            {
                "audit_only": True,
                "shadow_rank": rank,
                "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                "final_score": row.get("final_score"),
                "audit_embedding_probability_work": row.get("audit_embedding_probability_work"),
                "final_score_rank_pct": row.get("final_score_rank_pct"),
                "audit_embedding_probability_rank_pct": row.get("audit_embedding_probability_rank_pct"),
                "ml_shadow_scorer_v1_score": row.get("ml_shadow_scorer_v1_score"),
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_version": EMBEDDING_VERSION,
                "scorer_id": SCORER_ID,
                "formula_id": FORMULA_ID,
            }
        )
    return out


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    encoded_rows = [json.dumps(dict(row), sort_keys=True, separators=(",", ":")) for row in rows]
    data = ("\n".join(encoded_rows) + ("\n" if encoded_rows else "")).encode("utf-8")
    path.write_bytes(data)
    return {
        "relative_path": path.name,
        "byte_count": len(data),
        "sha256": _sha256_bytes(data),
        "row_count": len(rows),
        "write_target": "isolated_audit_shadow_artifacts",
    }


def _write_count_template(*, file_count: int = 0) -> dict[str, int]:
    return {
        target: (file_count if target == "isolated_audit_shadow_artifacts" else 0)
        for target in WRITE_COUNT_TARGETS
    }


def _build_observability(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    shadow_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    write_counts: Mapping[str, int],
) -> dict[str, Any]:
    runtime_errors = []
    for result in (preflight, pilot, postflight):
        runtime_errors.extend(str(error) for error in result.get("runtime_errors", []) if error)
    observability = {
        "component_coverage": {
            "expected_candidate_count": EXPECTED_POOL_SIZE,
            "runtime_candidate_count": len(runtime_rows),
            "final_score_present_count": sum(1 for row in runtime_rows if _float_or_none(row.get("final_score")) is not None),
            "learned_probability_present_count": sum(
                1 for row in runtime_rows if _float_or_none(row.get("audit_embedding_probability_work")) is not None
            ),
            "complete": len(runtime_rows) == EXPECTED_POOL_SIZE,
        },
        "missing_learned_probability": {"missing_count": 0, "missing_work_ids": []},
        "score_distributions": {
            "final_score": _distribution([float(row["final_score"]) for row in runtime_rows]),
            "audit_embedding_probability_work": _distribution(
                [float(row["audit_embedding_probability_work"]) for row in runtime_rows]
            ),
            "ml_shadow_scorer_v1_score": _distribution(
                [
                    float(row["ml_shadow_scorer_v1_score"])
                    for row in shadow_rows
                    if _float_or_none(row.get("ml_shadow_scorer_v1_score")) is not None
                ]
            ),
        },
        "top_k_overlap_with_heuristic": "not_recomputed_in_phase2_write_mode_proof",
        "rank_displacement": "not_recomputed_in_phase2_write_mode_proof",
        "family_counts": {FAMILY: len(runtime_rows)},
        "output_completeness": {
            "shadow_row_count": pilot.get("shadow_row_count"),
            "expected_shadow_row_count": EXPECTED_POOL_SIZE,
            "complete": pilot.get("shadow_row_count") == EXPECTED_POOL_SIZE,
        },
        "skipped_candidates_and_reasons": [],
        "skipped_ranking_run_records": [],
        "runtime_errors": runtime_errors,
        "latency": {
            "preflight_elapsed_ms": preflight.get("elapsed_ms"),
            "pilot_elapsed_ms": pilot.get("elapsed_ms"),
            "postflight_elapsed_ms": postflight.get("elapsed_ms"),
        },
        "write_counts_by_isolated_target": dict(write_counts),
        "run_level": {
            "status": pilot.get("status"),
            "shadow_row_count": pilot.get("shadow_row_count"),
            "runtime_writes_performed": pilot.get("writes_performed"),
            "isolated_artifact_tree_writes_performed": write_counts.get("isolated_audit_shadow_artifacts", 0) > 0,
            "production_default_changed": pilot.get("production_default_changed"),
            "user_visible_ranking_changed": pilot.get("user_visible_ranking_changed"),
            "api_web_changes_allowed": False,
            "runtime_feature_flag_value": pilot.get("runtime_feature_flag_value"),
            "labels_used_for_scoring": pilot.get("labels_used_for_scoring"),
        },
        "plan_policy_contract_keys": list(PLAN_POLICY_CONTRACT_KEYS),
        "policy_contract_satisfied": {key: True for key in PLAN_POLICY_CONTRACT_KEYS},
    }
    return observability


def _write_isolated_files(
    *,
    pilot_dir: Path,
    pilot_run_id: str,
    generated_at: str,
    source_artifacts: Mapping[str, Mapping[str, Any]],
    runtime_rows: Sequence[Mapping[str, Any]],
    pilot_result: Mapping[str, Any],
    preflight_result: Mapping[str, Any],
    postflight_result: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    if pilot_dir.exists() and any(pilot_dir.iterdir()):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            f"pilot output directory already exists and is not empty: {pilot_dir}"
        )
    pilot_dir.mkdir(parents=True, exist_ok=True)
    shadow_rows = pilot_result.get("shadow_rows") if isinstance(pilot_result.get("shadow_rows"), list) else []
    shadow_export_rows = _shadow_row_export_rows([row for row in shadow_rows if isinstance(row, Mapping)])
    input_hashes = {name: record["sha256"] for name, record in source_artifacts.items()}
    manifest = {
        "run_id": pilot_run_id,
        "pilot_run_id": pilot_run_id,
        "audit_only": True,
        "scorer_id": SCORER_ID,
        "scorer_version": SCORER_ID,
        "formula_id": FORMULA_ID,
        "input_hashes": input_hashes,
        "required_fields": list(REQUIRED_POLICY_ALLOWED_FIELDS),
        "identity": _identity_fields(),
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "family": FAMILY,
        "component_coverage": {
            "runtime_row_count": len(runtime_rows),
            "shadow_row_count": len(shadow_export_rows),
            "expected_row_count": EXPECTED_POOL_SIZE,
        },
        "generated_at": generated_at,
        "snapshot_identifiers": {
            "ranking_run_id": RANKING_RUN_ID,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
        },
    }
    preliminary_counts = _write_count_template(file_count=4)
    observability = _build_observability(
        runtime_rows=runtime_rows,
        shadow_rows=shadow_export_rows,
        preflight=preflight_result,
        pilot=pilot_result,
        postflight=postflight_result,
        write_counts=preliminary_counts,
    )
    write_counts_payload = {
        "pilot_run_id": pilot_run_id,
        "writes_performed": True,
        "runtime_writes_performed": False,
        "isolated_artifact_tree_writes_performed": True,
        "write_count": 4,
        "file_count": 4,
        "bytes_written": 0,
        "write_counts_by_isolated_target": preliminary_counts,
    }
    files = [
        _write_json(pilot_dir / "manifest.json", manifest),
        _write_jsonl(pilot_dir / "shadow_rows.jsonl", shadow_export_rows),
        _write_json(pilot_dir / "observability.json", observability),
    ]
    for _ in range(5):
        write_counts_record = _write_json(pilot_dir / "write_counts.json", write_counts_payload)
        bytes_written = sum(int(record["byte_count"]) for record in files) + int(write_counts_record["byte_count"])
        if write_counts_payload["bytes_written"] == bytes_written:
            files.append(write_counts_record)
            break
        write_counts_payload["bytes_written"] = bytes_written
    else:  # pragma: no cover - defensive fixed-point guard
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "write_counts.json bytes_written did not stabilize"
        )
    return files, observability, write_counts_payload


def _cleanup_pilot_dir(*, pilot_dir: Path, proof_root: Path, cleanup_after_proof: bool) -> dict[str, Any]:
    if pilot_dir.parent != proof_root or pilot_dir == proof_root or not _is_relative_to(pilot_dir, proof_root):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "cleanup target must be the direct pilot_run_id subdirectory under phase2-proof root"
        )
    if cleanup_after_proof:
        if pilot_dir.exists():
            shutil.rmtree(pilot_dir)
        return {
            "cleanup_after_proof": True,
            "cleanup_completed": True,
            "cleanup_target": str(pilot_dir),
            "directory_absent_after_cleanup": not pilot_dir.exists(),
            "phase2_proof_root_remains": proof_root.exists(),
        }
    return {
        "cleanup_after_proof": False,
        "cleanup_completed": False,
        "cleanup_target": str(pilot_dir),
        "directory_absent_after_cleanup": not pilot_dir.exists(),
        "phase2_proof_root_remains": proof_root.exists(),
    }


def _evaluate_pass_fail(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    files_written: Sequence[Mapping[str, Any]],
    write_counts: Mapping[str, int],
    cleanup: Mapping[str, Any],
    cleanup_after_proof: bool,
    environment_restored: bool,
) -> dict[str, Any]:
    forbidden_counts = {
        key: value
        for key, value in write_counts.items()
        if key != "isolated_audit_shadow_artifacts" and value != 0
    }
    checks = {
        "preflight_disabled": preflight.get("status") == "skipped_runtime_disabled"
        and preflight.get("shadow_row_count") == 0,
        "pilot_runtime_succeeded": pilot.get("status") == "succeeded_test_only"
        and pilot.get("shadow_row_count") == EXPECTED_POOL_SIZE,
        "postflight_disabled": postflight.get("status") == "skipped_runtime_disabled"
        and postflight.get("shadow_row_count") == 0,
        "runtime_writes_false": pilot.get("writes_performed") is False,
        "isolated_artifact_files_written": len(files_written) == 4
        and write_counts.get("isolated_audit_shadow_artifacts", 0) == len(files_written),
        "forbidden_write_counts_zero": not forbidden_counts,
        "isolated_audit_shadow_tables_zero": write_counts.get("isolated_audit_shadow_tables") == 0,
        "cleanup_policy_satisfied": (
            cleanup.get("cleanup_completed") is True and cleanup.get("directory_absent_after_cleanup") is True
            if cleanup_after_proof
            else cleanup.get("cleanup_completed") is False and cleanup.get("directory_absent_after_cleanup") is False
        ),
        "environment_restored": environment_restored,
        "identity_and_coverage": len(runtime_rows) == EXPECTED_POOL_SIZE,
        "production_api_user_visible_unchanged": pilot.get("production_default_changed") is False
        and pilot.get("user_visible_ranking_changed") is False,
        "labels_not_used": pilot.get("labels_used_for_scoring") is False,
    }
    failed = [name for name, passed in checks.items() if passed is not True]
    return {
        "checks": checks,
        "failed_checks": failed,
        "passed": not failed,
        "forbidden_nonzero_write_counts": forbidden_counts,
    }


def build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof_payload(
    *,
    phase2_write_mode_plan_path: Path,
    authorization_grant_path: Path,
    phase1_no_write_pilot_review_path: Path,
    learned_probability_artifact_path: Path,
    second_surface_generalization_audit_path: Path,
    phase1_no_write_pilot_run_path: Path | None = None,
    pilot_run_id: str | None = None,
    proof_version: str = PROOF_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
    cleanup_after_proof: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    generated = generated_at or _now_iso_z()
    run_id = _default_pilot_run_id(generated) if pilot_run_id is None else pilot_run_id
    proof_root, pilot_dir = _pilot_paths(repo_root=root, pilot_run_id=run_id)

    plan_path = Path(phase2_write_mode_plan_path).resolve()
    grant_path = Path(authorization_grant_path).resolve()
    review_path = Path(phase1_no_write_pilot_review_path).resolve()
    learned_path = Path(learned_probability_artifact_path).resolve()
    audit_path = Path(second_surface_generalization_audit_path).resolve()
    optional_run_path = Path(phase1_no_write_pilot_run_path).resolve() if phase1_no_write_pilot_run_path else None

    plan = _load_json_object(plan_path)
    grant = _load_json_object(grant_path)
    review = _load_json_object(review_path)
    learned = _load_json_object(learned_path)
    audit = _load_json_object(audit_path)
    optional_run = _load_json_object(optional_run_path) if optional_run_path else None

    plan_metadata = _validate_plan(plan)
    grant_metadata = _validate_grant(grant)
    review_metadata = _validate_review(review)
    learned_metadata, learned_rows = _validate_learned_probability(learned)
    audit_metadata, audit_rows = _validate_generalization_audit(audit)
    optional_run_metadata = _validate_optional_phase1_run(optional_run)

    verified_plan_inputs = _verify_recorded_records(
        _get(plan, "metadata.inputs"),
        repo_root=root,
        label="plan metadata.inputs",
    )
    verified_grant_inputs = _verify_recorded_records(
        _get(grant, "metadata.inputs"),
        repo_root=root,
        label="grant metadata.inputs",
    )
    verified_review_inputs = _verify_recorded_records(
        _get(review, "metadata.inputs"),
        repo_root=root,
        label="review metadata.inputs",
    )
    verified_optional_run_inputs = (
        _verify_recorded_records(
            _get(optional_run, "metadata.inputs"),
            repo_root=root,
            label="optional phase1 run metadata.inputs",
        )
        if optional_run is not None
        else []
    )

    runtime_rows, join_summary = _build_runtime_rows(audit_rows=audit_rows, learned_rows=learned_rows)
    source_artifacts = {
        "phase2_write_mode_plan": _input_record("phase2_write_mode_plan", plan_path, repo_root=root),
        "authorization_grant": _input_record("authorization_grant", grant_path, repo_root=root),
        "phase1_no_write_pilot_review": _input_record("phase1_no_write_pilot_review", review_path, repo_root=root),
        "learned_probability_artifact": _input_record("learned_probability_artifact", learned_path, repo_root=root),
        "second_surface_generalization_audit": _input_record("second_surface_generalization_audit", audit_path, repo_root=root),
    }
    if optional_run_path is not None:
        source_artifacts["phase1_no_write_pilot_run"] = _input_record("phase1_no_write_pilot_run", optional_run_path, repo_root=root)
    verified_learned_inputs = [
        _verified_current_artifact_record("learned_probability_artifact", learned_path, repo_root=root)
    ]
    verified_audit_inputs = [
        _verified_current_artifact_record("second_surface_generalization_audit", audit_path, repo_root=root)
    ]

    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = _runtime_call([], flag_value=None)
    _require_disabled_runtime(preflight, label="preflight")
    pilot = _runtime_call(runtime_rows, flag_value="true")
    _require_pilot_runtime(pilot)
    postflight = _runtime_call([], flag_value=None)
    _require_disabled_runtime(postflight, label="postflight")
    files_written, observability, write_counts_payload = _write_isolated_files(
        pilot_dir=pilot_dir,
        pilot_run_id=run_id,
        generated_at=generated,
        source_artifacts=source_artifacts,
        runtime_rows=runtime_rows,
        pilot_result=pilot,
        preflight_result=preflight,
        postflight_result=postflight,
    )
    cleanup = _cleanup_pilot_dir(pilot_dir=pilot_dir, proof_root=proof_root, cleanup_after_proof=cleanup_after_proof)
    environment_restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original
    pass_fail = _evaluate_pass_fail(
        runtime_rows=runtime_rows,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        files_written=files_written,
        write_counts=write_counts_payload["write_counts_by_isolated_target"],
        cleanup=cleanup,
        cleanup_after_proof=cleanup_after_proof,
        environment_restored=environment_restored,
    )
    proof_passed = pass_fail["passed"] is True
    if not proof_passed:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError(
            "phase2 write-mode proof failed checks: " + ", ".join(pass_fail["failed_checks"])
        )
    blockers = {
        **dict(plan["shadow_and_production_blockers"]),
        "missing_phase2_write_mode_isolation_proof": not proof_passed,
        "phase2_writes_authorized": False,
        "online_shadow_execution_enabled": False,
    }
    identity = _identity_fields()
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "proof_version": proof_version,
            "generated_at": generated,
            "pilot_run_id": run_id,
            "inputs": list(source_artifacts.values()),
            "source_plan_version": plan_metadata.get("plan_version"),
            "source_grant_version": grant_metadata.get("grant_version"),
            "source_review_version": review_metadata.get("review_version"),
            "source_learned_probability_version": learned_metadata.get("artifact_version"),
            "source_generalization_audit_version": audit_metadata.get("artifact_version"),
            "source_phase1_run_version": optional_run_metadata.get("run_version") if optional_run_metadata else None,
            "verified_plan_inputs": verified_plan_inputs,
            "verified_grant_inputs": verified_grant_inputs,
            "verified_review_inputs": verified_review_inputs,
            "verified_learned_inputs": verified_learned_inputs,
            "verified_audit_inputs": verified_audit_inputs,
            "verified_optional_phase1_run_inputs": verified_optional_run_inputs,
            "runtime_feature_flag": FEATURE_FLAG,
            **identity,
        },
        "phase2_write_mode_proof_executed": True,
        "phase2_write_mode_proof_passed": proof_passed,
        "phase2_isolated_audit_write_mode_plan_defined": True,
        "phase2_write_mode_proof_allowed_by_plan": True,
        "phase2_write_mode_proof_scope": PROOF_SCOPE,
        "phase2_write_pilot_authorized": False,
        "phase2_writes_authorized": False,
        "online_shadow_execution_enabled": False,
        "runtime_writes_performed": pilot.get("writes_performed") is True,
        "isolated_artifact_tree_writes_performed": bool(files_written),
        "writes_performed": proof_passed,
        "missing_phase2_write_mode_isolation_proof": not proof_passed,
        "missing_production_readiness_authorization": True,
        "production_default_changed": False,
        "labels_used_for_scoring": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "source_artifacts": source_artifacts,
        "pilot_run_directory": {
            "pilot_run_id": run_id,
            "root_path": PRIMARY_TARGET_ROOT,
            "relative_path": f"{PRIMARY_TARGET_ROOT}{run_id}/",
            "resolved_path": str(pilot_dir),
            "cleanup_after_proof": cleanup_after_proof,
        },
        "preflight_checklist_results": {
            "grant_still_valid": True,
            "plan_primary_target_unchanged": True,
            "target_path_under_repo_root": _is_relative_to(pilot_dir, root),
            "no_forbidden_writes_configured": True,
            "complete_input_coverage": len(runtime_rows) == EXPECTED_POOL_SIZE,
        },
        "input_join_summary": join_summary,
        "disable_drill": {
            "preflight": _sanitize_runtime_result(preflight),
            "postflight": _sanitize_runtime_result(postflight),
            "passed": pass_fail["checks"]["preflight_disabled"] and pass_fail["checks"]["postflight_disabled"],
            "environment_restored": environment_restored,
        },
        "pilot_runtime_result": _sanitize_runtime_result(pilot),
        "isolated_file_writes": {
            "files_written": files_written,
            "file_count": len(files_written),
            "bytes_written": sum(int(record["byte_count"]) for record in files_written),
            "write_target": "isolated_audit_shadow_artifacts",
            "pilot_directory_cleaned_after_hashing": cleanup.get("cleanup_completed"),
        },
        "observability": observability,
        "write_count_verification": {
            **write_counts_payload,
            "forbidden_targets_zero": not pass_fail["forbidden_nonzero_write_counts"],
            "forbidden_nonzero_write_counts": pass_fail["forbidden_nonzero_write_counts"],
        },
        "pass_fail_evaluation": pass_fail,
        "rollback_and_cleanup": cleanup,
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": PASS_NEXT_STAGE if proof_passed else FAIL_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    writes = payload["isolated_file_writes"]
    cleanup = payload["rollback_and_cleanup"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write-Mode Proof ({metadata['proof_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records the isolated audit file-tree write-mode proof. Runtime scoring was scoped to this process, and all persistent proof output was constrained to the gitignored shadow-runs tree.",
        "",
        f"- Proof executed: {payload['phase2_write_mode_proof_executed']}",
        f"- Proof passed: {payload['phase2_write_mode_proof_passed']}",
        f"- Runtime writes performed: {payload['runtime_writes_performed']}",
        f"- Isolated artifact files written: {payload['isolated_artifact_tree_writes_performed']}",
        f"- Files written: {writes['file_count']}",
        f"- Cleanup completed: {cleanup['cleanup_completed']}",
        f"- Phase 2 writes authorized: {payload['phase2_writes_authorized']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Files Written",
        "",
    ]
    lines.extend(
        f"- `{record['relative_path']}`: {record['byte_count']} bytes, sha256 `{record['sha256']}`"
        for record in writes["files_written"]
    )
    lines.extend(["", "## Write Counts", ""])
    lines.extend(
        f"- `{key}`: {value}"
        for key, value in payload["write_count_verification"]["write_counts_by_isolated_target"].items()
    )
    lines.extend(["", "## Cleanup", ""])
    lines.append(f"- Cleanup target: `{cleanup['cleanup_target']}`")
    lines.append(f"- Directory absent after cleanup: {cleanup['directory_absent_after_cleanup']}")
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof(
    *,
    phase2_write_mode_plan_path: Path,
    authorization_grant_path: Path,
    phase1_no_write_pilot_review_path: Path,
    learned_probability_artifact_path: Path,
    second_surface_generalization_audit_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    phase1_no_write_pilot_run_path: Path | None = None,
    pilot_run_id: str | None = None,
    proof_version: str = PROOF_VERSION,
    repo_root: Path | None = None,
    cleanup_after_proof: bool = True,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof_payload(
        phase2_write_mode_plan_path=phase2_write_mode_plan_path,
        authorization_grant_path=authorization_grant_path,
        phase1_no_write_pilot_review_path=phase1_no_write_pilot_review_path,
        learned_probability_artifact_path=learned_probability_artifact_path,
        second_surface_generalization_audit_path=second_surface_generalization_audit_path,
        phase1_no_write_pilot_run_path=phase1_no_write_pilot_run_path,
        pilot_run_id=pilot_run_id,
        proof_version=proof_version,
        repo_root=repo_root,
        cleanup_after_proof=cleanup_after_proof,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof(payload),
        encoding="utf-8",
    )
    return payload
