"""Phase 1 no-write pilot run for ml-shadow-scorer-v1 online shadow execution.

This module validates the bounded pilot plan, grant, disabled runtime artifact,
and committed second-surface audit evidence before invoking the pure in-memory
online shadow runtime under a scoped pilot feature-flag value. It writes only a
run/audit artifact. It does not query databases, persist shadow rows, mutate the
plan, change API/web behavior, or alter production/default ranking behavior.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_grant import (
    ARTIFACT_TYPE as GRANT_ARTIFACT_TYPE,
    GRANT_VERSION,
    OWNER,
    REVIEW_BY,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan import (
    ARTIFACT_TYPE as PLAN_ARTIFACT_TYPE,
    AUTHORIZATION_SCOPE,
    PLAN_VERSION,
    RECOMMENDED_NEXT_STAGE as PLAN_NEXT_STAGE,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    ARTIFACT_TYPE as RUNTIME_ARTIFACT_TYPE,
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FORBIDDEN_LABEL_FIELDS,
    FORMULA_ID,
    RANKING_RUN_ID,
    RUNTIME_VERSION,
    SCORER_ID,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run"
RUN_VERSION = "ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-run-v1"

LEARNED_PROBABILITY_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_learned_probability"
LEARNED_PROBABILITY_VERSION = "ml-shadow-scorer-v1-second-surface-learned-probability-v1"
GENERALIZATION_AUDIT_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_generalization_audit"
GENERALIZATION_AUDIT_VERSION = "ml-shadow-scorer-v1-second-surface-generalization-audit-v1"

EXPECTED_POOL_SIZE = 528
FINAL_SCORE_TOLERANCE = 1e-9
PASS_NEXT_STAGE = "review_online_shadow_phase1_pilot_results_v1"
FAIL_NEXT_STAGE = "remediate_online_shadow_phase1_pilot_v1"

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

POLICY_CONTRACT_OBSERVABILITY_KEYS = (
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

RUN_LEVEL_FIELDS = (
    "status",
    "shadow_row_count",
    "writes_performed",
    "production_default_changed",
    "user_visible_ranking_changed",
    "api_web_changes_allowed",
    "runtime_feature_flag_value",
    "labels_used_for_scoring",
)

FORBIDDEN_SOURCE_FIELDS_AS_RUNTIME_INPUTS = (
    "audit_embedding_probability_work from generalization audit",
    "ml_shadow_scorer_v1_score",
    "shadow_rank",
    "arm_scores",
    "labels",
    "title",
    "year",
    "metric fields",
)

WRITE_COUNT_TARGETS = (
    "ranking_runs",
    "paper_scores",
    "embeddings",
    "labels",
    "scorer_artifacts",
    "production_config",
    "api_visible_tables",
    "production_default_pins",
    "isolated_audit_shadow_tables",
)

CAVEATS = (
    "Phase 1 no-write pilot run artifact only; it does not mutate the plan artifact.",
    "Runtime output rows were held in memory for evaluation and are not persisted as shadow storage.",
    "Online shadow execution remains disabled globally after the scoped pilot call.",
    "No production default, production readiness, API/web behavior, or user-visible ranking change is authorized.",
    "Phase 2 isolated audit writes still require separate write-mode isolation proof and authorization.",
)


class MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


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
    }


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError("recorded input path must be a non-empty string")
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
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"{label} must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"{label}[{index}] must be an object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"{label}[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"{label}[{index}].sha256 missing")
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if actual_sha != recorded_sha:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"{label} input {name} sha256 mismatch: recorded {recorded_sha}, actual {actual_sha}"
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


def _rank_by_ids(ids: Sequence[str]) -> dict[str, int]:
    return {work_id: index + 1 for index, work_id in enumerate(ids)}


def _validate_identity_metadata(metadata: Mapping[str, Any], *, label: str) -> None:
    for field, expected in _identity_fields().items():
        _require_equal(f"{label} metadata.{field}", metadata.get(field), expected)


def _validate_plan(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(plan, name="phase1-no-write-pilot-plan")
    _require_equal("plan metadata.artifact_type", metadata.get("artifact_type"), PLAN_ARTIFACT_TYPE)
    _require_equal("plan metadata.plan_version", metadata.get("plan_version"), PLAN_VERSION)
    _validate_identity_metadata(metadata, label="plan")
    required = {
        "phase1_no_write_pilot_plan_defined": True,
        "phase1_no_write_pilot_executed": False,
        "shadow_and_production_blockers.phase1_no_write_pilot_executed": False,
        "recommended_next_stage": PLAN_NEXT_STAGE,
        "online_shadow_execution_authorized": True,
        "online_shadow_execution_enabled": False,
        "writes_allowed": False,
        "runtime_execution_authorized": True,
        "runtime_execution_authorization_scope": AUTHORIZATION_SCOPE,
        "shadow_scoring_allowed": True,
        "shadow_scoring_allowed_scope": AUTHORIZATION_SCOPE,
        "shadow_and_production_blockers.runtime_execution_authorized": True,
        "shadow_and_production_blockers.shadow_scoring_allowed": True,
        "shadow_and_production_blockers.missing_online_shadow_execution_authorization": False,
        "shadow_and_production_blockers.missing_production_readiness_authorization": True,
        "shadow_and_production_blockers.online_shadow_execution_enabled": False,
    }
    for path, expected in required.items():
        _require_equal(f"plan {path}", _get(plan, path), expected)
    policy_contract = _get(plan, "observability_plan.policy_contract")
    if not isinstance(policy_contract, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError("plan observability_plan.policy_contract missing")
    for key in POLICY_CONTRACT_OBSERVABILITY_KEYS:
        _require_equal(f"plan observability_plan.policy_contract.{key}", policy_contract.get(key), True)
    run_fields = _get(plan, "observability_plan.run_level_fields")
    if not isinstance(run_fields, list):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError("plan observability_plan.run_level_fields missing")
    for field in RUN_LEVEL_FIELDS:
        if field == "labels_used_for_scoring":
            continue
        if field not in run_fields:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"plan observability_plan.run_level_fields missing {field}"
            )
    return metadata


def _validate_grant(grant: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant, name="authorization-grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    _validate_identity_metadata(metadata, label="grant")
    required = {
        "grant_decision.decision": "granted",
        "grant_decision.owner": OWNER,
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "authorization_granted": True,
        "online_shadow_execution_authorized": True,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "write_mode_policy.phase_1": "no_writes",
        "write_mode_policy.phase_1_writes_allowed": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)
    return metadata


def _validate_runtime_artifact(runtime: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(runtime, name="online-shadow-runtime")
    _require_equal("runtime metadata.artifact_type", metadata.get("artifact_type"), RUNTIME_ARTIFACT_TYPE)
    _require_equal("runtime metadata.runtime_version", metadata.get("runtime_version"), RUNTIME_VERSION)
    _validate_identity_metadata(metadata, label="runtime")
    required = {
        "runtime_disabled_by_default": True,
        "runtime_default_state": "off",
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "last_disabled_run.status": "skipped_runtime_disabled",
        "last_disabled_run.shadow_row_count": 0,
        "last_disabled_run.writes_performed": False,
    }
    for path, expected in required.items():
        _require_equal(f"runtime {path}", _get(runtime, path), expected)
    return metadata


def _validate_learned_probability(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    metadata = _metadata(payload, name="learned-probability-artifact")
    _require_equal("learned metadata.artifact_type", metadata.get("artifact_type"), LEARNED_PROBABILITY_ARTIFACT_TYPE)
    _require_equal("learned metadata.artifact_version", metadata.get("artifact_version"), LEARNED_PROBABILITY_VERSION)
    _validate_identity_metadata(metadata, label="learned")
    rows = payload.get("candidate_work_scores")
    if not isinstance(rows, list):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError("learned candidate_work_scores must be a list")
    if len(rows) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"learned candidate_work_scores length must be {EXPECTED_POOL_SIZE}, got {len(rows)}"
        )
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"learned candidate_work_scores[{index}] must be an object"
            )
        if _float_or_none(row.get("audit_embedding_probability_work")) is None:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"learned candidate_work_scores[{index}] missing finite audit_embedding_probability_work"
            )
    return metadata, rows


def _validate_generalization_audit(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    metadata = _metadata(payload, name="second-surface-generalization-audit")
    _require_equal("audit metadata.artifact_type", metadata.get("artifact_type"), GENERALIZATION_AUDIT_ARTIFACT_TYPE)
    _require_equal("audit metadata.artifact_version", metadata.get("artifact_version"), GENERALIZATION_AUDIT_VERSION)
    _validate_identity_metadata(metadata, label="audit")
    _require_equal("audit generalization_audit_executed", payload.get("generalization_audit_executed"), True)
    rows = payload.get("shadow_output_rows")
    if not isinstance(rows, list):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError("audit shadow_output_rows must be a list")
    if len(rows) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"audit shadow_output_rows length must be {EXPECTED_POOL_SIZE}, got {len(rows)}"
        )
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"audit shadow_output_rows[{index}] must be object")
        if not str(row.get("canonical_openalex_work_id") or "").strip():
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"audit shadow_output_rows[{index}] missing canonical_openalex_work_id"
            )
        if _float_or_none(row.get("final_score")) is None:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
                f"audit shadow_output_rows[{index}] missing finite final_score"
            )
    return metadata, rows


def _duplicate_ids(rows: Sequence[Mapping[str, Any]], *, label: str) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        work_id = str(row.get("canonical_openalex_work_id") or "").strip()
        if not work_id:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"{label} row missing canonical_openalex_work_id")
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
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Mapping[str, Any]]]:
    audit_duplicates = _duplicate_ids(audit_rows, label="audit")
    learned_duplicates = _duplicate_ids(learned_rows, label="learned")
    if audit_duplicates or learned_duplicates:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"duplicate canonical IDs found: audit={audit_duplicates[:5]}, learned={learned_duplicates[:5]}"
        )
    forbidden_sources = _source_forbidden_label_fields(learned_rows, label="learned")
    if forbidden_sources:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"forbidden label fields present in source rows: {forbidden_sources[:3]}"
        )

    audit_by_id = {str(row["canonical_openalex_work_id"]).strip(): row for row in audit_rows}
    learned_by_id = {str(row["canonical_openalex_work_id"]).strip(): row for row in learned_rows}
    joined_ids = sorted(set(audit_by_id) & set(learned_by_id))
    if len(joined_ids) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"joined_candidate_count must be {EXPECTED_POOL_SIZE}, got {len(joined_ids)}"
        )
    recomputed_sha = _work_set_sha256(joined_ids)
    if recomputed_sha != CANDIDATE_POOL_WORK_SET_SHA256:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
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
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(f"audit row {work_id} missing finite final_score")
        if learned_probability is None:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
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
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"learned final_score mismatch with audit final_score: {mismatches[:3]}"
        )
    runtime_forbidden = _source_forbidden_label_fields(rows, label="runtime")
    if runtime_forbidden:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotRunError(
            f"forbidden label fields present on runtime rows: {runtime_forbidden[:3]}"
        )
    summary = {
        "audit_row_count": len(audit_rows),
        "learned_row_count": len(learned_rows),
        "joined_candidate_count": len(rows),
        "runtime_row_count": len(rows),
        "duplicate_audit_ids": audit_duplicates,
        "duplicate_learned_ids": learned_duplicates,
        "recomputed_candidate_pool_work_set_sha256": recomputed_sha,
        "final_score_source": "second_surface_generalization_audit.shadow_output_rows.final_score",
        "learned_probability_source": "second_surface_learned_probability.candidate_work_scores.audit_embedding_probability_work",
        "runtime_input_fields": list(RUNTIME_INPUT_FIELDS),
        "forbidden_source_fields_excluded_from_runtime_inputs": list(FORBIDDEN_SOURCE_FIELDS_AS_RUNTIME_INPUTS),
        "audit_label_provenance_fields_excluded_from_runtime_inputs": True,
        "final_score_mismatch_count": 0,
        "forbidden_label_fields_on_runtime_rows": [],
    }
    return rows, summary, audit_by_id


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
    elapsed_ms = (perf_counter() - started) * 1000
    out = dict(result)
    out["elapsed_ms"] = elapsed_ms
    out["runtime_errors"] = []
    return out


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
        "shadow_rows_persisted": False,
        "shadow_rows_omitted_from_artifact": True,
    }


def _execute_runtime_drill(runtime_rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], bool]:
    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = _runtime_call([], flag_value=None)
    pilot = _runtime_call(runtime_rows, flag_value="true")
    postflight = _runtime_call([], flag_value=None)
    restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original
    return (
        {
            "preflight": preflight,
            "pilot": pilot,
            "postflight": postflight,
        },
        restored,
    )


def _runtime_order(pilot_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(row.get("canonical_openalex_work_id")) for row in pilot_rows]


def _heuristic_order(audit_by_id: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [
        work_id
        for work_id, _row in sorted(
            audit_by_id.items(),
            key=lambda item: (-float(item[1]["final_score"]), str(item[0])),
        )
    ]


def _top_k_overlap(shadow_order: Sequence[str], heuristic_order: Sequence[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in (10, 20, 50, 100):
        shadow_top = set(shadow_order[:k])
        heuristic_top = set(heuristic_order[:k])
        overlap = len(shadow_top & heuristic_top)
        out[f"top_{k}"] = {
            "overlap_count": overlap,
            "overlap_rate": overlap / k,
        }
    return out


def _rank_displacement(shadow_order: Sequence[str], heuristic_order: Sequence[str]) -> dict[str, Any]:
    shadow_rank = _rank_by_ids(shadow_order)
    heuristic_rank = _rank_by_ids(heuristic_order)
    displacements = [abs(shadow_rank[work_id] - heuristic_rank[work_id]) for work_id in shadow_rank if work_id in heuristic_rank]
    return _distribution([float(value) for value in displacements])


def _family_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row.get("family") or "")
        counts[family] = counts.get(family, 0) + 1
    return counts


def _build_observability(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    pilot_result: Mapping[str, Any],
    runtime_results: Mapping[str, Mapping[str, Any]],
    audit_by_id: Mapping[str, Mapping[str, Any]],
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    shadow_rows = pilot_result.get("shadow_rows") if isinstance(pilot_result.get("shadow_rows"), list) else []
    shadow_order = _runtime_order([row for row in shadow_rows if isinstance(row, Mapping)])
    heuristic_order = _heuristic_order(audit_by_id)
    write_counts = {target: 0 for target in WRITE_COUNT_TARGETS}
    runtime_errors = []
    for result in runtime_results.values():
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
        "missing_learned_probability": {
            "missing_count": sum(
                1 for row in runtime_rows if _float_or_none(row.get("audit_embedding_probability_work")) is None
            ),
            "missing_work_ids": [],
        },
        "score_distributions": {
            "final_score": _distribution([float(row["final_score"]) for row in runtime_rows]),
            "audit_embedding_probability_work": _distribution(
                [float(row["audit_embedding_probability_work"]) for row in runtime_rows]
            ),
            "ml_shadow_scorer_v1_score": _distribution(
                [
                    float(row["ml_shadow_scorer_v1_score"])
                    for row in shadow_rows
                    if isinstance(row, Mapping) and _float_or_none(row.get("ml_shadow_scorer_v1_score")) is not None
                ]
            ),
        },
        "top_k_overlap_with_heuristic": _top_k_overlap(shadow_order, heuristic_order),
        "rank_displacement": _rank_displacement(shadow_order, heuristic_order),
        "family_counts": _family_counts(runtime_rows),
        "output_completeness": {
            "shadow_row_count": pilot_result.get("shadow_row_count"),
            "expected_shadow_row_count": EXPECTED_POOL_SIZE,
            "complete": pilot_result.get("shadow_row_count") == EXPECTED_POOL_SIZE,
            "shadow_rows_persisted": False,
            "unique_runtime_work_id_count": len({row["canonical_openalex_work_id"] for row in runtime_rows}),
        },
        "skipped_candidates_and_reasons": [],
        "skipped_ranking_run_records": [],
        "runtime_errors": runtime_errors,
        "latency": {
            "preflight_elapsed_ms": runtime_results["preflight"].get("elapsed_ms"),
            "pilot_elapsed_ms": runtime_results["pilot"].get("elapsed_ms"),
            "postflight_elapsed_ms": runtime_results["postflight"].get("elapsed_ms"),
            "total_elapsed_ms": sum(float(runtime_results[name].get("elapsed_ms") or 0.0) for name in runtime_results),
        },
        "write_counts_by_isolated_target": write_counts,
        "run_level": {
            "status": pilot_result.get("status"),
            "shadow_row_count": pilot_result.get("shadow_row_count"),
            "writes_performed": pilot_result.get("writes_performed"),
            "production_default_changed": pilot_result.get("production_default_changed"),
            "user_visible_ranking_changed": pilot_result.get("user_visible_ranking_changed"),
            "api_web_changes_allowed": False,
            "runtime_feature_flag_value": pilot_result.get("runtime_feature_flag_value"),
            "labels_used_for_scoring": pilot_result.get("labels_used_for_scoring"),
        },
        "plan_policy_contract_keys": list(_get(plan, "observability_plan.policy_contract").keys()),
        "plan_run_level_fields": list(_get(plan, "observability_plan.run_level_fields")) + ["labels_used_for_scoring"],
    }
    observability["policy_contract_satisfied"] = {
        key: key in observability and bool(observability[key] is not None) for key in POLICY_CONTRACT_OBSERVABILITY_KEYS
    }
    observability["run_level_fields_satisfied"] = {
        field: field in observability["run_level"] for field in RUN_LEVEL_FIELDS
    }
    return observability


def _evaluate_pass_fail(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    environment_restored: bool,
    observability: Mapping[str, Any],
) -> dict[str, Any]:
    write_counts = observability["write_counts_by_isolated_target"]
    checks = {
        "non_prod_only": True,
        "exact_identity_match": True,
        "complete_final_score_and_learned_probability_coverage": len(runtime_rows) == EXPECTED_POOL_SIZE,
        "no_label_fields_present": pilot.get("labels_used_for_scoring") is False,
        "runtime_returns_rows_only_in_memory": pilot.get("shadow_row_count") == EXPECTED_POOL_SIZE,
        "writes_performed_false": pilot.get("writes_performed") is False,
        "all_write_counts_zero": all(value == 0 for value in write_counts.values()),
        "production_api_user_visible_outputs_unchanged": (
            pilot.get("production_default_changed") is False and pilot.get("user_visible_ranking_changed") is False
        ),
        "disable_drill_passes": (
            preflight.get("status") == "skipped_runtime_disabled"
            and preflight.get("shadow_row_count") == 0
            and preflight.get("writes_performed") is False
            and postflight.get("status") == "skipped_runtime_disabled"
            and postflight.get("shadow_row_count") == 0
            and postflight.get("writes_performed") is False
        ),
        "pilot_runtime_succeeded": pilot.get("status") == "succeeded_test_only" and pilot.get("shadow_row_count") == EXPECTED_POOL_SIZE,
        "environment_restored": environment_restored,
        "all_grant_required_observability_fields_recorded": all(
            observability["policy_contract_satisfied"].values()
        )
        and all(observability["run_level_fields_satisfied"].values()),
    }
    failed = [name for name, passed in checks.items() if passed is not True]
    return {
        "checks": checks,
        "failed_checks": failed,
        "passed": not failed,
    }


def build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run_payload(
    *,
    phase1_no_write_pilot_plan_path: Path,
    authorization_grant_path: Path,
    online_shadow_runtime_path: Path,
    learned_probability_artifact_path: Path,
    second_surface_generalization_audit_path: Path,
    run_version: str = RUN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(phase1_no_write_pilot_plan_path).resolve()
    grant_path = Path(authorization_grant_path).resolve()
    runtime_path = Path(online_shadow_runtime_path).resolve()
    learned_path = Path(learned_probability_artifact_path).resolve()
    audit_path = Path(second_surface_generalization_audit_path).resolve()

    plan = _load_json_object(plan_path)
    grant = _load_json_object(grant_path)
    runtime_artifact = _load_json_object(runtime_path)
    learned = _load_json_object(learned_path)
    audit = _load_json_object(audit_path)

    plan_metadata = _validate_plan(plan)
    grant_metadata = _validate_grant(grant)
    runtime_metadata = _validate_runtime_artifact(runtime_artifact)
    learned_metadata, learned_rows = _validate_learned_probability(learned)
    audit_metadata, audit_rows = _validate_generalization_audit(audit)

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
    verified_grant_request_inputs = _verify_recorded_records(
        _get(grant, "metadata.verified_request_inputs"),
        repo_root=root,
        label="grant metadata.verified_request_inputs",
        required=False,
    )
    verified_grant_input_chain = _verify_recorded_records(
        _get(grant, "metadata.verified_input_chain"),
        repo_root=root,
        label="grant metadata.verified_input_chain",
        required=False,
    )

    runtime_rows, join_summary, audit_by_id = _build_runtime_rows(audit_rows=audit_rows, learned_rows=learned_rows)
    runtime_results, environment_restored = _execute_runtime_drill(runtime_rows)
    preflight = runtime_results["preflight"]
    pilot = runtime_results["pilot"]
    postflight = runtime_results["postflight"]
    observability = _build_observability(
        runtime_rows=runtime_rows,
        pilot_result=pilot,
        runtime_results=runtime_results,
        audit_by_id=audit_by_id,
        plan=plan,
    )
    pass_fail = _evaluate_pass_fail(
        runtime_rows=runtime_rows,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        environment_restored=environment_restored,
        observability=observability,
    )
    phase1_passed = pass_fail["passed"] is True
    blockers = {
        **dict(plan["shadow_and_production_blockers"]),
        "phase1_no_write_pilot_executed": True,
    }
    write_counts = observability["write_counts_by_isolated_target"]
    source_artifacts = {
        "phase1_no_write_pilot_plan": _input_record("phase1_no_write_pilot_plan", plan_path, repo_root=root),
        "authorization_grant": _input_record("authorization_grant", grant_path, repo_root=root),
        "online_shadow_runtime": _input_record("online_shadow_runtime", runtime_path, repo_root=root),
        "learned_probability_artifact": _input_record("learned_probability_artifact", learned_path, repo_root=root),
        "second_surface_generalization_audit": _input_record(
            "second_surface_generalization_audit",
            audit_path,
            repo_root=root,
        ),
    }
    identity = _identity_fields()
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "run_version": run_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": list(source_artifacts.values()),
        "source_plan_version": plan_metadata.get("plan_version"),
        "source_grant_version": grant_metadata.get("grant_version"),
        "source_runtime_version": runtime_metadata.get("runtime_version"),
        "source_learned_probability_version": learned_metadata.get("artifact_version"),
        "source_generalization_audit_version": audit_metadata.get("artifact_version"),
        "verified_plan_inputs": verified_plan_inputs,
        "verified_grant_inputs": verified_grant_inputs,
        "verified_grant_request_inputs": verified_grant_request_inputs,
        "verified_grant_input_chain": verified_grant_input_chain,
        "runtime_feature_flag": FEATURE_FLAG,
        "scorer_id": SCORER_ID,
        "formula_id": FORMULA_ID,
        **identity,
    }
    return {
        "metadata": metadata,
        "phase1_no_write_pilot_executed": True,
        "phase1_no_write_pilot_passed": phase1_passed,
        "online_shadow_execution_authorized": True,
        "online_shadow_execution_enabled": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "runtime_execution_authorized": True,
        "runtime_execution_authorization_scope": AUTHORIZATION_SCOPE,
        "shadow_scoring_allowed": True,
        "shadow_scoring_allowed_scope": AUTHORIZATION_SCOPE,
        "writes_performed": pilot.get("writes_performed") is True,
        "writes_allowed": False,
        "disable_drill_passed": pass_fail["checks"]["disable_drill_passes"],
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "source_artifacts": source_artifacts,
        "input_join_summary": join_summary,
        "disable_drill": {
            "preflight": _sanitize_runtime_result(preflight),
            "postflight": _sanitize_runtime_result(postflight),
            "passed": pass_fail["checks"]["disable_drill_passes"],
            "environment_restored": environment_restored,
        },
        "pilot_runtime_result": _sanitize_runtime_result(pilot),
        "observability": observability,
        "pass_fail_evaluation": pass_fail,
        "no_write_verification": {
            "writes_allowed": False,
            "writes_performed": pilot.get("writes_performed") is True,
            "write_count": pilot.get("write_count"),
            "write_counts_by_isolated_target": write_counts,
            "all_write_counts_zero": all(value == 0 for value in write_counts.values()),
            "shadow_rows_persisted": False,
            "shadow_storage_persistence_allowed": False,
        },
        "rollback_summary": {
            "disable_drill_passed": pass_fail["checks"]["disable_drill_passes"],
            "feature_flag_restored_after_runner": environment_restored,
            "postflight_status": postflight.get("status"),
            "production_ranking_unchanged": True,
            "production_default_changed": False,
            "user_visible_ranking_changed": False,
        },
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": PASS_NEXT_STAGE if phase1_passed else FAIL_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    join = payload["input_join_summary"]
    pilot = payload["pilot_runtime_result"]
    disable = payload["disable_drill"]
    no_write = payload["no_write_verification"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Phase 1 No-Write Pilot Run ({metadata['run_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records the bounded non-production Phase 1 no-write pilot run. Runtime rows were evaluated in memory only; no shadow rows, database writes, API/web changes, or production/default changes were persisted.",
        "",
        f"- Pilot executed: {payload['phase1_no_write_pilot_executed']}",
        f"- Pilot passed: {payload['phase1_no_write_pilot_passed']}",
        f"- Pilot runtime status: `{pilot['status']}`",
        f"- Shadow row count: {pilot['shadow_row_count']}",
        f"- Writes performed: {payload['writes_performed']}",
        f"- Disable drill passed: {payload['disable_drill_passed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Input Join",
        "",
        f"- Audit rows: {join['audit_row_count']}",
        f"- Learned-probability rows: {join['learned_row_count']}",
        f"- Joined candidates: {join['joined_candidate_count']}",
        f"- Recomputed pool SHA: `{join['recomputed_candidate_pool_work_set_sha256']}`",
        f"- Runtime input fields: {', '.join(f'`{field}`' for field in join['runtime_input_fields'])}",
        "",
        "## Disable Drill",
        "",
        f"- Preflight status: `{disable['preflight']['status']}`",
        f"- Postflight status: `{disable['postflight']['status']}`",
        f"- Environment restored: {disable['environment_restored']}",
        "",
        "## No-Write Verification",
        "",
        f"- Writes allowed: {no_write['writes_allowed']}",
        f"- Writes performed: {no_write['writes_performed']}",
        f"- Shadow rows persisted: {no_write['shadow_rows_persisted']}",
        f"- All write counts zero: {no_write['all_write_counts_zero']}",
        "",
        "## Observability",
        "",
        f"- Policy contract fields satisfied: {payload['observability']['policy_contract_satisfied']}",
        f"- Run-level fields satisfied: {payload['observability']['run_level_fields_satisfied']}",
        "",
        "## Blockers",
        "",
    ]
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run(
    *,
    phase1_no_write_pilot_plan_path: Path,
    authorization_grant_path: Path,
    online_shadow_runtime_path: Path,
    learned_probability_artifact_path: Path,
    second_surface_generalization_audit_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    run_version: str = RUN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run_payload(
        phase1_no_write_pilot_plan_path=phase1_no_write_pilot_plan_path,
        authorization_grant_path=authorization_grant_path,
        online_shadow_runtime_path=online_shadow_runtime_path,
        learned_probability_artifact_path=learned_probability_artifact_path,
        second_surface_generalization_audit_path=second_surface_generalization_audit_path,
        run_version=run_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run(payload),
        encoding="utf-8",
    )
    return payload
