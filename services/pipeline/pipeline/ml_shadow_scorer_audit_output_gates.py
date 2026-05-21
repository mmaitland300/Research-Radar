"""Metric gates for isolated ml-shadow-scorer-v1 audit output.

This evaluator reads committed audit artifacts only. It verifies that the
offline ml-shadow-scorer-v1 audit output is complete, isolated, and exact
against validation replay. It does not query databases, rerun scoring, execute
ranking, train, embed, ingest labels, or authorize online shadow or production
behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_v1_audit_output_gates"
GATES_VERSION = "ml-shadow-scorer-v1-audit-output-gates"

AUDIT_OUTPUT_ARTIFACT_TYPE = "ml_shadow_scorer_v1_audit_output"
AUDIT_OUTPUT_ARTIFACT_VERSION = "ml-shadow-scorer-v1-audit-output"
READINESS_ARTIFACT_TYPE = "ml_shadow_scorer_v1_execution_readiness_gates"
READINESS_GATES_VERSION = "ml-shadow-scorer-v1-execution-readiness-gates"
IMPLEMENTATION_ARTIFACT_TYPE = "ml_shadow_scorer_v1_implementation"
IMPLEMENTATION_VERSION = "ml-shadow-scorer-v1-implementation"
SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
VALIDATION_ARTIFACT_TYPE = "ml_hybrid_validation_on_fresh_surface"
VALIDATION_VERSION = "ml-hybrid-validation-on-fresh-surface-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

FORMULA_ID = "hybrid_rank_mean_50_50"
SCORER_ID = "ml-shadow-scorer-v1"
EXPECTED_CANDIDATE_POOL_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
EXPECTED_POOL_SIZE = 358
EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT = 143
PASSED_NEXT_STAGE = "draft_online_shadow_execution_policy_v1"
FAILED_NEXT_STAGE = "repair_ml_shadow_scorer_v1_audit_output"

REQUIRED_ROW_FIELDS = (
    "shadow_rank",
    "canonical_openalex_work_id",
    "final_score",
    "audit_embedding_probability_work",
    "final_score_rank_pct",
    "audit_embedding_probability_rank_pct",
    "ml_shadow_scorer_v1_score",
)

CAVEATS = (
    "Passing confirms offline audit output only.",
    "draft_online_shadow_execution_policy_v1 is the next authorized spec/plan step only.",
    "No online shadow, API/web, production default, or user-visible ranking change is authorized.",
)


class MLShadowScorerAuditOutputGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerAuditOutputGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerAuditOutputGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerAuditOutputGatesError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _float_or_none(value: Any) -> float | None:
    if _is_number(value):
        return float(value)
    return None


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerAuditOutputGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    if metadata.get("artifact_type") != artifact_type:
        raise MLShadowScorerAuditOutputGatesError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerAuditOutputGatesError(f"{name} metadata.{version_field} must be {version}")
    return metadata


def _readiness_value(payload: Mapping[str, Any], field: str) -> Any:
    top_level_present = field in payload
    top_level = payload.get(field)
    nested = _get(payload, f"overall_outcomes.{field}")
    if top_level_present and nested is not None and top_level != nested:
        raise MLShadowScorerAuditOutputGatesError(
            f"readiness {field} disagrees between top-level and overall_outcomes"
        )
    return top_level if top_level_present else nested


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    good = _get(payload, "targets.good_or_acceptable")
    good_blocked = isinstance(good, Mapping) and good.get("production_eligible") is False
    default_unauthorized = payload.get("production_default_authorized") is not True
    return bool(good_blocked and default_unauthorized)


def _formula_components_ok(spec_payload: Mapping[str, Any]) -> bool:
    if _get(spec_payload, "scoring_formula.formula_id") != FORMULA_ID:
        return False
    components = _get(spec_payload, "scoring_formula.components")
    if not isinstance(components, list) or len(components) != 2:
        return False
    expected = {
        "final_score_rank_pct": ("rank_pct(final_score)", 0.5),
        "audit_embedding_probability_rank_pct": ("rank_pct(audit_embedding_probability_work)", 0.5),
    }
    seen: set[str] = set()
    for component in components:
        if not isinstance(component, Mapping):
            return False
        name = str(component.get("name") or "")
        source_weight = expected.get(name)
        if source_weight is None:
            return False
        source, weight = source_weight
        if component.get("source") != source or _float_or_none(component.get("weight")) != weight:
            return False
        seen.add(name)
    return seen == set(expected)


def _validate_audit_output(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-audit-output",
        artifact_type=AUDIT_OUTPUT_ARTIFACT_TYPE,
        version_field="artifact_version",
        version=AUDIT_OUTPUT_ARTIFACT_VERSION,
    )
    checks = {
        "execution_summary.status": _get(payload, "execution_summary.status") == "succeeded",
        "execution_summary.output_row_count": _get(payload, "execution_summary.output_row_count")
        == EXPECTED_POOL_SIZE,
        "execution_summary.candidate_pool_size": _get(payload, "execution_summary.candidate_pool_size")
        == EXPECTED_POOL_SIZE,
        "execution_summary.learned_probability_coverage_count": _get(
            payload, "execution_summary.learned_probability_coverage_count"
        )
        == EXPECTED_POOL_SIZE,
        "execution_summary.missing_learned_probability_count": _get(
            payload, "execution_summary.missing_learned_probability_count"
        )
        == 0,
        "coverage_summary.confirmatory_metric_eligible_count": _get(
            payload, "coverage_summary.confirmatory_metric_eligible_count"
        )
        == EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
        "execution_verification.output_matches_validation_replay": _get(
            payload, "execution_verification.output_matches_validation_replay"
        )
        is True,
        "execution_verification.max_abs_score_delta": _get(
            payload, "execution_verification.max_abs_score_delta"
        )
        == 0.0,
        "execution_verification.max_abs_rank_pct_delta": _get(
            payload, "execution_verification.max_abs_rank_pct_delta"
        )
        == 0.0,
        "execution_verification.mismatched_work_count": _get(
            payload, "execution_verification.mismatched_work_count"
        )
        == 0,
        "metadata.shadow_execution_enabled": metadata.get("shadow_execution_enabled") is False,
        "metadata.production_default_changed": metadata.get("production_default_changed") is False,
        "metadata.api_web_changed": metadata.get("api_web_changed") is False,
        "shadow_and_production_blockers.shadow_scoring_allowed": _get(
            payload, "shadow_and_production_blockers.shadow_scoring_allowed"
        )
        is False,
        "shadow_and_production_blockers.production_default_allowed": _get(
            payload, "shadow_and_production_blockers.production_default_allowed"
        )
        is False,
        "recommended_next_stage": payload.get("recommended_next_stage")
        == "draft_ml_shadow_scorer_v1_audit_output_gates",
        "shadow_and_production_blockers.missing_ml_shadow_scorer_v1_audit_output_gates": _get(
            payload, "shadow_and_production_blockers.missing_ml_shadow_scorer_v1_audit_output_gates"
        )
        is True,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerAuditOutputGatesError(f"audit output pre-checks failed: {failed}")

    rows = payload.get("shadow_output_rows")
    if not isinstance(rows, list):
        raise MLShadowScorerAuditOutputGatesError("audit output shadow_output_rows must be a list")
    if len(rows) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerAuditOutputGatesError(
            f"audit output shadow_output_rows length must be {EXPECTED_POOL_SIZE}"
        )
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerAuditOutputGatesError(f"shadow_output_rows[{index}] must be an object")
    return metadata, rows


def _validate_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    missing_required: list[dict[str, Any]] = []
    bad_label_flag: list[str] = []
    ranks: list[int] = []
    duplicate_work_ids: set[str] = set()
    seen_work_ids: set[str] = set()
    for index, row in enumerate(rows):
        work_id = str(row.get("canonical_openalex_work_id") or "")
        if not work_id:
            missing_required.append({"index": index, "missing": ["canonical_openalex_work_id"]})
        elif work_id in seen_work_ids:
            duplicate_work_ids.add(work_id)
        seen_work_ids.add(work_id)

        missing = [field for field in REQUIRED_ROW_FIELDS if field not in row or row.get(field) is None]
        numeric_missing = [
            field
            for field in (
                "final_score",
                "audit_embedding_probability_work",
                "final_score_rank_pct",
                "audit_embedding_probability_rank_pct",
                "ml_shadow_scorer_v1_score",
            )
            if _float_or_none(row.get(field)) is None
        ]
        if missing or numeric_missing:
            missing_required.append({"work_id": work_id or index, "missing": sorted(set(missing + numeric_missing))})
        if row.get("label_any_positive_not_used_for_scoring") is not True:
            bad_label_flag.append(work_id or str(index))
        rank = row.get("shadow_rank")
        if isinstance(rank, int) and not isinstance(rank, bool):
            ranks.append(rank)
        else:
            missing_required.append({"work_id": work_id or index, "missing": ["shadow_rank"]})

    expected_ranks = list(range(1, EXPECTED_POOL_SIZE + 1))
    ranks_ok = sorted(ranks) == expected_ranks and len(set(ranks)) == EXPECTED_POOL_SIZE
    return {
        "row_count": len(rows),
        "required_fields_present": not missing_required,
        "required_fields_missing_preview": missing_required[:10],
        "label_any_positive_not_used_for_scoring_all_true": not bad_label_flag,
        "bad_label_flag_preview": bad_label_flag[:10],
        "shadow_rank_unique_1_to_358": ranks_ok,
        "duplicate_canonical_work_ids": sorted(duplicate_work_ids)[:10],
        "duplicate_canonical_work_id_count": len(duplicate_work_ids),
    }


def _validate_readiness(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-execution-readiness-gates",
        artifact_type=READINESS_ARTIFACT_TYPE,
        version_field="gates_version",
        version=READINESS_GATES_VERSION,
    )
    expected_values = {
        "shadow_scorer_execution_readiness_passed": True,
        "shadow_audit_execution_allowed": True,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }
    for field, expected in expected_values.items():
        if _readiness_value(payload, field) != expected:
            raise MLShadowScorerAuditOutputGatesError(f"readiness {field} must be {expected}")
    return metadata


def _validate_implementation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-implementation",
        artifact_type=IMPLEMENTATION_ARTIFACT_TYPE,
        version_field="implementation_version",
        version=IMPLEMENTATION_VERSION,
    )
    checks = {
        "implemented": _get(payload, "implementation_status.implemented") is True,
        "disabled_by_default": _get(payload, "implementation_status.disabled_by_default") is True,
        "implementation_matches_spec": _get(payload, "implementation_status.implementation_matches_spec") is True,
        "implementation_matches_validation_replay": _get(
            payload, "implementation_status.implementation_matches_validation_replay"
        )
        is True,
        "missing_ml_shadow_scorer_v1_implementation": _get(
            payload, "implementation_status.missing_ml_shadow_scorer_v1_implementation"
        )
        is False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerAuditOutputGatesError(f"implementation pre-checks failed: {failed}")
    return metadata


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-spec",
        artifact_type=SPEC_ARTIFACT_TYPE,
        version_field="spec_version",
        version=SPEC_VERSION,
    )
    checks = {
        "formula_id": _get(payload, "scoring_formula.formula_id") == FORMULA_ID,
        "formula_components": _formula_components_ok(payload),
        "shadow_scoring_allowed": payload.get("shadow_scoring_allowed") is False,
        "production_default_allowed": payload.get("production_default_allowed") is False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerAuditOutputGatesError(f"spec pre-checks failed: {failed}")
    return metadata


def _validate_validation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="hybrid-validation-on-fresh-surface",
        artifact_type=VALIDATION_ARTIFACT_TYPE,
        version_field="validation_version",
        version=VALIDATION_VERSION,
    )
    if metadata.get("candidate_pool_work_set_sha256") != EXPECTED_CANDIDATE_POOL_SHA:
        raise MLShadowScorerAuditOutputGatesError("validation metadata candidate_pool_work_set_sha256 mismatch")
    if _get(payload, "candidate_eval_coverage.candidate_pool_work_set_sha256") != EXPECTED_CANDIDATE_POOL_SHA:
        raise MLShadowScorerAuditOutputGatesError("validation candidate_eval_coverage candidate_pool_work_set_sha256 mismatch")
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="production-readiness-plan",
        artifact_type=PRODUCTION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PRODUCTION_PLAN_VERSION,
    )
    if not _production_plan_blocked(payload):
        raise MLShadowScorerAuditOutputGatesError("production readiness plan must remain blocked")
    return metadata


def _candidate_sha_checks(
    *,
    audit_output_payload: Mapping[str, Any],
    readiness_payload: Mapping[str, Any],
    implementation_payload: Mapping[str, Any],
    spec_payload: Mapping[str, Any],
    validation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    values = {
        "audit_output.metadata.candidate_pool_work_set_sha256": _get(
            audit_output_payload, "metadata.candidate_pool_work_set_sha256"
        ),
        "audit_output.execution_verification.candidate_pool_work_set_sha256": _get(
            audit_output_payload, "execution_verification.candidate_pool_work_set_sha256"
        ),
        "readiness.metadata.candidate_pool_work_set_sha256": _get(
            readiness_payload, "metadata.candidate_pool_work_set_sha256"
        ),
        "implementation.metadata.candidate_pool_work_set_sha256": _get(
            implementation_payload, "metadata.candidate_pool_work_set_sha256"
        ),
        "implementation.audit_replay_summary.candidate_pool_work_set_sha256": _get(
            implementation_payload, "audit_replay_summary.candidate_pool_work_set_sha256"
        ),
        "spec.metadata.candidate_pool_work_set_sha256": _get(spec_payload, "metadata.candidate_pool_work_set_sha256"),
        "validation.metadata.candidate_pool_work_set_sha256": _get(
            validation_payload, "metadata.candidate_pool_work_set_sha256"
        ),
        "validation.candidate_eval_coverage.candidate_pool_work_set_sha256": _get(
            validation_payload, "candidate_eval_coverage.candidate_pool_work_set_sha256"
        ),
    }
    mismatches = {key: value for key, value in values.items() if value != EXPECTED_CANDIDATE_POOL_SHA}
    if mismatches:
        raise MLShadowScorerAuditOutputGatesError(f"candidate_pool_work_set_sha256 mismatch: {mismatches}")
    return {**values, "all_match_expected": True}


def _observability_populated(payload: Mapping[str, Any]) -> dict[str, Any]:
    sections = {
        "score_distribution": payload.get("score_distribution"),
        "rank_displacement_summary": payload.get("rank_displacement_summary"),
        "top_k_overlap_summary": payload.get("top_k_overlap_summary"),
        "coverage_summary": payload.get("coverage_summary"),
        "observability_summary": payload.get("observability_summary"),
        "top_k_preview": payload.get("top_k_preview"),
    }
    populated = {
        key: isinstance(value, Mapping) and bool(value) if key != "top_k_preview" else isinstance(value, list) and bool(value)
        for key, value in sections.items()
    }
    return {"sections_populated": populated, "all_populated": all(populated.values())}


def _readiness_contract_satisfied(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _metadata(payload, name="shadow-scorer-audit-output")
    source_contract = payload.get("source_contract")
    source_contract = source_contract if isinstance(source_contract, Mapping) else {}
    isolation = source_contract.get("required_shadow_output_isolation_contract")
    observability = source_contract.get("required_observability_contract")
    inputs = metadata.get("inputs")
    input_hashes_ok = isinstance(inputs, list) and bool(inputs) and all(
        isinstance(item, Mapping) and bool(item.get("sha256")) for item in inputs
    )
    checks = {
        "isolation_contract_copied": isinstance(isolation, Mapping) and bool(isolation),
        "observability_contract_copied": isinstance(observability, Mapping) and bool(observability),
        "audit_file_satisfies_offline_isolation": source_contract.get(
            "audit_file_satisfies_isolation_contract_for_offline_path_only"
        )
        is True,
        "run_id": bool(metadata.get("ranking_run_id")),
        "scorer_identity_version": metadata.get("scorer_id") == SCORER_ID
        and metadata.get("artifact_version") == AUDIT_OUTPUT_ARTIFACT_VERSION,
        "formula_id": metadata.get("formula_id") == FORMULA_ID,
        "input_hashes": input_hashes_ok,
        "candidate_pool_work_set_sha256": metadata.get("candidate_pool_work_set_sha256") == EXPECTED_CANDIDATE_POOL_SHA,
        "coverage": isinstance(payload.get("execution_summary"), Mapping)
        and isinstance(payload.get("coverage_summary"), Mapping),
    }
    return {**checks, "all_satisfied": all(checks.values())}


def _gate(
    gate_id: str,
    title: str,
    passed: bool,
    observed: Any,
    rationale: str,
    *,
    status: str | None = None,
) -> dict[str, Any]:
    gate_status = status or ("pass" if passed else "fail")
    return {
        "gate_id": gate_id,
        "title": title,
        "status": gate_status,
        "passed": passed,
        "observed_value": observed,
        "rationale": rationale,
    }


def build_ml_shadow_scorer_audit_output_gates_payload(
    *,
    shadow_scorer_audit_output_path: Path,
    shadow_scorer_execution_readiness_gates_path: Path,
    shadow_scorer_implementation_path: Path,
    shadow_scorer_spec_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    production_readiness_plan_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    audit_output_path = Path(shadow_scorer_audit_output_path).resolve()
    readiness_path = Path(shadow_scorer_execution_readiness_gates_path).resolve()
    implementation_path = Path(shadow_scorer_implementation_path).resolve()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    validation_path = Path(hybrid_validation_on_fresh_surface_path).resolve()
    production_plan_path = Path(production_readiness_plan_path).resolve()

    audit_output_payload = _load_json_object(audit_output_path)
    readiness_payload = _load_json_object(readiness_path)
    implementation_payload = _load_json_object(implementation_path)
    spec_payload = _load_json_object(spec_path)
    validation_payload = _load_json_object(validation_path)
    production_plan_payload = _load_json_object(production_plan_path)

    audit_metadata, audit_rows = _validate_audit_output(audit_output_payload)
    readiness_metadata = _validate_readiness(readiness_payload)
    implementation_metadata = _validate_implementation(implementation_payload)
    spec_metadata = _validate_spec(spec_payload)
    validation_metadata = _validate_validation(validation_payload)
    production_plan_metadata = _validate_production_plan(production_plan_payload)
    sha_checks = _candidate_sha_checks(
        audit_output_payload=audit_output_payload,
        readiness_payload=readiness_payload,
        implementation_payload=implementation_payload,
        spec_payload=spec_payload,
        validation_payload=validation_payload,
    )

    row_schema = _validate_rows(audit_rows)
    if not (
        row_schema["required_fields_present"]
        and row_schema["label_any_positive_not_used_for_scoring_all_true"]
        and row_schema["shadow_rank_unique_1_to_358"]
        and row_schema["duplicate_canonical_work_id_count"] == 0
    ):
        raise MLShadowScorerAuditOutputGatesError(f"audit output row schema pre-checks failed: {row_schema}")
    observability = _observability_populated(audit_output_payload)
    readiness_contract = _readiness_contract_satisfied(audit_output_payload)
    isolation_observed = {
        "metadata.shadow_execution_enabled": audit_metadata.get("shadow_execution_enabled"),
        "metadata.production_default_changed": audit_metadata.get("production_default_changed"),
        "metadata.api_web_changed": audit_metadata.get("api_web_changed"),
        "shadow_scoring_allowed": _get(audit_output_payload, "shadow_and_production_blockers.shadow_scoring_allowed"),
        "production_default_allowed": _get(
            audit_output_payload, "shadow_and_production_blockers.production_default_allowed"
        ),
    }

    g01 = True
    g02 = (
        _get(audit_output_payload, "execution_summary.output_row_count") == EXPECTED_POOL_SIZE
        and _get(audit_output_payload, "execution_summary.candidate_pool_size") == EXPECTED_POOL_SIZE
        and len(audit_rows) == EXPECTED_POOL_SIZE
    )
    g03 = (
        _get(audit_output_payload, "execution_summary.learned_probability_coverage_count") == EXPECTED_POOL_SIZE
        and _get(audit_output_payload, "execution_summary.missing_learned_probability_count") == 0
    )
    g04 = (
        _get(audit_output_payload, "execution_verification.output_matches_validation_replay") is True
        and _get(audit_output_payload, "execution_verification.max_abs_score_delta") == 0.0
        and _get(audit_output_payload, "execution_verification.max_abs_rank_pct_delta") == 0.0
        and _get(audit_output_payload, "execution_verification.mismatched_work_count") == 0
    )
    g05 = (
        row_schema["required_fields_present"]
        and row_schema["label_any_positive_not_used_for_scoring_all_true"]
        and row_schema["shadow_rank_unique_1_to_358"]
        and row_schema["duplicate_canonical_work_id_count"] == 0
    )
    g06 = (
        audit_metadata.get("shadow_execution_enabled") is False
        and audit_metadata.get("production_default_changed") is False
        and audit_metadata.get("api_web_changed") is False
        and _get(audit_output_payload, "shadow_and_production_blockers.shadow_scoring_allowed") is False
        and _get(audit_output_payload, "shadow_and_production_blockers.production_default_allowed") is False
    )
    g07 = observability["all_populated"]
    g08 = readiness_contract["all_satisfied"]
    g09 = _production_plan_blocked(production_plan_payload)
    prior_gate_passes = [g01, g02, g03, g04, g05, g06, g07, g08, g09]
    g10 = all(prior_gate_passes)

    gate_results = [
        _gate(
            "G01_input_artifacts_valid",
            "Input Artifacts Valid",
            g01,
            {"candidate_sha_checks": sha_checks},
            "All inputs have valid artifact/version identity and candidate SHA linkage.",
        ),
        _gate(
            "G02_audit_output_complete",
            "Audit Output Complete",
            g02,
            {
                "summary_output_row_count": _get(audit_output_payload, "execution_summary.output_row_count"),
                "summary_candidate_pool_size": _get(audit_output_payload, "execution_summary.candidate_pool_size"),
                "shadow_output_rows_length": len(audit_rows),
            },
            "Audit output summary and row list contain the complete 358-work pool.",
        ),
        _gate(
            "G03_component_coverage",
            "Component Coverage",
            g03,
            {
                "learned_probability_coverage_count": _get(
                    audit_output_payload, "execution_summary.learned_probability_coverage_count"
                ),
                "missing_learned_probability_count": _get(
                    audit_output_payload, "execution_summary.missing_learned_probability_count"
                ),
            },
            "All 358 rows have learned probability coverage and none are missing.",
        ),
        _gate(
            "G04_validation_replay_exact",
            "Validation Replay Exact",
            g04,
            audit_output_payload.get("execution_verification"),
            "Audit output matches the validation primary hybrid arm exactly.",
        ),
        _gate(
            "G05_row_schema_complete",
            "Row Schema Complete",
            g05,
            row_schema,
            "Every row contains required score fields, label-not-used marker, and unique shadow_rank 1..358.",
        ),
        _gate(
            "G06_isolation_preserved",
            "Isolation Preserved",
            g06,
            isolation_observed,
            "Audit output preserves disabled shadow/prod/API state.",
        ),
        _gate(
            "G07_observability_populated",
            "Observability Populated",
            g07,
            observability,
            "Audit output includes non-empty coverage, distributions, overlap, displacement, observability, and preview sections.",
        ),
        _gate(
            "G08_readiness_contract_satisfied",
            "Readiness Contract Satisfied",
            g08,
            readiness_contract,
            "Audit output carries isolation/observability contract and required audit-file metadata.",
        ),
        _gate(
            "G09_production_readiness_alignment",
            "Production Readiness Alignment",
            g09,
            {"production_plan_blocked": _production_plan_blocked(production_plan_payload)},
            "Production readiness plan still blocks production default.",
        ),
        _gate(
            "G10_audit_output_decision",
            "Audit Output Decision",
            g10,
            {"prior_gate_statuses": ["pass" if passed else "fail" for passed in prior_gate_passes]},
            "Audit output gates pass iff G01 through G09 pass.",
        ),
    ]

    blockers = {
        "missing_ml_shadow_scorer_v1_spec": False,
        "missing_ml_shadow_scorer_v1_implementation": False,
        "missing_shadow_execution_readiness_gates": False,
        "missing_shadow_output_isolation_check": False,
        "missing_ml_shadow_scorer_v1_audit_output_artifact": False,
        "missing_ml_shadow_scorer_v1_audit_output_gates": not g10,
        "missing_online_shadow_execution_policy": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }

    inputs = [
        _input_record("shadow_scorer_audit_output", audit_output_path, repo_root=root),
        _input_record("shadow_scorer_execution_readiness_gates", readiness_path, repo_root=root),
        _input_record("shadow_scorer_implementation", implementation_path, repo_root=root),
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("hybrid_validation_on_fresh_surface", validation_path, repo_root=root),
        _input_record("production_readiness_plan", production_plan_path, repo_root=root),
    ]

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": gates_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_audit_output_version": audit_metadata.get("artifact_version"),
        "source_readiness_gates_version": readiness_metadata.get("gates_version"),
        "source_implementation_version": implementation_metadata.get("implementation_version"),
        "source_spec_version": spec_metadata.get("spec_version"),
        "source_validation_version": validation_metadata.get("validation_version"),
        "source_production_readiness_plan_version": production_plan_metadata.get("plan_version"),
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "gate_results": gate_results,
        "prechecks": {
            "candidate_sha_checks": sha_checks,
            "row_schema": row_schema,
            "observability": observability,
            "readiness_contract": readiness_contract,
            "production_plan_blocked": _production_plan_blocked(production_plan_payload),
        },
        "overall_outcomes": {
            "shadow_audit_output_gates_passed": g10,
            "audit_output_complete": g02,
            "validation_replay_exact": g04,
            "offline_audit_output_ready": g10,
            "shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "recommended_next_stage": PASSED_NEXT_STAGE if g10 else FAILED_NEXT_STAGE,
        },
        "shadow_and_production_blockers": blockers,
        "blocked_actions": [
            "online_shadow_execution",
            "production_default_change",
            "api_web_change",
            "user_visible_ranking_change",
            "scorer_execution_or_rerun",
            "database_access",
            "ranking_run",
            "training",
            "embedding_generation",
            "label_ingest",
        ],
        "shadow_audit_output_gates_passed": g10,
        "audit_output_complete": g02,
        "validation_replay_exact": g04,
        "offline_audit_output_ready": g10,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "recommended_next_stage": PASSED_NEXT_STAGE if g10 else FAILED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def markdown_from_ml_shadow_scorer_audit_output_gates(payload: Mapping[str, Any]) -> str:
    outcomes = payload["overall_outcomes"]
    prechecks = payload["prechecks"]
    lines = [
        f"# ML Shadow Scorer v1 Audit Output Gates ({payload['metadata']['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact evaluates the isolated `ml-shadow-scorer-v1` audit output. It confirms offline audit-output readiness only; it does not authorize online shadowing or production behavior.",
        "",
        f"- Audit output gates passed: {outcomes['shadow_audit_output_gates_passed']}",
        f"- Offline audit output ready: {outcomes['offline_audit_output_ready']}",
        f"- Validation replay exact: {outcomes['validation_replay_exact']}",
        f"- Shadow execution enabled: {outcomes['shadow_execution_enabled']}",
        f"- Production default allowed: {outcomes['production_default_allowed']}",
        f"- Recommended next stage: `{outcomes['recommended_next_stage']}`",
        "",
        "## Gate Results",
        "",
        "| Gate | Status | Rationale |",
        "| --- | --- | --- |",
    ]
    for gate in payload["gate_results"]:
        lines.append(f"| `{gate['gate_id']}` | {gate['status']} | {gate['rationale']} |")
    lines.extend(
        [
            "",
            "## Completeness",
            "",
            f"- Row count: {prechecks['row_schema']['row_count']}",
            f"- Required fields present: {prechecks['row_schema']['required_fields_present']}",
            f"- Unique shadow ranks 1..358: {prechecks['row_schema']['shadow_rank_unique_1_to_358']}",
            f"- Label-not-used marker on every row: {prechecks['row_schema']['label_any_positive_not_used_for_scoring_all_true']}",
            "",
            "## Observability",
            "",
        ]
    )
    for section, populated in prechecks["observability"]["sections_populated"].items():
        lines.append(f"- `{section}` populated: {populated}")
    lines.extend(
        [
            "",
            "## Readiness Contract",
            "",
        ]
    )
    for key, value in prechecks["readiness_contract"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Blockers",
            "",
        ]
    )
    for key, value in payload["shadow_and_production_blockers"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_audit_output_gates(
    *,
    shadow_scorer_audit_output_path: Path,
    shadow_scorer_execution_readiness_gates_path: Path,
    shadow_scorer_implementation_path: Path,
    shadow_scorer_spec_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_audit_output_gates_payload(
        shadow_scorer_audit_output_path=shadow_scorer_audit_output_path,
        shadow_scorer_execution_readiness_gates_path=shadow_scorer_execution_readiness_gates_path,
        shadow_scorer_implementation_path=shadow_scorer_implementation_path,
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        hybrid_validation_on_fresh_surface_path=hybrid_validation_on_fresh_surface_path,
        production_readiness_plan_path=production_readiness_plan_path,
        gates_version=gates_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_audit_output_gates(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "GATES_VERSION",
    "MLShadowScorerAuditOutputGatesError",
    "build_ml_shadow_scorer_audit_output_gates_payload",
    "markdown_from_ml_shadow_scorer_audit_output_gates",
    "write_ml_shadow_scorer_audit_output_gates",
]
