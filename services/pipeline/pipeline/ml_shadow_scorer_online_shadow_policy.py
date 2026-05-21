"""Online shadow execution policy for ml-shadow-scorer-v1.

This command drafts a policy artifact after the isolated offline audit-output
gates pass. It reads committed JSON artifacts only. It does not query
databases, execute scoring, run ranking, train, generate embeddings, ingest
labels, implement runtime shadowing, or authorize production behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"

AUDIT_OUTPUT_GATES_ARTIFACT_TYPE = "ml_shadow_scorer_v1_audit_output_gates"
AUDIT_OUTPUT_GATES_VERSION = "ml-shadow-scorer-v1-audit-output-gates"
AUDIT_OUTPUT_ARTIFACT_TYPE = "ml_shadow_scorer_v1_audit_output"
AUDIT_OUTPUT_ARTIFACT_VERSION = "ml-shadow-scorer-v1-audit-output"
READINESS_ARTIFACT_TYPE = "ml_shadow_scorer_v1_execution_readiness_gates"
READINESS_GATES_VERSION = "ml-shadow-scorer-v1-execution-readiness-gates"
SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
VALIDATION_ARTIFACT_TYPE = "ml_hybrid_validation_on_fresh_surface"
VALIDATION_VERSION = "ml-hybrid-validation-on-fresh-surface-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

SCORER_ID = "ml-shadow-scorer-v1"
FORMULA_ID = "hybrid_rank_mean_50_50"
RANKING_RUN_ID = "rank-9f4b2a2084"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-fresh-hybrid-v1-20260518"
EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
EXPECTED_CANDIDATE_POOL_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
EXPECTED_POOL_SIZE = 358
AUDIT_OUTPUT_GATES_NEXT_STAGE = "draft_online_shadow_execution_policy_v1"
PASSED_NEXT_STAGE = "draft_ml_shadow_scorer_v1_generalization_audit_v1"

FEATURE_FLAG = "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"

CAVEATS = (
    "Policy document only; no online shadow execution is implemented or enabled.",
    "Validation evidence is snapshot-bound to rank-9f4b2a2084 / emerging / source-snapshot-fresh-hybrid-v1-20260518.",
    "Runtime implementation is explicitly deferred until a second-surface generalization audit passes.",
    "No API/web behavior, production default, user-visible ranking, training, embedding generation, or label ingest is authorized.",
)


class MLShadowScorerOnlineShadowPolicyError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPolicyError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPolicyError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPolicyError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerOnlineShadowPolicyError(f"Input {name} does not exist: {path}")
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
        raise MLShadowScorerOnlineShadowPolicyError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerOnlineShadowPolicyError(f"{name} metadata.{version_field} must be {version}")
    return metadata


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowPolicyError(f"{name} must be {expected!r}, got {observed!r}")


def _matching_outcome_value(payload: Mapping[str, Any], field: str) -> Any:
    top_level_present = field in payload
    top_level = payload.get(field)
    nested = _get(payload, f"overall_outcomes.{field}")
    if top_level_present and nested is not None and top_level != nested:
        raise MLShadowScorerOnlineShadowPolicyError(
            f"{field} disagrees between top-level and overall_outcomes"
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
        if component.get("source") != source or component.get("weight") != weight:
            return False
        seen.add(name)
    return seen == set(expected)


def _validate_audit_output_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-audit-output-gates",
        artifact_type=AUDIT_OUTPUT_GATES_ARTIFACT_TYPE,
        version_field="gates_version",
        version=AUDIT_OUTPUT_GATES_VERSION,
    )
    expected = {
        "shadow_audit_output_gates_passed": True,
        "offline_audit_output_ready": True,
        "validation_replay_exact": True,
        "recommended_next_stage": AUDIT_OUTPUT_GATES_NEXT_STAGE,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }
    for field, expected_value in expected.items():
        if _matching_outcome_value(payload, field) != expected_value:
            raise MLShadowScorerOnlineShadowPolicyError(
                f"audit output gates {field} must be {expected_value!r}"
            )
    return metadata


def _validate_audit_output(payload: Mapping[str, Any]) -> Mapping[str, Any]:
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
        "execution_verification.output_matches_validation_replay": _get(
            payload, "execution_verification.output_matches_validation_replay"
        )
        is True,
        "shadow_scoring_allowed": _get(payload, "shadow_and_production_blockers.shadow_scoring_allowed")
        is False,
        "production_default_allowed": _get(payload, "shadow_and_production_blockers.production_default_allowed")
        is False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerOnlineShadowPolicyError(f"audit output pre-checks failed: {failed}")
    return metadata


def _validate_readiness(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-execution-readiness-gates",
        artifact_type=READINESS_ARTIFACT_TYPE,
        version_field="gates_version",
        version=READINESS_GATES_VERSION,
    )
    if _matching_outcome_value(payload, "shadow_scorer_execution_readiness_passed") is not True:
        raise MLShadowScorerOnlineShadowPolicyError("shadow_scorer_execution_readiness_passed must be true")
    return metadata


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-spec",
        artifact_type=SPEC_ARTIFACT_TYPE,
        version_field="spec_version",
        version=SPEC_VERSION,
    )
    if not _formula_components_ok(payload):
        raise MLShadowScorerOnlineShadowPolicyError("spec formula must be hybrid_rank_mean_50_50")
    _require_equal("spec shadow_scoring_allowed", payload.get("shadow_scoring_allowed"), False)
    _require_equal("spec production_default_allowed", payload.get("production_default_allowed"), False)
    return metadata


def _validate_validation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="hybrid-validation-on-fresh-surface",
        artifact_type=VALIDATION_ARTIFACT_TYPE,
        version_field="validation_version",
        version=VALIDATION_VERSION,
    )
    _require_equal(
        "validation metadata.candidate_pool_work_set_sha256",
        metadata.get("candidate_pool_work_set_sha256"),
        EXPECTED_CANDIDATE_POOL_SHA,
    )
    _require_equal(
        "validation candidate_eval_coverage.candidate_pool_work_set_sha256",
        _get(payload, "candidate_eval_coverage.candidate_pool_work_set_sha256"),
        EXPECTED_CANDIDATE_POOL_SHA,
    )
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
        raise MLShadowScorerOnlineShadowPolicyError("production readiness plan must remain blocked")
    return metadata


def _candidate_sha_checks(
    *,
    audit_output_payload: Mapping[str, Any],
    readiness_payload: Mapping[str, Any],
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
        raise MLShadowScorerOnlineShadowPolicyError(f"candidate_pool_work_set_sha256 mismatch: {mismatches}")
    return {**values, "all_match_expected": True}


def _validation_snapshot_scope() -> dict[str, Any]:
    return {
        "evidence_applies_only_to_this_scope": True,
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
        "candidate_pool_size": EXPECTED_POOL_SIZE,
        "scope_extension_requires": [
            "new ml-hybrid-validation-on-fresh-surface cycle for that surface",
            "or completed ml-shadow-scorer-v1 generalization audit on a second fresh surface",
        ],
        "formula_generalization_beyond_validated_surface_asserted": False,
    }


def _terminology() -> dict[str, Any]:
    return {
        "shadow_scoring_allowed": {
            "definition": "Umbrella authorization for any shadow scoring path.",
            "value": False,
        },
        "shadow_audit_execution_allowed": {
            "definition": "Offline JSON audit only from prior gates.",
            "value": True,
            "source": "ml-shadow-scorer-v1-audit-output-gates",
        },
        "online_shadow_execution_enabled": {
            "definition": "Runtime path on.",
            "value": False,
            "policy_effect": "This policy does not enable it.",
        },
        "runtime_implementation_authorized": {
            "definition": "Authorization to implement online runtime shadowing.",
            "value": False,
            "until": "second-surface generalization audit gates pass",
        },
    }


def _generalization_requirement() -> dict[str, Any]:
    return {
        "required_before_runtime_implementation": True,
        "second_surface_generalization_audit_must_pass": True,
        "required_next_artifacts_in_order": [
            "draft_ml_shadow_scorer_v1_generalization_audit_v1",
            "audit_ml_shadow_scorer_v1_on_second_fresh_surface",
            "ml-shadow-scorer-v1-generalization-audit-gates",
        ],
        "runtime_recommended_next_stage_before_generalization_gates": None,
        "after_generalization_gates_pass_may_move_to": "implement_online_shadow_runtime_disabled_by_default",
        "runtime_implementation_authorized_by_this_policy": False,
    }


def _online_shadow_scope() -> dict[str, Any]:
    return {
        "future_only_not_authorized_now": True,
        "future_shadow_run_may_compute_alongside_existing_ranking_only_after": [
            "this policy exists",
            "second-surface generalization audit passes",
            "later runtime isolation verification artifact passes",
        ],
        "must_not_affect": [
            "user-visible ranking",
            "API responses",
            "bridge defaults",
            "production defaults",
        ],
        "results_use": "audit/monitoring only",
    }


def _data_dependency_contract() -> dict[str, Any]:
    return {
        "final_score_source": "production ranking outputs, read-only",
        "learned_probability_source": (
            "pre-existing audit_embedding_probability_work or an upstream approved artifact that applied "
            "frozen ml-offline-audit-embedding-scorer-v2 to pre-existing embeddings, read-only"
        ),
        "learned_probability_storage_requirement": (
            "does not require learned probability to live in embedding storage"
        ),
        "embedding_generation_at_shadow_time_allowed": False,
        "learned_scorer_refit_allowed": False,
        "learned_probability_creation_by_online_shadow_runtime_allowed": False,
        "skip_on_incomplete_coverage": True,
        "skip_behavior": "If learned probability coverage is incomplete for a ranking run, skip that run and emit skipped_ranking_run observability. No partial shadow scoring.",
    }


def _runtime_isolation_policy() -> dict[str, Any]:
    return {
        "future_only_not_implemented_here": True,
        "feature_flag": FEATURE_FLAG,
        "feature_flag_default": "off",
        "feature_flag_default_off": True,
        "local_eval_dry_run_required_before_write_mode": True,
        "write_mode_forbidden_until_runtime_isolation_verification_passes": True,
        "write_namespace": "shadow/audit-specific, specified by future implementation",
        "required_output_identity_fields": [
            "scorer_id",
            "formula_id",
            "scorer_version",
            "ranking_run_id",
            "candidate_pool_work_set_sha256",
            "family",
            "coverage",
        ],
    }


def _allowed_write_scope() -> dict[str, Any]:
    return {
        "future_only_after_later_gates": True,
        "targets": ["isolated shadow/audit table", "isolated shadow/audit artifact"],
        "required_fields": [
            "run_id",
            "scorer_id",
            "scorer_version",
            "formula_id",
            "input_hashes",
            "candidate_pool_work_set_sha256",
            "family",
            "component_coverage",
            "generated_at",
            "snapshot_identifiers",
        ],
    }


def _forbidden_write_scope() -> list[str]:
    return [
        "ranking_runs production/default pins",
        "paper_scores used by production/default ranking",
        "API-visible result tables",
        "production config/env/default bridge weights",
        "label datasets",
        "training/scorer artifacts",
        "embedding storage writes",
        "user-visible UI/API paths",
    ]


def _input_contract() -> dict[str, Any]:
    return {
        "required_inputs": [
            "canonical_openalex_work_id",
            "final_score",
            "audit_embedding_probability_work",
            "ranking_run_id",
            "family",
            "corpus_snapshot_version",
            "embedding_version",
        ],
        "display_only_inputs": ["title", "year", "source metadata"],
        "forbidden_inputs": [
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
            "good_or_acceptable",
            "label_any_positive",
            "reviewer_notes",
            "row_id",
            "sample_reason",
            "review_pool_variant",
            "holdout assignment",
            "fresh validation labels",
            "any feature selected or tuned using labels",
        ],
    }


def _output_contract() -> dict[str, Any]:
    return {
        "score_field": "ml_shadow_scorer_v1_score",
        "formula_id": FORMULA_ID,
        "scorer_id": SCORER_ID,
        "must_include": [
            "ranking_run_id",
            "family",
            "candidate_pool_work_set_sha256",
            "final_score_rank_pct",
            "audit_embedding_probability_rank_pct",
            "component coverage",
            "generated_at",
            "input hashes",
        ],
        "must_mark_audit_only": True,
    }


def _observability_contract() -> dict[str, Any]:
    return {
        "component_coverage": True,
        "missing_learned_probability": True,
        "score_distributions": True,
        "top_k_overlap_with_heuristic": True,
        "rank_displacement": True,
        "family_counts": True,
        "output_completeness": True,
        "runtime_errors": True,
        "latency": True,
        "skipped_candidates_and_reasons": True,
        "skipped_ranking_run_records": True,
        "write_counts_by_isolated_target": True,
    }


def _disable_and_rollback_policy() -> dict[str, Any]:
    return {
        "disable_switch": f"{FEATURE_FLAG}=off",
        "disable_switch_default": "off",
        "production_ranking_unaffected_when_on_or_off": True,
        "future_shadow_storage_cleanup_or_archival_policy_required": True,
    }


def _privacy_and_safety_policy() -> dict[str, Any]:
    return {
        "label_data_use_in_runtime_scoring_allowed": False,
        "user_visible_behavior_change_allowed": False,
        "audit_outputs_must_avoid_sensitive_review_notes": True,
        "runtime_records_must_be_traceable_to_input_hashes": True,
    }


def _future_runtime_verification_requirements() -> dict[str, Any]:
    return {
        "future_artifact": "ml-shadow-scorer-v1-runtime-isolation-verification",
        "must_prove": [
            "This policy implemented exactly",
            "Generalization audit on second surface passed first",
            "Feature flag default off",
            "Shadow writes confined to isolated namespace",
            "Zero production ranking writes",
            "API/web unchanged",
            "Observability complete",
            "Disable path tested",
            "Skip-on-incomplete-coverage tested",
            "snapshot/family/run scope on every record",
        ],
    }


def _separation_from_production_default_chain() -> dict[str, Any]:
    return {
        "future_online_shadow_gates_do_not_set_production_default_allowed": True,
        "production_default_authorization_chain_is_separate_and_stricter": True,
        "production_default_allowed": False,
    }


def build_ml_shadow_scorer_online_shadow_policy_payload(
    *,
    shadow_scorer_audit_output_gates_path: Path,
    shadow_scorer_audit_output_path: Path,
    shadow_scorer_execution_readiness_gates_path: Path,
    shadow_scorer_spec_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    production_readiness_plan_path: Path,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    audit_output_gates_path = Path(shadow_scorer_audit_output_gates_path).resolve()
    audit_output_path = Path(shadow_scorer_audit_output_path).resolve()
    readiness_path = Path(shadow_scorer_execution_readiness_gates_path).resolve()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    validation_path = Path(hybrid_validation_on_fresh_surface_path).resolve()
    production_plan_path = Path(production_readiness_plan_path).resolve()

    audit_output_gates_payload = _load_json_object(audit_output_gates_path)
    audit_output_payload = _load_json_object(audit_output_path)
    readiness_payload = _load_json_object(readiness_path)
    spec_payload = _load_json_object(spec_path)
    validation_payload = _load_json_object(validation_path)
    production_plan_payload = _load_json_object(production_plan_path)

    audit_output_gates_metadata = _validate_audit_output_gates(audit_output_gates_payload)
    audit_output_metadata = _validate_audit_output(audit_output_payload)
    readiness_metadata = _validate_readiness(readiness_payload)
    spec_metadata = _validate_spec(spec_payload)
    validation_metadata = _validate_validation(validation_payload)
    production_plan_metadata = _validate_production_plan(production_plan_payload)
    sha_checks = _candidate_sha_checks(
        audit_output_payload=audit_output_payload,
        readiness_payload=readiness_payload,
        spec_payload=spec_payload,
        validation_payload=validation_payload,
    )

    inputs = [
        _input_record("shadow_scorer_audit_output_gates", audit_output_gates_path, repo_root=root),
        _input_record("shadow_scorer_audit_output", audit_output_path, repo_root=root),
        _input_record("shadow_scorer_execution_readiness_gates", readiness_path, repo_root=root),
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("hybrid_validation_on_fresh_surface", validation_path, repo_root=root),
        _input_record("production_readiness_plan", production_plan_path, repo_root=root),
    ]

    blockers = {
        "missing_online_shadow_execution_policy": False,
        "missing_generalization_audit_on_second_surface": True,
        "missing_online_shadow_implementation_disabled_by_default": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "online_shadow_execution_enabled": False,
        "runtime_implementation_authorized": False,
    }

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "policy_version": policy_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_audit_output_gates_version": audit_output_gates_metadata.get("gates_version"),
        "source_audit_output_version": audit_output_metadata.get("artifact_version"),
        "source_execution_readiness_gates_version": readiness_metadata.get("gates_version"),
        "source_spec_version": spec_metadata.get("spec_version"),
        "source_validation_version": validation_metadata.get("validation_version"),
        "source_production_readiness_plan_version": production_plan_metadata.get("plan_version"),
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
        "scorer_id": SCORER_ID,
        "formula_id": FORMULA_ID,
        "caveats": list(CAVEATS),
    }

    validation_snapshot_scope = _validation_snapshot_scope()
    generalization_requirement = _generalization_requirement()
    data_dependency_contract = _data_dependency_contract()
    runtime_isolation_policy = _runtime_isolation_policy()

    return {
        "metadata": metadata,
        "evidence_summary": {
            "audit_output_gates_passed": _matching_outcome_value(
                audit_output_gates_payload, "shadow_audit_output_gates_passed"
            ),
            "offline_audit_output_ready": _matching_outcome_value(
                audit_output_gates_payload, "offline_audit_output_ready"
            ),
            "validation_replay_exact": _matching_outcome_value(
                audit_output_gates_payload, "validation_replay_exact"
            ),
            "audit_output_rows": _get(audit_output_payload, "execution_summary.output_row_count"),
            "audit_output_matches_validation_replay": _get(
                audit_output_payload, "execution_verification.output_matches_validation_replay"
            ),
            "execution_readiness_passed": _matching_outcome_value(
                readiness_payload, "shadow_scorer_execution_readiness_passed"
            ),
            "production_plan_blocked": _production_plan_blocked(production_plan_payload),
            "candidate_sha_checks": sha_checks,
        },
        "validation_snapshot_scope": validation_snapshot_scope,
        "online_shadow_scope": _online_shadow_scope(),
        "data_dependency_contract": data_dependency_contract,
        "runtime_isolation_policy": runtime_isolation_policy,
        "allowed_write_scope": _allowed_write_scope(),
        "forbidden_write_scope": _forbidden_write_scope(),
        "input_contract": _input_contract(),
        "output_contract": _output_contract(),
        "observability_contract": _observability_contract(),
        "disable_and_rollback_policy": _disable_and_rollback_policy(),
        "privacy_and_safety_policy": _privacy_and_safety_policy(),
        "future_runtime_verification_requirements": _future_runtime_verification_requirements(),
        "separation_from_production_default_chain": _separation_from_production_default_chain(),
        "generalization_requirement_before_runtime": generalization_requirement,
        "terminology": _terminology(),
        "blocked_actions": [
            "online_shadow_execution_enablement",
            "runtime_implementation",
            "production_default_change",
            "api_web_change",
            "user_visible_ranking_change",
            "embedding_generation",
            "learned_scorer_refit",
            "weight_tuning",
            "label_use_as_feature",
        ],
        "shadow_and_production_blockers": blockers,
        "online_shadow_execution_policy_defined": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "runtime_implementation_authorized": False,
        "recommended_next_stage": PASSED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_policy(payload: Mapping[str, Any]) -> str:
    scope = payload["validation_snapshot_scope"]
    data_contract = payload["data_dependency_contract"]
    runtime_policy = payload["runtime_isolation_policy"]
    generalization = payload["generalization_requirement_before_runtime"]
    lines = [
        f"# ML Shadow Scorer v1 Online Shadow Policy ({payload['metadata']['policy_version']})",
        "",
        "## Executive Summary",
        "",
        "This policy defines the minimum rules for any future online shadow path for `ml-shadow-scorer-v1`. It does not implement runtime shadowing, enable shadow execution, or authorize production behavior.",
        "",
        f"- Online shadow execution policy defined: {payload['online_shadow_execution_policy_defined']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Runtime implementation authorized: {payload['runtime_implementation_authorized']}",
        f"- Production default allowed: {payload['production_default_allowed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Evidence Chain",
        "",
        f"- Audit output gates passed: {payload['evidence_summary']['audit_output_gates_passed']}",
        f"- Offline audit output ready: {payload['evidence_summary']['offline_audit_output_ready']}",
        f"- Validation replay exact: {payload['evidence_summary']['validation_replay_exact']}",
        f"- Production plan still blocked: {payload['evidence_summary']['production_plan_blocked']}",
        "",
        "## Validation Snapshot Scope",
        "",
        f"- Ranking run: `{scope['ranking_run_id']}`",
        f"- Family: `{scope['family']}`",
        f"- Corpus snapshot: `{scope['corpus_snapshot_version']}`",
        f"- Embedding version: `{scope['embedding_version']}`",
        f"- Candidate pool SHA: `{scope['candidate_pool_work_set_sha256']}`",
        f"- Evidence applies only to this scope: {scope['evidence_applies_only_to_this_scope']}",
        f"- Formula generalization beyond validated surface asserted: {scope['formula_generalization_beyond_validated_surface_asserted']}",
        "",
        "## Terminology",
        "",
        "| Term | Value | Meaning |",
        "| --- | --- | --- |",
    ]
    for key, item in payload["terminology"].items():
        lines.append(f"| `{key}` | {item['value']} | {item['definition']} |")

    lines.extend(
        [
            "",
            "## Generalization Requirement Before Runtime",
            "",
            f"- Second-surface generalization audit must pass: {generalization['second_surface_generalization_audit_must_pass']}",
            f"- Runtime implementation authorized by this policy: {generalization['runtime_implementation_authorized_by_this_policy']}",
            "- Required next artifacts:",
        ]
    )
    lines.extend(f"  - `{item}`" for item in generalization["required_next_artifacts_in_order"])
    lines.extend(
        [
            "",
            "## Online Shadow Scope",
            "",
            "- Future-only and not authorized now.",
            "- Shadow results must not affect user-visible ranking, API responses, bridge defaults, or production defaults.",
            "- Results are audit/monitoring only.",
            "",
            "## Data Dependency Contract",
            "",
            f"- Final score source: {data_contract['final_score_source']}",
            f"- Learned probability source: {data_contract['learned_probability_source']}",
            f"- Embedding generation at shadow time allowed: {data_contract['embedding_generation_at_shadow_time_allowed']}",
            f"- Skip on incomplete coverage: {data_contract['skip_on_incomplete_coverage']}",
            "",
            "## Runtime Isolation",
            "",
            f"- Feature flag: `{runtime_policy['feature_flag']}`",
            f"- Feature flag default off: {runtime_policy['feature_flag_default_off']}",
            f"- Write mode forbidden until runtime isolation verification passes: {runtime_policy['write_mode_forbidden_until_runtime_isolation_verification_passes']}",
            "",
            "## Allowed Writes",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["allowed_write_scope"]["targets"])
    lines.extend(["", "## Forbidden Writes", ""])
    lines.extend(f"- {item}" for item in payload["forbidden_write_scope"])
    lines.extend(["", "## Observability", ""])
    for key, value in payload["observability_contract"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Disable And Rollback",
            "",
            f"- Disable switch: `{payload['disable_and_rollback_policy']['disable_switch']}`",
            f"- Production ranking unaffected when on or off: {payload['disable_and_rollback_policy']['production_ranking_unaffected_when_on_or_off']}",
            "",
            "## Separation From Production Default",
            "",
            f"- Future online shadow gates do not set production default allowed: {payload['separation_from_production_default_chain']['future_online_shadow_gates_do_not_set_production_default_allowed']}",
            f"- Production default authorization chain is separate: {payload['separation_from_production_default_chain']['production_default_authorization_chain_is_separate_and_stricter']}",
            "",
            "## Future Runtime Verification Requirements",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["future_runtime_verification_requirements"]["must_prove"])
    lines.extend(["", "## Remaining Blockers", ""])
    for key, value in payload["shadow_and_production_blockers"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_policy(
    *,
    shadow_scorer_audit_output_gates_path: Path,
    shadow_scorer_audit_output_path: Path,
    shadow_scorer_execution_readiness_gates_path: Path,
    shadow_scorer_spec_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_policy_payload(
        shadow_scorer_audit_output_gates_path=shadow_scorer_audit_output_gates_path,
        shadow_scorer_audit_output_path=shadow_scorer_audit_output_path,
        shadow_scorer_execution_readiness_gates_path=shadow_scorer_execution_readiness_gates_path,
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        hybrid_validation_on_fresh_surface_path=hybrid_validation_on_fresh_surface_path,
        production_readiness_plan_path=production_readiness_plan_path,
        policy_version=policy_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_online_shadow_policy(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "POLICY_VERSION",
    "MLShadowScorerOnlineShadowPolicyError",
    "build_ml_shadow_scorer_online_shadow_policy_payload",
    "markdown_from_ml_shadow_scorer_online_shadow_policy",
    "write_ml_shadow_scorer_online_shadow_policy",
]
