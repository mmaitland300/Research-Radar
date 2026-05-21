"""Specification writer for ml-shadow-scorer-v1.

This command drafts a contract artifact after fresh-surface hybrid validation
metric gates pass. It reads existing JSON artifacts only. It does not query a
database, execute scoring, train, generate embeddings, ingest labels, run
shadow scoring, or authorize production changes.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
SCORER_ID = "ml-shadow-scorer-v1"

GATES_ARTIFACT_TYPE = "ml_hybrid_validation_metric_gates"
GATES_VERSION = "ml-hybrid-validation-metric-gates-v1"
VALIDATION_ARTIFACT_TYPE = "ml_hybrid_validation_on_fresh_surface"
VALIDATION_VERSION = "ml-hybrid-validation-on-fresh-surface-v1"
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
AUDIT_SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
AUDIT_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
AUDIT_SCORER_FIT_MODE = "holdout_bound_train_only"
HYBRID_EXPERIMENT_SPEC_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment_spec"
HYBRID_EXPERIMENT_SPEC_VERSION = "ml-hybrid-scorer-offline-experiment-v1-spec"

PRIMARY_CONFIRMATORY_ARM = "hybrid_rank_mean_50_50"
SECONDARY_REPORTING_ARM = "hybrid_rank_mean_25_75_heuristic"
RANKING_RUN_ID = "rank-9f4b2a2084"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-fresh-hybrid-v1-20260518"
EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
FROZEN_FORMULA = (
    "0.5 * rank_pct(final_score) + "
    "0.5 * rank_pct(audit_embedding_probability_work)"
)

ALLOWED_INPUTS = (
    "canonical_openalex_work_id",
    "final_score",
    "audit_embedding_probability_work",
    "ranking_run_id",
    "family",
    "corpus_snapshot_version",
    "embedding_version",
    "title",
    "year",
    "source metadata for audit display only",
)

FORBIDDEN_INPUTS = (
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "good_or_acceptable",
    "label_any_positive",
    "any derived label targets",
    "reviewer_notes",
    "row_id",
    "sample_reason",
    "review_pool_variant",
    "holdout assignment",
    "fresh validation labels",
    "any feature selected or tuned using labels",
)

OBSERVABILITY_REQUIREMENTS = (
    "component coverage counts",
    "missing learned probability count",
    "score distribution for final_score",
    "score distribution for audit_embedding_probability_work",
    "score distribution for hybrid shadow score",
    "top-k overlap with heuristic final_score",
    "rank displacement summary",
    "family-level counts",
    "shadow output completeness",
    "error counters if implemented online",
    "latency counters if implemented online",
)

FUTURE_READINESS_GATES = (
    "implementation matches this exact formula",
    "learned component uses frozen ml-offline-audit-embedding-scorer-v2 output or successor explicitly validated by a new gate",
    "no production/default config changed",
    "shadow writes isolated from production ranking",
    "full component coverage",
    "no label leakage",
    "monitoring fields emitted",
    "rollback/disable path documented",
    "production default remains blocked",
)

CAVEATS = (
    "Spec only; no shadow scoring executed.",
    "Spec does not authorize shadow execution.",
    "Spec does not authorize production default, API, web, or model deployment changes.",
    "Frozen formula uses the primary hybrid arm confirmed on the fresh 143-work denominator.",
    "Learned component must use frozen scorer output without refit.",
    "No eval-label weight tuning or label-derived features are allowed.",
)


class MLShadowScorerSpecError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSpecError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSpecError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSpecError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSpecError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _as_string(value: Any) -> str:
    return str(value or "").strip()


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerSpecError(f"{name} must be {expected!r}, got {observed!r}")


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    good = _get(payload, "targets.good_or_acceptable")
    good_blocked = isinstance(good, Mapping) and good.get("production_eligible") is False
    default_unauthorized = payload.get("production_default_authorized") is not True
    return bool(good_blocked and default_unauthorized)


def _validate_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-validation-metric-gates")
    _require_equal("gates metadata.artifact_type", metadata.get("artifact_type"), GATES_ARTIFACT_TYPE)
    _require_equal("gates metadata.gates_version", metadata.get("gates_version"), GATES_VERSION)
    _require_equal("gates confirmatory_validation_passed", payload.get("confirmatory_validation_passed"), True)
    _require_equal(
        "gates fresh_surface_hybrid_validation_passed",
        payload.get("fresh_surface_hybrid_validation_passed"),
        True,
    )
    _require_equal("gates primary_hybrid_material_lift_passed", payload.get("primary_hybrid_material_lift_passed"), True)
    _require_equal("gates recommended_next_stage", payload.get("recommended_next_stage"), "draft_ml_shadow_scorer_v1_spec")
    _require_equal("gates primary_confirmatory_arm", payload.get("primary_confirmatory_arm"), PRIMARY_CONFIRMATORY_ARM)
    _require_equal("gates shadow_scoring_allowed", payload.get("shadow_scoring_allowed"), False)
    _require_equal("gates production_default_allowed", payload.get("production_default_allowed"), False)

    shadow_blockers = payload.get("shadow_blockers")
    if not isinstance(shadow_blockers, list):
        shadow_blockers = []
    blockers_obj = payload.get("shadow_and_production_blockers")
    missing_impl = (
        "missing_ml_shadow_scorer_v1_implementation" in shadow_blockers
        or "missing_ml_shadow_scorer_v1" in shadow_blockers
        or (isinstance(blockers_obj, Mapping) and blockers_obj.get("missing_ml_shadow_scorer_v1_implementation") is True)
        or (isinstance(blockers_obj, Mapping) and blockers_obj.get("missing_ml_shadow_scorer_v1") is True)
    )
    if not missing_impl:
        raise MLShadowScorerSpecError("gates blockers must still include missing ml-shadow-scorer-v1 implementation")
    not_complete_present = "confirmatory_validation_not_complete" in shadow_blockers
    if isinstance(blockers_obj, Mapping):
        not_complete_present = not_complete_present or blockers_obj.get("confirmatory_validation_not_complete") is True
    if not_complete_present:
        raise MLShadowScorerSpecError("gates blockers must not include confirmatory_validation_not_complete")
    return metadata


def _validate_validation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-validation-on-fresh-surface")
    _require_equal("validation metadata.artifact_type", metadata.get("artifact_type"), VALIDATION_ARTIFACT_TYPE)
    _require_equal("validation metadata.validation_version", metadata.get("validation_version"), VALIDATION_VERSION)
    _require_equal(
        "validation confirmatory primary arm",
        _get(payload, "confirmatory_decision_inputs.primary_confirmatory_arm"),
        PRIMARY_CONFIRMATORY_ARM,
    )
    return metadata


def _validate_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-surface")
    _require_equal("fresh surface metadata.artifact_type", metadata.get("artifact_type"), SURFACE_ARTIFACT_TYPE)
    _require_equal("fresh surface metadata.surface_version", metadata.get("surface_version"), SURFACE_VERSION)
    _require_equal("fresh surface metadata.status", metadata.get("status"), "materialized_ready")
    _require_equal("fresh surface ready_for_hybrid_validation_scoring", payload.get("ready_for_hybrid_validation_scoring"), True)
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    _require_equal("fresh policy metadata.artifact_type", metadata.get("artifact_type"), POLICY_ARTIFACT_TYPE)
    _require_equal("fresh policy metadata.policy_version", metadata.get("policy_version"), POLICY_VERSION)
    _require_equal("policy frozen primary arm", _get(payload, "frozen_hybrid_arms.primary_confirmatory_arm"), PRIMARY_CONFIRMATORY_ARM)
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    _require_equal("production plan metadata.artifact_type", metadata.get("artifact_type"), PRODUCTION_PLAN_ARTIFACT_TYPE)
    _require_equal("production plan metadata.plan_version", metadata.get("plan_version"), PRODUCTION_PLAN_VERSION)
    if not _production_plan_blocked(payload):
        raise MLShadowScorerSpecError("production readiness plan must keep production default blocked")
    return metadata


def _validate_optional_scorer(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export")
    _require_equal("audit scorer metadata.artifact_type", metadata.get("artifact_type"), AUDIT_SCORER_ARTIFACT_TYPE)
    _require_equal("audit scorer metadata.scorer_version", metadata.get("scorer_version"), AUDIT_SCORER_VERSION)
    _require_equal("audit scorer metadata.fit_mode", metadata.get("fit_mode"), AUDIT_SCORER_FIT_MODE)
    if _get(payload, "policy_compliance.eval_works_excluded_from_fit") is not True:
        raise MLShadowScorerSpecError("audit scorer policy_compliance.eval_works_excluded_from_fit must be true")
    return metadata


def _validate_optional_hybrid_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-experiment-spec")
    _require_equal("hybrid experiment spec metadata.artifact_type", metadata.get("artifact_type"), HYBRID_EXPERIMENT_SPEC_ARTIFACT_TYPE)
    _require_equal("hybrid experiment spec metadata.spec_version", metadata.get("spec_version"), HYBRID_EXPERIMENT_SPEC_VERSION)
    return metadata


def _cross_artifact_checks(
    *,
    gates_payload: Mapping[str, Any],
    validation_payload: Mapping[str, Any],
    surface_payload: Mapping[str, Any],
    policy_payload: Mapping[str, Any],
) -> dict[str, Any]:
    validation_metadata = _metadata(validation_payload, name="hybrid-validation-on-fresh-surface")
    surface_candidate_source = surface_payload.get("candidate_source")
    if not isinstance(surface_candidate_source, Mapping):
        raise MLShadowScorerSpecError("fresh surface candidate_source must be present")

    checks = {
        "ranking_run_id_matches": validation_metadata.get("ranking_run_id") == RANKING_RUN_ID
        and _get(validation_payload, "validation_scope.ranking_run_id") == RANKING_RUN_ID
        and surface_candidate_source.get("ranking_run_id") == RANKING_RUN_ID,
        "family_matches": validation_metadata.get("family") == FAMILY
        and _get(validation_payload, "validation_scope.family") == FAMILY
        and surface_candidate_source.get("family") == FAMILY,
        "corpus_snapshot_version_matches": validation_metadata.get("corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION
        and _get(validation_payload, "validation_scope.corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION
        and surface_candidate_source.get("corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION,
        "embedding_version_matches": validation_metadata.get("embedding_version") == EMBEDDING_VERSION
        and _get(validation_payload, "validation_scope.embedding_version") == EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256_matches": validation_metadata.get("candidate_pool_work_set_sha256")
        == _get(surface_payload, "candidate_pool.candidate_work_set_sha256")
        == _get(gates_payload, "metadata.candidate_pool_work_set_sha256"),
        "primary_confirmatory_arm_matches": gates_payload.get("primary_confirmatory_arm") == PRIMARY_CONFIRMATORY_ARM
        and _get(validation_payload, "confirmatory_decision_inputs.primary_confirmatory_arm") == PRIMARY_CONFIRMATORY_ARM
        and _get(policy_payload, "frozen_hybrid_arms.primary_confirmatory_arm") == PRIMARY_CONFIRMATORY_ARM,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerSpecError(f"cross-artifact provenance checks failed: {failed}")

    return {
        **checks,
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": validation_metadata.get("candidate_pool_work_set_sha256"),
    }


def build_ml_shadow_scorer_spec_payload(
    *,
    hybrid_validation_metric_gates_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    audit_embedding_scorer_export_path: Path | None = None,
    hybrid_experiment_spec_path: Path | None = None,
    spec_version: str = SPEC_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    gates_path = Path(hybrid_validation_metric_gates_path).resolve()
    validation_path = Path(hybrid_validation_on_fresh_surface_path).resolve()
    surface_path = Path(fresh_eval_surface_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    scorer_path = Path(audit_embedding_scorer_export_path).resolve() if audit_embedding_scorer_export_path else None
    hybrid_spec_path = Path(hybrid_experiment_spec_path).resolve() if hybrid_experiment_spec_path else None

    gates_payload = _load_json_object(gates_path)
    validation_payload = _load_json_object(validation_path)
    surface_payload = _load_json_object(surface_path)
    policy_payload = _load_json_object(policy_path)
    plan_payload = _load_json_object(plan_path)

    gates_metadata = _validate_gates(gates_payload)
    validation_metadata = _validate_validation(validation_payload)
    surface_metadata = _validate_surface(surface_payload)
    policy_metadata = _validate_policy(policy_payload)
    plan_metadata = _validate_production_plan(plan_payload)
    scorer_metadata: Mapping[str, Any] | None = None
    if scorer_path is not None:
        scorer_metadata = _validate_optional_scorer(_load_json_object(scorer_path))
    if hybrid_spec_path is not None:
        _validate_optional_hybrid_spec(_load_json_object(hybrid_spec_path))
    checks = _cross_artifact_checks(
        gates_payload=gates_payload,
        validation_payload=validation_payload,
        surface_payload=surface_payload,
        policy_payload=policy_payload,
    )

    inputs = [
        _input_record("hybrid_validation_metric_gates", gates_path, repo_root=root),
        _input_record("hybrid_validation_on_fresh_surface", validation_path, repo_root=root),
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    if scorer_path is not None:
        inputs.append(_input_record("audit_embedding_scorer_export", scorer_path, repo_root=root))
    if hybrid_spec_path is not None:
        inputs.append(_input_record("hybrid_experiment_spec", hybrid_spec_path, repo_root=root))

    coverage = gates_payload.get("comparison_summary", {}).get("candidate_eval_coverage", {})
    primary = gates_payload.get("comparison_summary", {}).get("primary_hybrid_arm", {})
    primary_deltas = primary.get("deltas_vs_heuristic") if isinstance(primary, Mapping) else {}
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "spec_version": spec_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_hybrid_validation_metric_gates_version": gates_metadata.get("gates_version"),
        "source_hybrid_validation_version": validation_metadata.get("validation_version"),
        "source_fresh_surface_version": surface_metadata.get("surface_version"),
        "source_fresh_surface_policy_version": policy_metadata.get("policy_version"),
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "candidate_pool_work_set_sha256": checks["candidate_pool_work_set_sha256"],
        "strategic_framing": [
            "confirmatory_validation_passed_enables_shadow_spec_drafting_only",
            "shadow_execution_remains_blocked",
            "production_default_remains_blocked",
            "implementation_must_be_disabled_by_default",
        ],
    }

    return {
        "metadata": metadata,
        "evidence_chain": {
            "hybrid_validation_metric_gates": {
                "confirmatory_validation_passed": gates_payload.get("confirmatory_validation_passed"),
                "fresh_surface_hybrid_validation_passed": gates_payload.get("fresh_surface_hybrid_validation_passed"),
                "primary_hybrid_material_lift_passed": gates_payload.get("primary_hybrid_material_lift_passed"),
                "recommended_next_stage": gates_payload.get("recommended_next_stage"),
                "primary_deltas_vs_heuristic": primary_deltas if isinstance(primary_deltas, Mapping) else {},
            },
            "fresh_surface_reference": {
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_version": EMBEDDING_VERSION,
                "candidate_pool_work_set_sha256": checks["candidate_pool_work_set_sha256"],
                "candidate_pool_work_count": _get(coverage, "candidate_pool_work_count"),
                "confirmatory_metric_work_count": _get(coverage, "confirmatory_metric_work_count"),
                "confirmatory_positive_work_count": _get(coverage, "confirmatory_positive_work_count"),
                "confirmatory_negative_work_count": _get(coverage, "confirmatory_negative_work_count"),
            },
        },
        "cross_artifact_provenance_checks": checks,
        "scorer_contract": {
            "scorer_id": SCORER_ID,
            "frozen_formula_id": PRIMARY_CONFIRMATORY_ARM,
            "learned_component_source": "audit_embedding_probability_work from frozen ml-offline-audit-embedding-scorer-v2 application, as used in fresh validation",
            "learned_component_scorer_version": AUDIT_SCORER_VERSION,
            "learned_component_fit_mode": scorer_metadata.get("fit_mode") if scorer_metadata else AUDIT_SCORER_FIT_MODE,
            "no_refit_of_learned_scorer": True,
            "no_eval_label_weight_tuning": True,
            "no_label_derived_features": True,
            "production_default_changes_allowed": False,
        },
        "scoring_formula": {
            "formula_id": PRIMARY_CONFIRMATORY_ARM,
            "expression": FROZEN_FORMULA,
            "components": [
                {"name": "final_score_rank_pct", "source": "rank_pct(final_score)", "weight": 0.5},
                {
                    "name": "audit_embedding_probability_rank_pct",
                    "source": "rank_pct(audit_embedding_probability_work)",
                    "weight": 0.5,
                },
            ],
            "scoring_formula_literal": "score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
        },
        "rank_percentile_policy": {
            "same_tie_policy_as": ["ml_hybrid_validation_on_fresh_surface.py", "offline hybrid experiment v1"],
            "higher_raw_score_is_better": True,
            "ties": "average rank",
            "n_equals_1_behavior": "rank_pct = 1.0",
            "otherwise": "rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0))",
            "scope": "full candidate pool for that scoring run",
            "fresh_validation_reference_pool_size": 358,
            "future_shadow_scope_note": "future shadow runs compute rank percentiles within their own full candidate pool",
        },
        "allowed_inputs": list(ALLOWED_INPUTS),
        "forbidden_inputs": list(FORBIDDEN_INPUTS),
        "execution_policy": {
            "shadow_scorer_disabled_by_default": True,
            "future_implementation_write_scope": "isolated shadow/audit outputs only",
            "production_ranking_changes_allowed": False,
            "api_web_changes_allowed": False,
            "bridge_default_changes_allowed": False,
            "production_default_promotion_authorized": False,
            "spec_removes_missing_implementation_blocker": False,
        },
        "observability_requirements": list(OBSERVABILITY_REQUIREMENTS),
        "future_readiness_gates": list(FUTURE_READINESS_GATES),
        "implementation_blockers": {
            "missing_ml_shadow_scorer_v1_implementation": True,
            "shadow_execution_gate_not_written": True,
            "production_default_blocked": True,
            "no_production_model_artifact": True,
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "missing_ml_shadow_scorer_v1_implementation": True,
            "shadow_execution_gate_not_written": True,
            "production_default_blocked": True,
            "no_production_model_artifact": True,
        },
        "scorer_id": SCORER_ID,
        "frozen_formula_id": PRIMARY_CONFIRMATORY_ARM,
        "learned_component_source": "audit_embedding_probability_work from frozen ml-offline-audit-embedding-scorer-v2 application, as used in fresh validation",
        "spec_ready_for_implementation": True,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": "implement_ml_shadow_scorer_v1_disabled_by_default",
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_spec(payload: Mapping[str, Any]) -> str:
    contract = payload["scorer_contract"]
    formula = payload["scoring_formula"]
    rank_policy = payload["rank_percentile_policy"]
    evidence = payload["evidence_chain"]
    lines = [
        f"# ML Shadow Scorer Spec ({payload['metadata']['spec_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact specifies `ml-shadow-scorer-v1` after fresh hybrid validation gates passed. It is a spec only: it does not implement or execute shadow scoring, and it does not authorize production default changes.",
        "",
        f"- Spec ready for implementation: {payload['spec_ready_for_implementation']}",
        f"- Shadow scoring allowed: {payload['shadow_scoring_allowed']}",
        f"- Production default allowed: {payload['production_default_allowed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Evidence Chain",
        "",
        f"- Confirmatory validation passed: {evidence['hybrid_validation_metric_gates']['confirmatory_validation_passed']}",
        f"- Primary material lift passed: {evidence['hybrid_validation_metric_gates']['primary_hybrid_material_lift_passed']}",
        f"- Ranking run: `{evidence['fresh_surface_reference']['ranking_run_id']}` / `{evidence['fresh_surface_reference']['family']}`",
        f"- Snapshot: `{evidence['fresh_surface_reference']['corpus_snapshot_version']}`",
        f"- Embedding version: `{evidence['fresh_surface_reference']['embedding_version']}`",
        f"- Candidate pool SHA: `{evidence['fresh_surface_reference']['candidate_pool_work_set_sha256']}`",
        "",
        "## Frozen Formula",
        "",
        f"- Scorer ID: `{contract['scorer_id']}`",
        f"- Formula ID: `{contract['frozen_formula_id']}`",
        f"- Formula: `{formula['scoring_formula_literal']}`",
        "- No eval-label weight tuning, refit, or label-derived features are allowed.",
        "",
        "## Learned Component Source",
        "",
        f"- {contract['learned_component_source']}.",
        f"- Scorer version: `{contract['learned_component_scorer_version']}`.",
        f"- Fit mode: `{contract['learned_component_fit_mode']}`.",
        "",
        "## Rank Percentile Definition",
        "",
        "- Higher raw score is better.",
        "- Ties use average rank.",
        f"- If n == 1: `{rank_policy['n_equals_1_behavior']}`.",
        f"- Otherwise: `{rank_policy['otherwise']}`.",
        "- Scope is the full candidate pool for the scoring run; future shadow runs compute within their own full candidate pool.",
        "",
        "## Allowed Inputs",
        "",
    ]
    lines.extend(f"- `{item}`" for item in payload["allowed_inputs"])
    lines.extend(["", "## Forbidden Inputs", ""])
    lines.extend(f"- `{item}`" for item in payload["forbidden_inputs"])
    lines.extend(
        [
            "",
            "## Execution Boundaries",
            "",
            "- Shadow scorer must be disabled by default.",
            "- Future implementation may only write isolated shadow/audit outputs.",
            "- Existing production ranking, API behavior, bridge defaults, and public UI remain unchanged.",
            "- No production default promotion is authorized by this spec.",
            "",
            "## Observability",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["observability_requirements"])
    lines.extend(["", "## Future Gates", ""])
    lines.extend(f"- {item}" for item in payload["future_readiness_gates"])
    lines.extend(
        [
            "",
            "## Not Shadow Execution / Not Production",
            "",
            "- This spec does not authorize shadow execution.",
            "- This spec does not authorize production default, API, web, or model deployment changes.",
            "- The missing implementation blocker remains until a future disabled-by-default implementation artifact exists.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_spec(
    *,
    hybrid_validation_metric_gates_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    audit_embedding_scorer_export_path: Path | None = None,
    hybrid_experiment_spec_path: Path | None = None,
    spec_version: str = SPEC_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_spec_payload(
        hybrid_validation_metric_gates_path=hybrid_validation_metric_gates_path,
        hybrid_validation_on_fresh_surface_path=hybrid_validation_on_fresh_surface_path,
        fresh_eval_surface_path=fresh_eval_surface_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        audit_embedding_scorer_export_path=audit_embedding_scorer_export_path,
        hybrid_experiment_spec_path=hybrid_experiment_spec_path,
        spec_version=spec_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_spec(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "SPEC_VERSION",
    "MLShadowScorerSpecError",
    "build_ml_shadow_scorer_spec_payload",
    "markdown_from_ml_shadow_scorer_spec",
    "write_ml_shadow_scorer_spec",
]
