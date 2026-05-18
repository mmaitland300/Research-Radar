"""Fresh eval surface policy for hybrid validation.

This module writes a policy artifact only. It reads existing JSON/Markdown
evidence and defines what counts as a fresh confirmatory surface for hybrid
validation. It does not query databases, run ranking, train, score, generate
embeddings, import labels, or authorize shadow/prod.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
HYBRID_GATES_ARTIFACT_TYPE = "ml_hybrid_scorer_metric_gates"
HYBRID_GATES_VERSION = "ml-hybrid-scorer-metric-gates-v1"
HYBRID_EXPERIMENT_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment"
HYBRID_EXPERIMENT_VERSION = "ml-hybrid-scorer-offline-experiment-v1"
HYBRID_SPEC_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment_spec"
HYBRID_SPEC_VERSION = "ml-hybrid-scorer-offline-experiment-v1-spec"
SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
SCORING_VERSION = "ml-offline-production-candidate-scoring-v3"
HOLDOUT_ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"
RECOMMENDED_NEXT_STAGE = "create_fresh_eval_surface_for_hybrid_validation_v1"

MINIMUM_LABEL_THRESHOLDS: dict[str, float | int] = {
    "minimum_candidate_work_count": 100,
    "minimum_confirmatory_labeled_work_count": 100,
    "minimum_confirmatory_positive_work_count": 50,
    "minimum_confirmatory_negative_work_count": 20,
    "minimum_confirmatory_label_coverage_rate": 0.60,
    "minimum_distinct_negative_work_count": 20,
}
CAVEATS = (
    "Policy only; no fresh surface materialized yet.",
    "Not live recommender validation.",
    "Hybrid lift so far is exploratory on an already-seen surface.",
    "Single-reviewer audit labels remain a limitation.",
    "Future surface must satisfy label coverage and negative-count thresholds.",
    "No shadow or production authorization.",
)


class MLFreshEvalSurfacePolicyHybridError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshEvalSurfacePolicyHybridError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshEvalSurfacePolicyHybridError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshEvalSurfacePolicyHybridError(f"{name} JSON missing metadata object")
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
        raise MLFreshEvalSurfacePolicyHybridError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_hybrid_metric_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-metric-gates")
    if metadata.get("artifact_type") != HYBRID_GATES_ARTIFACT_TYPE:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected hybrid gates metadata.artifact_type={HYBRID_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != HYBRID_GATES_VERSION:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected hybrid gates metadata.gates_version={HYBRID_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("hybrid_material_lift_passed") is not True:
        raise MLFreshEvalSurfacePolicyHybridError("hybrid gates hybrid_material_lift_passed must be true")
    if payload.get("confirmatory_validation_passed") is not False:
        raise MLFreshEvalSurfacePolicyHybridError("hybrid gates confirmatory_validation_passed must be false")
    if payload.get("recommended_next_stage") != RECOMMENDED_NEXT_STAGE:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"hybrid gates recommended_next_stage must be {RECOMMENDED_NEXT_STAGE!r}"
        )
    if payload.get("shadow_scoring_allowed") is not False:
        raise MLFreshEvalSurfacePolicyHybridError("hybrid gates shadow_scoring_allowed must be false")
    if payload.get("production_default_allowed") is not False:
        raise MLFreshEvalSurfacePolicyHybridError("hybrid gates production_default_allowed must be false")
    return metadata


def _validate_hybrid_experiment(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-experiment")
    if metadata.get("artifact_type") != HYBRID_EXPERIMENT_ARTIFACT_TYPE:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected hybrid experiment metadata.artifact_type={HYBRID_EXPERIMENT_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != HYBRID_EXPERIMENT_VERSION:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected hybrid experiment metadata.experiment_version={HYBRID_EXPERIMENT_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if _get(payload, "summary.best_arm_selection_is_exploratory_only") is not True:
        raise MLFreshEvalSurfacePolicyHybridError(
            "hybrid experiment summary.best_arm_selection_is_exploratory_only must be true"
        )
    if _get(payload, "summary.hybrid_material_lift_passed") is not True:
        raise MLFreshEvalSurfacePolicyHybridError("hybrid experiment summary.hybrid_material_lift_passed must be true")
    return metadata


def _validate_hybrid_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-experiment-spec")
    if metadata.get("artifact_type") != HYBRID_SPEC_ARTIFACT_TYPE:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected hybrid spec metadata.artifact_type={HYBRID_SPEC_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("spec_version") != HYBRID_SPEC_VERSION:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected hybrid spec metadata.spec_version={HYBRID_SPEC_VERSION!r}, got {metadata.get('spec_version')!r}"
        )
    return metadata


def _validate_scoring(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if not str(metadata.get("eval_work_set_sha256") or "").strip():
        raise MLFreshEvalSurfacePolicyHybridError("scoring metadata.eval_work_set_sha256 must be present")
    return metadata


def _validate_assignment(payload: Mapping[str, Any], *, scoring_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("assignment_version") != HOLDOUT_ASSIGNMENT_VERSION:
        raise MLFreshEvalSurfacePolicyHybridError(
            f"expected assignment metadata.assignment_version={HOLDOUT_ASSIGNMENT_VERSION!r}, got {metadata.get('assignment_version')!r}"
        )
    if metadata.get("eval_work_set_sha256") != scoring_eval_sha:
        raise MLFreshEvalSurfacePolicyHybridError("assignment metadata.eval_work_set_sha256 must match scoring")
    return metadata


def _candidate_count(scoring_payload: Mapping[str, Any], assignment_metadata: Mapping[str, Any]) -> int | None:
    value = _get(scoring_payload, "candidate_pool_summary.candidate_unique_canonical_work_count")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    assignment_count = assignment_metadata.get("eval_work_count")
    if isinstance(assignment_count, int) and not isinstance(assignment_count, bool):
        return assignment_count
    return None


def _ranking_run_id(scoring_metadata: Mapping[str, Any], scoring_payload: Mapping[str, Any]) -> str | None:
    value = scoring_metadata.get("ranking_run_id") or _get(scoring_payload, "candidate_pool_definition.ranking_run_id")
    return str(value) if value is not None else None


def _family(scoring_metadata: Mapping[str, Any], scoring_payload: Mapping[str, Any]) -> str | None:
    value = scoring_metadata.get("family") or _get(scoring_payload, "candidate_pool_definition.family")
    return str(value) if value is not None else None


def _surface_id(ranking_run_id: str | None, family: str | None) -> str:
    rank_part = ranking_run_id or "unknown-ranking-run"
    family_part = family or "unknown-family"
    return f"product_candidate_eval_surface_{rank_part}_{family_part}_v3"


def _purpose() -> dict[str, Any]:
    return {
        "summary": "This policy defines a confirmatory validation surface for hybrid scorer v1.",
        "separates_exploratory_from_confirmatory": (
            "Exploratory lift on the already-seen 217-work surface is not future confirmatory evidence."
        ),
        "does_not_run": ["ranking", "scoring", "training", "labeling"],
    }


def _disallowed_surfaces(
    *,
    scoring_metadata: Mapping[str, Any],
    scoring_payload: Mapping[str, Any],
    assignment_metadata: Mapping[str, Any],
    eval_sha: str,
) -> list[dict[str, Any]]:
    ranking_run_id = _ranking_run_id(scoring_metadata, scoring_payload)
    family = _family(scoring_metadata, scoring_payload)
    return [
        {
            "surface_id": _surface_id(ranking_run_id, family),
            "eval_work_set_sha256": eval_sha,
            "candidate_work_count": _candidate_count(scoring_payload, assignment_metadata),
            "ranking_run_id": ranking_run_id,
            "family": family,
            "source_artifacts": [
                "ml-offline-production-candidate-scoring-v3.json",
                "ml-learned-scorer-holdout-assignment-v1.json",
                "ml-hybrid-scorer-offline-experiment-v1.json",
                "ml-hybrid-scorer-metric-gates-v1.json",
            ],
            "confirmatory_use": "disallowed",
            "allowed_use": "regression_smoke_or_historical_comparison_only",
            "freshness_rule": {
                "old_canonical_works_confirmatory_metric_denominator_allowed": False,
                "future_candidate_pool_may_contain_overlaps_for_operational_visibility": True,
                "overlap_tag": "previous_eval_overlap",
                "overlap_confirmatory_metric_denominator_allowed": False,
                "future_artifacts_must_report": [
                    "overlap_work_count",
                    "excluded_previous_eval_overlap_count",
                ],
            },
        }
    ]


def _fresh_surface_definition(eval_sha: str) -> dict[str, Any]:
    return {
        "primary_strategy": {
            "strategy_id": "new_snapshot_new_product_candidate_run",
            "status": "selected_for_v1",
            "requirements": [
                "Future materialization uses a new corpus snapshot and a new product-candidate ranking/scoring run produced after this policy.",
                "The materialized candidate work-set SHA must differ from the disallowed eval_work_set_sha256.",
                "Confirmatory metric rows must be canonical works not in the disallowed 217-work set.",
                "The surface must be frozen into an artifact before any hybrid validation scoring runs.",
            ],
            "disallowed_eval_work_set_sha256": eval_sha,
        },
        "secondary_alternatives": [
            {
                "strategy_id": "time_sliced_product_candidate_pool",
                "status": "future_alternative_only",
            },
            {
                "strategy_id": "separately_labeled_product_like_pool",
                "status": "future_alternative_only",
            },
        ],
    }


def _candidate_source_policy() -> dict[str, Any]:
    return {
        "primary_candidate_source_requirements": [
            "product-candidate style pool",
            "ranking_run_id or surface_id recorded",
            "family or candidate family recorded",
            "candidate acquisition timestamp / snapshot id recorded",
            "candidate work-set SHA recorded",
            "no silent candidate filtering after labels are inspected",
        ],
        "label_blind_candidate_filtering_required": True,
    }


def _canonical_grouping_policy() -> dict[str, Any]:
    return {
        "grouping_key": "canonical_openalex_work_id",
        "normalization_rules": [
            "normalize W tokens to uppercase W token",
            "normalize https://openalex.org/W... URLs to uppercase W token",
        ],
        "all_observations_for_same_canonical_work_share_one_metric_group": True,
        "duplicate_conflicting_observations_preserved_and_reported": True,
        "row_id_level_metric_splitting_allowed": False,
    }


def _label_policy(*, conflict_policy_record: Mapping[str, str]) -> dict[str, Any]:
    return {
        "conflict_policy_path": conflict_policy_record["path"],
        "conflict_policy_sha256": conflict_policy_record["sha256"],
        "labels_remain_observation_level": True,
        "silent_merge_of_conflicting_labels_allowed": False,
        "reviewer_notes_as_labels_or_features_allowed": False,
        "label_inference_from_rank_score_sample_reason_or_source_pool_allowed": False,
        "minimum_confirmatory_label_thresholds": dict(MINIMUM_LABEL_THRESHOLDS),
        "threshold_application_rules": [
            "Thresholds apply after excluding old 217-work overlap from confirmatory metric denominators.",
            "If thresholds are not met, future gates must route to labeling or new surface collection, not shadow.",
        ],
    }


def _frozen_hybrid_arms() -> dict[str, Any]:
    return {
        "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
        "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
        "baselines_for_future_comparison": [
            "heuristic_final_score_baseline",
            "holdout_embedding_probability_baseline",
        ],
        "rules": [
            "No re-tuning weights on the fresh surface.",
            "No selecting a new best arm on fresh labels and calling it confirmatory.",
            "The primary confirmatory decision should be based on hybrid_rank_mean_50_50 unless a later policy version changes this before scoring.",
        ],
    }


def _gate_linkage() -> dict[str, Any]:
    return {
        "inherits_from": HYBRID_GATES_VERSION,
        "material_lift_thresholds": {
            "delta_roc_auc_gte": 0.03,
            "or_delta_average_precision_gte": 0.02,
        },
        "top_k_precision_policy": "advisory when saturated",
        "best_arm_on_seen_eval_is_exploratory_only": True,
        "must_be_rerun_on_fresh_surface": [
            "candidate coverage",
            "label coverage",
            "class balance",
            "leakage/overlap checks",
            "heuristic metrics",
            "learned metrics",
            "hybrid metrics",
            "material lift",
            "confirmatory_validation_passed",
        ],
        "must_not_be_inherited_from_old_surface": [
            "hybrid_material_lift_passed",
            "best arm metrics",
            "ROC-AUC/AP values",
            "top-k values",
            "confirmatory validation",
        ],
    }


def _future_artifact_contract() -> dict[str, Any]:
    return {
        "materialization_command": "ml-fresh-eval-surface-hybrid-materialize",
        "expected_outputs": [
            "docs/audit/ml-fresh-eval-surface-hybrid-v1.json",
            "docs/audit/ml-fresh-eval-surface-hybrid-v1.md",
        ],
        "materialized_surface_must_include": [
            "surface_id",
            "candidate_source",
            "snapshot id / ranking run id",
            "candidate work set SHA",
            "disallowed overlap report",
            "confirmatory_metric_eligible_work_count",
            "label coverage status",
            "blocked/unblocked status for scoring",
        ],
        "future_scoring_command": "ml-hybrid-validation-on-fresh-surface",
        "future_gates_command": "ml-hybrid-validation-metric-gates",
    }


def build_ml_fresh_eval_surface_policy_hybrid_payload(
    *,
    hybrid_metric_gates_path: Path,
    hybrid_experiment_path: Path,
    hybrid_experiment_spec_path: Path,
    production_candidate_scoring_path: Path,
    holdout_assignment_path: Path,
    conflict_policy_path: Path,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    gates_path = Path(hybrid_metric_gates_path).resolve()
    experiment_path = Path(hybrid_experiment_path).resolve()
    spec_path = Path(hybrid_experiment_spec_path).resolve()
    scoring_path = Path(production_candidate_scoring_path).resolve()
    assignment_path = Path(holdout_assignment_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    gates_payload = _load_json_object(gates_path)
    experiment_payload = _load_json_object(experiment_path)
    spec_payload = _load_json_object(spec_path)
    scoring_payload = _load_json_object(scoring_path)
    assignment_payload = _load_json_object(assignment_path)

    gates_metadata = _validate_hybrid_metric_gates(gates_payload)
    experiment_metadata = _validate_hybrid_experiment(experiment_payload)
    spec_metadata = _validate_hybrid_spec(spec_payload)
    scoring_metadata = _validate_scoring(scoring_payload)
    eval_sha = str(scoring_metadata["eval_work_set_sha256"])
    assignment_metadata = _validate_assignment(assignment_payload, scoring_eval_sha=eval_sha)

    for name, metadata in (
        ("hybrid gates", gates_metadata),
        ("hybrid experiment", experiment_metadata),
        ("hybrid spec", spec_metadata),
    ):
        metadata_eval_sha = metadata.get("eval_work_set_sha256")
        if metadata_eval_sha is not None and metadata_eval_sha != eval_sha:
            raise MLFreshEvalSurfacePolicyHybridError(f"{name} eval_work_set_sha256 must match scoring")

    inputs = [
        _input_record("hybrid_metric_gates", gates_path, repo_root=root),
        _input_record("hybrid_experiment", experiment_path, repo_root=root),
        _input_record("hybrid_experiment_spec", spec_path, repo_root=root),
        _input_record("production_candidate_scoring", scoring_path, repo_root=root),
        _input_record("holdout_assignment", assignment_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    conflict_record = inputs[-1]
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "policy_version": policy_version,
        "generated_at": generated_at or _now_iso_z(),
        "status": "blocked_for_confirmatory_until_surface_materialized",
        "inputs": inputs,
        "source_hybrid_metric_gates_version": gates_metadata.get("gates_version"),
        "source_hybrid_experiment_version": experiment_metadata.get("experiment_version"),
        "source_hybrid_experiment_spec_version": spec_metadata.get("spec_version"),
        "source_product_candidate_scoring_version": scoring_metadata.get("experiment_version"),
        "holdout_assignment_version": assignment_metadata.get("assignment_version"),
        "disallowed_eval_work_set_sha256": eval_sha,
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "purpose": _purpose(),
        "disallowed_surfaces": _disallowed_surfaces(
            scoring_metadata=scoring_metadata,
            scoring_payload=scoring_payload,
            assignment_metadata=assignment_metadata,
            eval_sha=eval_sha,
        ),
        "fresh_surface_definition": _fresh_surface_definition(eval_sha),
        "candidate_source_policy": _candidate_source_policy(),
        "canonical_grouping_policy": _canonical_grouping_policy(),
        "label_policy": _label_policy(conflict_policy_record=conflict_record),
        "frozen_hybrid_arms": _frozen_hybrid_arms(),
        "gate_linkage": _gate_linkage(),
        "allowed_next_stages": [
            "materialize_fresh_eval_surface_hybrid_v1",
            "create_fresh_eval_labeling_plan_if_thresholds_not_met",
            "execute_hybrid_validation_on_fresh_surface_v1",
            "run_hybrid_validation_metric_gates_v1",
        ],
        "blocked_actions": [
            "shadow_scoring",
            "production_default_change",
            "bridge/default ranking changes",
            "public copy claiming production readiness",
            "model/scorer deployment",
            "silent label conflict resolution",
            "using old 217-work surface for confirmatory hybrid validation",
        ],
        "future_artifact_contract": _future_artifact_contract(),
        "policy_assertions": {
            "old_217_surface_confirmatory_reuse_allowed": False,
            "old_217_surface_regression_smoke_allowed": True,
            "requires_canonical_work_grouping": True,
            "requires_overlap_exclusion_from_confirmatory_metrics": True,
            "frozen_primary_hybrid_arm": "hybrid_rank_mean_50_50",
            "retuning_on_fresh_eval_labels_allowed": False,
            "shadow_allowed_by_this_policy": False,
            "production_default_allowed_by_this_policy": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_fresh_eval_surface_policy_hybrid(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    disallowed = payload["disallowed_surfaces"][0]
    label_thresholds = payload["label_policy"]["minimum_confirmatory_label_thresholds"]
    frozen = payload["frozen_hybrid_arms"]
    lines = [
        f"# Fresh Eval Surface Policy For Hybrid Validation ({metadata['policy_version']})",
        "",
        "## Executive Summary",
        "",
        "This policy defines what counts as a genuinely fresh product-candidate eval surface for confirming the hybrid scorer result. It does not materialize a pool, score candidates, train, label, authorize shadow, or authorize production.",
        "",
        f"- **Status:** `{metadata['status']}`",
        f"- **Disallowed eval work-set SHA:** `{metadata['disallowed_eval_work_set_sha256']}`",
        f"- **Primary fresh-surface strategy:** `{payload['fresh_surface_definition']['primary_strategy']['strategy_id']}`",
        f"- **Frozen primary hybrid arm:** `{frozen['primary_confirmatory_arm']}`",
        "",
        "## Why A Fresh Surface Is Required",
        "",
        "Hybrid scorer metric gates v1 showed material lift on an already-observed 217-work eval surface, but confirmatory validation remains false. The old 217-work surface cannot be used as confirmatory evidence.",
        "",
        "## Disallowed 217-Work Surface Details",
        "",
        "| Field | Value |",
        "| --- | --- |",
        f"| Surface ID | `{disallowed['surface_id']}` |",
        f"| Candidate works | {disallowed['candidate_work_count']} |",
        f"| Eval work-set SHA | `{disallowed['eval_work_set_sha256']}` |",
        f"| Ranking run | `{disallowed['ranking_run_id']}` |",
        f"| Family | `{disallowed['family']}` |",
        f"| Confirmatory use | `{disallowed['confirmatory_use']}` |",
        f"| Allowed use | `{disallowed['allowed_use']}` |",
        "",
        "Overlapping works in a future pool must be tagged `previous_eval_overlap` and excluded from confirmatory metric denominators.",
        "",
        "## Primary Fresh-Surface Path",
        "",
    ]
    lines.extend(f"- {item}" for item in payload["fresh_surface_definition"]["primary_strategy"]["requirements"])
    lines.extend(
        [
            "",
            "## Label Thresholds",
            "",
            "| Threshold | Value |",
            "| --- | ---: |",
        ]
    )
    for key, value in label_thresholds.items():
        lines.append(f"| `{key}` | {value} |")
    lines.extend(
        [
            "",
            "Thresholds apply after excluding old 217-work overlap from confirmatory metric denominators.",
            "",
            "## Frozen Hybrid Arms",
            "",
            f"- Primary confirmatory arm: `{frozen['primary_confirmatory_arm']}`",
            f"- Secondary reporting arm: `{frozen['secondary_reporting_arm']}`",
            f"- Baselines: {', '.join(f'`{item}`' for item in frozen['baselines_for_future_comparison'])}",
            "- No re-tuning weights on fresh labels.",
            "- No selecting a new best arm on fresh labels and calling it confirmatory.",
            "",
            "## Gate Linkage",
            "",
            f"- Inherits material lift thresholds from `{payload['gate_linkage']['inherits_from']}`.",
            "- Material lift remains ROC-AUC delta >= 0.03 OR AP delta >= 0.02.",
            "- Candidate coverage, label coverage, class balance, overlap checks, metrics, material lift, and confirmatory validation must be rerun on the fresh surface.",
            "- Old-surface lift, best-arm metrics, ROC-AUC/AP values, top-k values, and confirmatory validation must not be inherited.",
            "",
            "## Blocked Actions",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["blocked_actions"])
    lines.extend(
        [
            "",
            "## Future Artifact Chain",
            "",
            f"- Materialization command: `{payload['future_artifact_contract']['materialization_command']}`",
            "- Outputs: `docs/audit/ml-fresh-eval-surface-hybrid-v1.json`, `docs/audit/ml-fresh-eval-surface-hybrid-v1.md`",
            f"- Future scoring command: `{payload['future_artifact_contract']['future_scoring_command']}`",
            f"- Future gates command: `{payload['future_artifact_contract']['future_gates_command']}`",
            "",
            "## Not Shadow / Not Production Caveats",
            "",
            f"- Shadow allowed by this policy: {payload['policy_assertions']['shadow_allowed_by_this_policy']}",
            f"- Production default allowed by this policy: {payload['policy_assertions']['production_default_allowed_by_this_policy']}",
            f"- Confirmatory validation complete: {payload['policy_assertions']['confirmatory_validation_complete']}",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_eval_surface_policy_hybrid(
    *,
    hybrid_metric_gates_path: Path,
    hybrid_experiment_path: Path,
    hybrid_experiment_spec_path: Path,
    production_candidate_scoring_path: Path,
    holdout_assignment_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_fresh_eval_surface_policy_hybrid_payload(
        hybrid_metric_gates_path=hybrid_metric_gates_path,
        hybrid_experiment_path=hybrid_experiment_path,
        hybrid_experiment_spec_path=hybrid_experiment_spec_path,
        production_candidate_scoring_path=production_candidate_scoring_path,
        holdout_assignment_path=holdout_assignment_path,
        conflict_policy_path=conflict_policy_path,
        policy_version=policy_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_eval_surface_policy_hybrid(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "POLICY_VERSION",
    "MLFreshEvalSurfacePolicyHybridError",
    "build_ml_fresh_eval_surface_policy_hybrid_payload",
    "markdown_from_ml_fresh_eval_surface_policy_hybrid",
    "write_ml_fresh_eval_surface_policy_hybrid",
]
