"""Fresh candidate source expansion plan for hybrid validation.

This module writes a policy/spec artifact only. It reads existing JSON and
Markdown inputs and defines how to move from the best existing 44-work fresh
surface to a future materializable source with at least 100 fresh eligible
works. It does not query databases, write rankings, score, train, generate
embeddings, import labels, or authorize shadow/prod.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_candidate_source_expansion_plan"
PLAN_VERSION = "ml-fresh-candidate-source-expansion-plan-v1"
RANKING_SOURCE_ARTIFACT_TYPE = "ml_fresh_product_candidate_ranking_source"
RANKING_SOURCE_VERSION = "ml-fresh-product-candidate-ranking-source-v1"
LABELING_PLAN_ARTIFACT_TYPE = "ml_fresh_eval_labeling_plan_hybrid"
LABELING_PLAN_VERSION = "ml-fresh-eval-labeling-plan-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
DISALLOWED_OLD_EVAL_SHA = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
BEST_EXISTING_UNDERPOWERED_SHA = "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926"
TARGET = "good_or_acceptable"

CAVEATS = (
    "Plan only; no candidate source is built in this artifact.",
    "Not hybrid validation and not live recommender validation.",
    "No database access, ranking creation, scoring, training, embeddings, or label import.",
    "Existing 44-work surface is not confirmatory-ready and must not be used for hybrid validation.",
    "Policy thresholds are not lowered by this plan.",
    "No shadow or production authorization.",
)


class MLFreshCandidateSourceExpansionPlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshCandidateSourceExpansionPlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshCandidateSourceExpansionPlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshCandidateSourceExpansionPlanError(f"{name} JSON missing metadata object")
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
        raise MLFreshCandidateSourceExpansionPlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_ranking_source(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-product-candidate-ranking-source")
    if metadata.get("artifact_type") != RANKING_SOURCE_ARTIFACT_TYPE:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected ranking source metadata.artifact_type={RANKING_SOURCE_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("source_version") != RANKING_SOURCE_VERSION:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected ranking source metadata.source_version={RANKING_SOURCE_VERSION!r}, got {metadata.get('source_version')!r}"
        )
    if _get(payload, "source_selection.status") != "blocked_no_source_meets_candidate_threshold":
        raise MLFreshCandidateSourceExpansionPlanError(
            "ranking source source_selection.status must be blocked_no_source_meets_candidate_threshold"
        )
    if _get(payload, "source_selection.selected_source") is not None:
        raise MLFreshCandidateSourceExpansionPlanError("ranking source source_selection.selected_source must be null")
    if _get(payload, "source_selection.recommended_next_stage") != "create_new_or_larger_candidate_snapshot":
        raise MLFreshCandidateSourceExpansionPlanError(
            "ranking source source_selection.recommended_next_stage must be create_new_or_larger_candidate_snapshot"
        )
    return metadata


def _validate_labeling_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-labeling-plan")
    if metadata.get("artifact_type") != LABELING_PLAN_ARTIFACT_TYPE:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected labeling plan metadata.artifact_type={LABELING_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != LABELING_PLAN_VERSION:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected labeling plan metadata.plan_version={LABELING_PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    if payload.get("recommended_next_stage") != "create_fresh_product_candidate_ranking_source_v1":
        raise MLFreshCandidateSourceExpansionPlanError(
            "labeling plan recommended_next_stage must be create_fresh_product_candidate_ranking_source_v1"
        )
    if _get(payload, "blocking_diagnosis.current_surface_can_be_made_ready_by_labeling_alone") is not False:
        raise MLFreshCandidateSourceExpansionPlanError(
            "labeling plan must state current_surface_can_be_made_ready_by_labeling_alone is false"
        )
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshCandidateSourceExpansionPlanError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )


def _best_source(ranking_source_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    sources = ranking_source_payload.get("candidate_sources_considered")
    if not isinstance(sources, list) or not sources:
        raise MLFreshCandidateSourceExpansionPlanError("ranking source candidate_sources_considered must be non-empty")
    first = sources[0]
    if not isinstance(first, Mapping):
        raise MLFreshCandidateSourceExpansionPlanError("ranking source best candidate source must be an object")
    return first


def _minimum_candidate_works(ranking_source_payload: Mapping[str, Any], policy_payload: Mapping[str, Any]) -> int:
    value = _get(ranking_source_payload, "source_selection.minimum_confirmatory_candidate_works")
    if value is None:
        value = _get(policy_payload, "label_policy.minimum_confirmatory_label_thresholds.minimum_candidate_work_count")
    if not isinstance(value, int) or isinstance(value, bool):
        raise MLFreshCandidateSourceExpansionPlanError("minimum_confirmatory_candidate_works must be an integer")
    return value


def _frozen_arms(policy_payload: Mapping[str, Any]) -> dict[str, Any]:
    arms = policy_payload.get("frozen_hybrid_arms")
    if not isinstance(arms, Mapping):
        arms = {}
    return {
        "primary_confirmatory_arm": arms.get("primary_confirmatory_arm") or "hybrid_rank_mean_50_50",
        "secondary_reporting_arm": arms.get("secondary_reporting_arm") or "hybrid_rank_mean_25_75_heuristic",
        "baselines_for_future_comparison": arms.get("baselines_for_future_comparison")
        or ["heuristic_final_score_baseline", "holdout_embedding_probability_baseline"],
        "no_retuning": True,
    }


def _label_summary(source: Mapping[str, Any]) -> Mapping[str, Any]:
    summary = source.get("label_coverage_summary")
    if not isinstance(summary, Mapping):
        raise MLFreshCandidateSourceExpansionPlanError("best source missing label_coverage_summary")
    return summary


def build_ml_fresh_candidate_source_expansion_plan_payload(
    *,
    fresh_product_candidate_ranking_source_path: Path,
    fresh_eval_labeling_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path | None = None,
    markdown_output_path: Path | None = None,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    ranking_path = Path(fresh_product_candidate_ranking_source_path).resolve()
    labeling_path = Path(fresh_eval_labeling_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    ranking_payload = _load_json_object(ranking_path)
    labeling_payload = _load_json_object(labeling_path)
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(label_path)

    ranking_metadata = _validate_ranking_source(ranking_payload)
    labeling_metadata = _validate_labeling_plan(labeling_payload)
    policy_metadata = _validate_policy(policy_payload)
    _validate_label_dataset(label_payload)
    if not conflict_path.exists():
        raise MLFreshCandidateSourceExpansionPlanError(f"conflict policy does not exist: {conflict_path}")

    inputs = [
        _input_record("fresh_product_candidate_ranking_source", ranking_path, repo_root=root),
        _input_record("fresh_eval_labeling_plan", labeling_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]

    best = _best_source(ranking_payload)
    labels = _label_summary(best)
    minimum_candidate_works = _minimum_candidate_works(ranking_payload, policy_payload)
    best_eligible = int(best.get("confirmatory_eligible_work_count") or 0)
    candidate_gap = max(0, minimum_candidate_works - best_eligible)
    labeled_abs_deficit = int(labeling_payload.get("labeled_work_absolute_deficit") or 0)
    coverage_only_deficit = int(labeling_payload.get("coverage_only_deficit_at_current_eligible_count") or 0)
    negative_deficit = int(_get(labeling_payload, "recommended_collection_plan.minimum_negative_works_needed") or 0)
    distinct_negative_deficit = int(_get(labeling_payload, "recommended_collection_plan.minimum_distinct_negative_works_needed") or negative_deficit)
    frozen_arms = _frozen_arms(policy_payload)

    current_blocker_summary = {
        "sources_considered_count": len(ranking_payload.get("candidate_sources_considered") or []),
        "best_source_ranking_run_id": best.get("ranking_run_id"),
        "best_source_confirmatory_eligible_work_count": best_eligible,
        "best_source_candidate_work_set_sha256": best.get("candidate_work_set_sha256"),
        "minimum_confirmatory_candidate_works": minimum_candidate_works,
        "candidate_gap": candidate_gap,
        "best_source_overlap_with_old_217": best.get("overlap_with_old_217_count"),
        "best_source_label_snapshot": {
            "labeled_work_count": labels.get("labeled_work_count"),
            "positive_labeled_work_count": labels.get("positive_labeled_work_count"),
            "negative_labeled_work_count": labels.get("negative_labeled_work_count"),
            "distinct_negative_work_count": labels.get("distinct_negative_work_count"),
            "label_coverage_rate": labels.get("label_coverage_rate"),
        },
        "labeled_work_absolute_deficit_after_expansion": labeled_abs_deficit,
        "coverage_only_labeled_deficit_on_current_44": coverage_only_deficit,
        "negative_work_deficit": negative_deficit,
        "distinct_negative_work_deficit": distinct_negative_deficit,
        "current_surface_can_be_made_ready_by_labeling_alone": False,
        "why_not_hybrid_validation": "No source has >=100 fresh eligible works after old-217 exclusion; best source has 0 negatives.",
        "recommended_next_stage": "implement_or_run_fresh_product_candidate_source_build_v1",
    }

    source_expansion_requirements = {
        "new_candidate_work_set_sha256_must_differ_from": [
            BEST_EXISTING_UNDERPOWERED_SHA,
            DISALLOWED_OLD_EVAL_SHA,
        ],
        "minimum_confirmatory_eligible_work_count": minimum_candidate_works,
        "exclude_old_217_confirmatory_surface": True,
        "old_eval_work_set_sha256": DISALLOWED_OLD_EVAL_SHA,
        "canonical_openalex_work_id_required": True,
        "candidate_source_must_be_frozen_before_scoring": True,
        "overlap_with_old_217_must_be_tagged_and_excluded_from_confirmatory_denominators": True,
        "new_candidate_work_set_sha256_must_differ_from_best_existing_1a62e980_and_disallowed_21398640": True,
        "no_hybrid_validation_until_materialized_ready": True,
        "frozen_primary_hybrid_arm": frozen_arms["primary_confirmatory_arm"],
        "frozen_secondary_reporting_arm": frozen_arms["secondary_reporting_arm"],
    }

    allowed_expansion_strategies = [
        {
            "strategy_id": "create_newer_corpus_snapshot_and_candidate_run",
            "description": "Preferred. New snapshot plus product-candidate ranking/scoring run after this plan; freeze before materialize.",
            "priority": "primary",
        },
        {
            "strategy_id": "broaden_existing_snapshot_candidate_pool",
            "description": "Expand product-candidate selection within newer or broader snapshot filters while preserving product-like scope.",
            "priority": "secondary",
        },
        {
            "strategy_id": "targeted_negative_candidate_collection",
            "description": "Intentionally add product-plausible borderline/negative candidates for label balance; separate from hybrid scoring/training.",
            "priority": "required_for_label_readiness",
        },
        {
            "strategy_id": "multi_family_candidate_source_with_declared_family_rules",
            "description": "Allowed only if family rules are explicit and materializer reports per-family breakdown.",
            "priority": "optional",
        },
    ]

    forbidden_expansion_strategies = [
        "reusing old 217 eval surface as confirmatory denominator (rank-ee2ba6c816 / SHA 21398640...)",
        "selecting rank-3904fec89d / 44-work surface as confirmatory-ready without expansion",
        "scoring hybrid arms before source materialization",
        "lowering policy thresholds to make current 44 pass",
        "post-hoc cherry-picking candidates by existing labels",
        "DB writes in this planning command",
        "shadow_scoring",
        "production_default_change",
        "bridge_default_change",
        "claiming confirmatory_validation_complete",
    ]

    candidate_generation_contract = {
        "future_command_name": "ml-fresh-product-candidate-source-build",
        "future_commit_scope": "may use DB writes and ranking/candidate pipeline (first implementation leaving pure eval specs)",
        "must_emit": [
            "frozen source artifact or ranking_run_id",
            "snapshot version",
            "family",
            "query/SQL provenance",
            "candidate counts",
            "canonical candidate_work_set_sha256",
        ],
        "must_rerun": [
            "ml-fresh-product-candidate-ranking-source",
            "ml-fresh-eval-surface-hybrid-materialize with --ranking-run-id when supported",
        ],
        "success_criteria": "confirmatory_eligible_work_count >= 100 after old-217 exclusion",
        "on_success_recommended_next_stage": "rerun_fresh_product_candidate_ranking_source_after_source_build",
        "on_failure_recommended_next_stage": "revise_corpus_scope_or_candidate_filters",
    }

    labeling_implications = {
        "current_best_source_labels_are_positive_only": True,
        "current_best_source_label_snapshot": current_blocker_summary["best_source_label_snapshot"],
        "labeling_worksheet_for_confirmatory_path_should_wait_until_candidate_source_plausibly_meets_100_eligible": True,
        "optional_partial_labeling_on_current_44_allowed_only_if_marked_partial_non_confirmatory": True,
        "future_source_build_must_include_negative_or_borderline_sampling_plan_before_confirmatory_hybrid_validation": True,
        "absolute_labeled_minimum_100_remains_policy_requirement_after_expansion": True,
        "negative_work_minimum_20_remains_policy_requirement_after_expansion": True,
        "labeled_work_absolute_deficit_after_expansion": labeled_abs_deficit,
        "coverage_only_labeled_deficit_on_current_44": coverage_only_deficit,
        "negative_work_deficit": negative_deficit,
        "distinct_negative_work_deficit": distinct_negative_deficit,
    }

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "source_ranking_artifact_version": ranking_metadata.get("source_version"),
            "source_labeling_plan_version": labeling_metadata.get("plan_version"),
            "fresh_surface_policy_version": policy_metadata.get("policy_version"),
            "label_dataset_version": label_payload.get("dataset_version"),
            "conflict_policy_sha256": inputs[-1]["sha256"],
            "caveats": list(CAVEATS),
        },
        "current_blocker_summary": current_blocker_summary,
        "source_expansion_requirements": source_expansion_requirements,
        "allowed_expansion_strategies": allowed_expansion_strategies,
        "forbidden_expansion_strategies": forbidden_expansion_strategies,
        "candidate_generation_contract": candidate_generation_contract,
        "labeling_implications": labeling_implications,
        "frozen_hybrid_arms_reference": frozen_arms,
        "recommended_next_stages": [
            {"stage": "implement_or_run_fresh_product_candidate_source_build_v1", "priority": "primary"},
            {"stage": "rerun_fresh_product_candidate_ranking_source_after_source_build", "priority": "secondary"},
            {"stage": "rerun_fresh_eval_surface_hybrid_materialize_with_selected_source", "priority": "tertiary"},
            {
                "stage": "create_fresh_eval_labeling_worksheet_hybrid_v1",
                "priority": "quaternary",
                "condition": "only after materialize shows >=100 eligible or explicit partial path",
            },
        ],
        "recommended_next_stage": "implement_or_run_fresh_product_candidate_source_build_v1",
        "blocked_next_stages": [
            "execute_hybrid_validation_on_fresh_surface_v1",
            "hybrid_validation_metric_gates_v1",
            "ml-shadow-scorer-v1",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
            "missing_sufficient_fresh_candidate_surface": True,
            "missing_sufficient_fresh_labels": True,
            "missing_negative_labels": True,
        },
        "policy_assertions": {
            "source_expansion_required_before_hybrid_confirmatory_validation": True,
            "existing_local_db_has_no_qualifying_source": True,
            "hybrid_validation_on_44_work_surface_allowed": False,
            "threshold_lowering_allowed": False,
        },
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_fresh_candidate_source_expansion_plan(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    blocker = payload["current_blocker_summary"]
    labels = payload["labeling_implications"]
    lines = [
        f"# Fresh Candidate Source Expansion Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "No existing local source meets the fresh hybrid confirmatory candidate floor. This plan defines how to expand the candidate source before materialization, labeling, and any hybrid validation.",
        "",
        f"- **Sources considered:** {blocker['sources_considered_count']}",
        f"- **Best existing source:** `{blocker['best_source_ranking_run_id']}`",
        f"- **Best confirmatory eligible works:** {blocker['best_source_confirmatory_eligible_work_count']}",
        f"- **Candidate gap:** {blocker['candidate_gap']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_and_production_blockers']['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['shadow_and_production_blockers']['production_default_allowed']}",
        "",
        "## Current Blocker",
        "",
        f"`{blocker['best_source_ranking_run_id']}` has {blocker['best_source_confirmatory_eligible_work_count']} confirmatory-eligible works after old-surface exclusion, below the policy minimum of {blocker['minimum_confirmatory_candidate_works']}. It also has {blocker['best_source_label_snapshot']['negative_labeled_work_count']} negative labeled works.",
        "",
        "## Why This Is Source Expansion, Not ML Tuning",
        "",
        "The blocker is upstream of model evidence: there is no sufficiently large fresh candidate denominator. The plan does not tune hybrid weights, lower thresholds, train models, or run validation.",
        "",
        "## Allowed Strategies",
        "",
        "| Priority | Strategy | Description |",
        "| --- | --- | --- |",
    ]
    for strategy in payload["allowed_expansion_strategies"]:
        lines.append(f"| {strategy['priority']} | `{strategy['strategy_id']}` | {strategy['description']} |")
    lines.extend(["", "## Forbidden Shortcuts", ""])
    lines.extend(f"- {item}" for item in payload["forbidden_expansion_strategies"])
    contract = payload["candidate_generation_contract"]
    lines.extend(
        [
            "",
            "## Candidate Generation Contract",
            "",
            f"- Future command name: `{contract['future_command_name']}`",
            f"- Future commit scope: {contract['future_commit_scope']}",
            f"- Success criteria: {contract['success_criteria']}",
            f"- On success: `{contract['on_success_recommended_next_stage']}`",
            f"- On failure: `{contract['on_failure_recommended_next_stage']}`",
            "",
            "## Labeling Implications",
            "",
            f"- Absolute labeled-work deficit after expansion: {labels['labeled_work_absolute_deficit_after_expansion']}",
            f"- Coverage-only deficit on current 44: {labels['coverage_only_labeled_deficit_on_current_44']}",
            f"- Negative work deficit: {labels['negative_work_deficit']}",
            f"- Distinct negative work deficit: {labels['distinct_negative_work_deficit']}",
            "- Current best-source labels are positive-only; future source build must include a negative/borderline sampling plan.",
            "",
            "## Frozen Hybrid Arms",
            "",
            f"- Primary: `{payload['frozen_hybrid_arms_reference']['primary_confirmatory_arm']}`",
            f"- Secondary reporting: `{payload['frozen_hybrid_arms_reference']['secondary_reporting_arm']}`",
            "- No retuning on fresh eval labels.",
            "",
            "## Next Stages",
            "",
        ]
    )
    lines.extend(f"- `{item['stage']}` ({item['priority']})" for item in payload["recommended_next_stages"])
    lines.extend(
        [
            "",
            "## Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_candidate_source_expansion_plan(
    *,
    fresh_product_candidate_ranking_source_path: Path,
    fresh_eval_labeling_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_fresh_candidate_source_expansion_plan_payload(
        fresh_product_candidate_ranking_source_path=fresh_product_candidate_ranking_source_path,
        fresh_eval_labeling_plan_path=fresh_eval_labeling_plan_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        output_path=output_path,
        markdown_output_path=markdown_output_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_candidate_source_expansion_plan(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLFreshCandidateSourceExpansionPlanError",
    "build_ml_fresh_candidate_source_expansion_plan_payload",
    "markdown_from_ml_fresh_candidate_source_expansion_plan",
    "write_ml_fresh_candidate_source_expansion_plan",
]
