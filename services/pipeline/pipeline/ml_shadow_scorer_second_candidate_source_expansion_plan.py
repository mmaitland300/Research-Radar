"""Second candidate-source expansion plan for shadow scorer generalization.

This module writes a policy/spec artifact only. It reads committed JSON and
Markdown inputs and defines how to create or expand a second fresh candidate
source after local read-only discovery found no qualifying source. It does not
query databases, call OpenAlex/OpenAI, create ranking runs, generate embeddings,
score, ingest labels, implement online shadowing, or authorize production.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_candidate_source_expansion_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1"

SECOND_SURFACE_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SECOND_SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"
FRESH_SURFACE_POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
FRESH_SURFACE_POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"

DISALLOWED_RANKING_RUN_ID = "rank-9f4b2a2084"
DISALLOWED_CANDIDATE_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
OLD_217_EVAL_SHA = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
EXPECTED_SECOND_SURFACE_NEXT_STAGE = "create_or_expand_second_fresh_candidate_source_for_shadow_generalization_v1"
RECOMMENDED_NEXT_STAGE = "implement_or_run_second_fresh_candidate_source_build_for_shadow_generalization_v1"

BLOCKED_DISCOVERY_STATUSES = {
    "blocked_no_candidate_source_meets_minimum",
    "blocked_no_distinct_second_surface",
}

CAVEATS = (
    "Plan only; no candidate source build, ranking creation, scorer execution, embeddings, runtime, shadow/prod, or API/web changes.",
    "This is source expansion because local DB inventory lacks a qualifying second surface, not because the hybrid formula failed on surface one.",
    "Policy thresholds are not lowered by this plan.",
    "Labels remain metric-only and must not be used for candidate selection.",
)


class MLShadowScorerSecondCandidateSourceExpansionPlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(f"Input {name} does not exist: {path}")
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
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(f"{name} metadata.{version_field} must be {version}")
    return metadata


def _validate_second_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-second-surface",
        artifact_type=SECOND_SURFACE_ARTIFACT_TYPE,
        version_field="surface_version",
        version=SECOND_SURFACE_VERSION,
    )
    status = _get(payload, "discovery_summary.status") or _get(payload, "readiness_for_generalization_audit.status")
    if status not in BLOCKED_DISCOVERY_STATUSES:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            f"second-surface discovery status must be blocked, got {status!r}"
        )
    if payload.get("recommended_next_stage") != EXPECTED_SECOND_SURFACE_NEXT_STAGE:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            f"second-surface recommended_next_stage must be {EXPECTED_SECOND_SURFACE_NEXT_STAGE}"
        )
    if payload.get("selected_second_surface") is not None:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError("second-surface selected_second_surface must be null")
    if _get(payload, "readiness_for_generalization_audit.ready_for_generalization_audit_execution") is not False:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "second-surface ready_for_generalization_audit_execution must be false"
        )
    if int(_get(payload, "discovery_summary.candidate_sources_considered_count") or 0) < 1:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "second-surface candidate_sources_considered_count must be >= 1"
        )
    return metadata


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-plan",
        artifact_type=GENERALIZATION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=GENERALIZATION_PLAN_VERSION,
    )
    if payload.get("generalization_audit_plan_defined") is not True:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "generalization audit plan must define generalization_audit_plan_defined=true"
        )
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "generalization audit plan runtime_implementation_authorized must be false"
        )
    return metadata


def _validate_online_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "online shadow policy runtime_implementation_authorized must be false"
        )
    return metadata


def _validate_fresh_surface_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    metadata = _validate_identity(
        payload,
        name="fresh-surface-policy",
        artifact_type=FRESH_SURFACE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=FRESH_SURFACE_POLICY_VERSION,
    )
    thresholds = _get(payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "fresh-surface policy missing label_policy.minimum_confirmatory_label_thresholds"
        )
    required = {
        "minimum_candidate_work_count",
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
        "minimum_confirmatory_label_coverage_rate",
    }
    missing = sorted(required.difference(thresholds))
    if missing:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            f"fresh-surface policy missing label thresholds: {missing}"
        )
    return metadata, thresholds


def _candidate_count(source: Mapping[str, Any]) -> int:
    for key in ("confirmatory_metric_eligible_work_count", "confirmatory_eligible_work_count"):
        value = source.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return int(source.get("confirmatory_metric_eligible_work_count") or 0)


def _pool_count(source: Mapping[str, Any]) -> int:
    for key in ("candidate_pool_work_count", "candidate_work_count"):
        value = source.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return int(source.get("candidate_pool_work_count") or 0)


def _source_sha(source: Mapping[str, Any]) -> str | None:
    value = source.get("candidate_pool_work_set_sha256") or source.get("candidate_work_set_sha256")
    return str(value) if value else None


def _best_distinct_source(payload: Mapping[str, Any], *, minimum_confirmatory_eligible: int) -> Mapping[str, Any] | None:
    sources = payload.get("candidate_sources_considered")
    if not isinstance(sources, list):
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "second-surface candidate_sources_considered must be a list"
        )
    distinct: list[Mapping[str, Any]] = []
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        rid = source.get("ranking_run_id")
        sha = _source_sha(source)
        reasons = source.get("disallowed_reasons") or []
        if source.get("distinct_from_first_validated_surface") is True and not reasons:
            if rid == DISALLOWED_RANKING_RUN_ID or sha == DISALLOWED_CANDIDATE_SHA:
                raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
                    "best distinct source cannot reuse rank-9f4b2a2084 or its candidate SHA"
                )
            distinct.append(source)
    if not distinct:
        return None
    best = max(distinct, key=lambda item: (_candidate_count(item), _pool_count(item), str(item.get("ranking_run_id") or "")))
    if _candidate_count(best) >= minimum_confirmatory_eligible:
        raise MLShadowScorerSecondCandidateSourceExpansionPlanError(
            "best distinct source already meets the confirmatory-eligible minimum"
        )
    return best


def _strategy(strategy_id: str, description: str, priority: str) -> dict[str, str]:
    return {"strategy_id": strategy_id, "description": description, "priority": priority}


def build_ml_shadow_scorer_second_candidate_source_expansion_plan_payload(
    *,
    generalization_second_surface_path: Path,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path | None = None,
    markdown_output_path: Path | None = None,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    second_surface_path = Path(generalization_second_surface_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    fresh_policy_path = Path(fresh_surface_policy_path).resolve()

    second_surface_payload = _load_json_object(second_surface_path)
    audit_plan_payload = _load_json_object(audit_plan_path)
    online_policy_payload = _load_json_object(policy_path)
    fresh_policy_payload = _load_json_object(fresh_policy_path)

    second_surface_metadata = _validate_second_surface(second_surface_payload)
    audit_plan_metadata = _validate_generalization_plan(audit_plan_payload)
    online_policy_metadata = _validate_online_policy(online_policy_payload)
    fresh_policy_metadata, thresholds = _validate_fresh_surface_policy(fresh_policy_payload)

    inputs = [
        _input_record("generalization_second_surface", second_surface_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("online_shadow_policy", policy_path, repo_root=root),
        _input_record("fresh_surface_policy", fresh_policy_path, repo_root=root),
    ]

    minimum_confirmatory_eligible = int(thresholds["minimum_candidate_work_count"])
    best = _best_distinct_source(second_surface_payload, minimum_confirmatory_eligible=minimum_confirmatory_eligible)
    best_eligible = _candidate_count(best) if best is not None else 0
    candidate_gap = max(0, minimum_confirmatory_eligible - best_eligible)
    sources_considered = int(_get(second_surface_payload, "discovery_summary.candidate_sources_considered_count") or 0)

    current_blocker_summary = {
        "sources_considered_count": sources_considered,
        "best_distinct_ranking_run_id": best.get("ranking_run_id") if best is not None else None,
        "best_distinct_candidate_pool_work_set_sha256": _source_sha(best) if best is not None else None,
        "best_candidate_work_count": _pool_count(best) if best is not None else 0,
        "best_confirmatory_eligible_work_count": best_eligible,
        "minimum_confirmatory_eligible_work_count": minimum_confirmatory_eligible,
        "candidate_gap": candidate_gap,
        "first_validated_surface_excluded": True,
        "first_validated_ranking_run_id": DISALLOWED_RANKING_RUN_ID,
        "first_validated_candidate_work_set_sha256": DISALLOWED_CANDIDATE_SHA,
        "old_217_overlap_excluded": True,
        "old_217_eval_work_set_sha256": OLD_217_EVAL_SHA,
        "note": "Best source may have embedding coverage but still lacks learned probability and labels; expansion addresses pool size first.",
    }

    evidence_summary = {
        "second_surface_status": _get(second_surface_payload, "discovery_summary.status")
        or _get(second_surface_payload, "readiness_for_generalization_audit.status"),
        "second_surface_recommended_next_stage": second_surface_payload.get("recommended_next_stage"),
        "best_distinct_source_found": best is not None,
        "best_distinct_source_below_minimum": best is None or best_eligible < minimum_confirmatory_eligible,
        "generalization_audit_plan_defined": audit_plan_payload.get("generalization_audit_plan_defined") is True,
        "online_shadow_policy_runtime_authorized": online_policy_payload.get("runtime_implementation_authorized") is True,
    }

    second_source_expansion_requirements = {
        "minimum_confirmatory_eligible_work_count_after_exclusions": minimum_confirmatory_eligible,
        "exclude_old_217_eval_surface": {
            "required": True,
            "work_set_sha256": OLD_217_EVAL_SHA,
        },
        "exclude_first_validated_surface": {
            "required": True,
            "ranking_run_id": DISALLOWED_RANKING_RUN_ID,
            "candidate_pool_work_set_sha256": DISALLOWED_CANDIDATE_SHA,
        },
        "ranking_run_id_must_differ_from": DISALLOWED_RANKING_RUN_ID,
        "candidate_pool_work_set_sha256_must_differ_from": DISALLOWED_CANDIDATE_SHA,
        "prefer_newer_or_broader_corpus_snapshot_version": True,
        "canonical_openalex_work_ids_required": True,
        "freeze_candidate_source_metadata_before_scoring_or_audit": True,
        "full_final_score_coverage_required": True,
        "must_later_support_full_audit_embedding_probability_work_coverage": True,
        "labels_must_not_drive_candidate_selection": True,
    }

    allowed_expansion_strategies = [
        _strategy(
            "create_newer_corpus_snapshot_and_candidate_run",
            "Preferred. Create or select a newer corpus snapshot and product-candidate ranking run, then rerun discovery.",
            "primary",
        ),
        _strategy(
            "broaden_existing_snapshot_candidate_pool",
            "Broaden product-plausible filters within an existing snapshot while preserving source provenance and final_score coverage.",
            "secondary",
        ),
        _strategy(
            "targeted_borderline_and_negative_candidate_collection",
            "Deliberately include product-plausible borderline and negative-oriented candidates so later labels are not positive-only.",
            "label_readiness_support",
        ),
        _strategy(
            "multi_family_candidate_source_with_declared_family_rules",
            "Optional only; emerging remains the default and any multi-family source needs explicit family accounting.",
            "optional",
        ),
    ]

    forbidden_expansion_strategies = [
        f"reusing {DISALLOWED_RANKING_RUN_ID} as the second surface",
        f"reusing candidate SHA {DISALLOWED_CANDIDATE_SHA}",
        "lowering thresholds to make rank-3904fec89d pass",
        "cherry-picking candidates by labels",
        "scorer execution before surface re-selection",
        "embedding generation or learned scorer application inside this plan",
        "online shadow/runtime/prod/API changes",
    ]

    candidate_generation_contract = {
        "future_command": "ml-shadow-scorer-second-candidate-source-build",
        "future_build_scope": "separate explicitly scoped commit; may use OpenAlex/DB writes only if that future task permits them",
        "must_emit_frozen_source_fields": [
            "ranking_run_id",
            "family",
            "corpus_snapshot_version",
            "embedding_version",
            "candidate_pool_work_set_sha256",
            "candidate_pool_work_count",
            "confirmatory_eligible_work_count_after_exclusions",
        ],
        "must_rerun": "ml-shadow-scorer-generalization-second-surface",
        "success_criteria": (
            "rerun discovery reaches a status other than blocked_no_candidate_source_meets_minimum / "
            "blocked_no_distinct_second_surface, with >=100 confirmatory-eligible works after exclusions"
        ),
        "labels_or_probability_may_still_block": True,
    }

    learned_probability_contract = {
        "full_audit_embedding_probability_work_coverage_required_eventually": True,
        "approved_sources": [
            "pre-existing audit_embedding_probability_work values",
            "approved upstream application of frozen ml-offline-audit-embedding-scorer-v2 to pre-existing embeddings",
        ],
        "online_shadow_runtime_must_not_create_probabilities": True,
        "this_plan_generates_embeddings_or_probabilities": False,
    }

    labeling_implications = {
        "pool_size_does_not_imply_generalization_ready": True,
        "future_label_thresholds": {
            "minimum_confirmatory_labeled_work_count": thresholds["minimum_confirmatory_labeled_work_count"],
            "minimum_confirmatory_positive_work_count": thresholds["minimum_confirmatory_positive_work_count"],
            "minimum_confirmatory_negative_work_count": thresholds["minimum_confirmatory_negative_work_count"],
            "minimum_distinct_negative_work_count": thresholds["minimum_distinct_negative_work_count"],
            "minimum_confirmatory_label_coverage_rate": thresholds["minimum_confirmatory_label_coverage_rate"],
        },
        "labels_are_metric_only_never_scoring_features": True,
    }

    planned_commit_sequence = [
        "feat(eval): add second fresh candidate source expansion plan for shadow generalization v1",
        "feat(eval): implement or run second fresh candidate source build for shadow generalization v1",
        "feat(eval): materialize or select second fresh surface for shadow generalization v1 (rerun discovery)",
        "feat(eval): create second-surface labeling plan if selected_needs_labels",
        "feat(eval): create second-surface learned-probability coverage plan if selected_needs_learned_probability_coverage",
        "feat(eval): audit ml-shadow-scorer-v1 on second fresh surface",
        "feat(eval): add ml-shadow-scorer-v1 generalization audit gates",
        "feat(eval): implement online shadow runtime disabled by default (only after generalization gates pass)",
    ]

    blockers = {
        "missing_second_fresh_candidate_source_expansion_plan_v1": False,
        "missing_second_fresh_candidate_source": True,
        "missing_generalization_audit_on_second_surface": True,
        "missing_generalization_audit_gates": True,
        "missing_online_shadow_implementation_disabled_by_default": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "source_second_surface_version": second_surface_metadata.get("surface_version"),
            "source_generalization_plan_version": audit_plan_metadata.get("plan_version"),
            "source_online_shadow_policy_version": online_policy_metadata.get("policy_version"),
            "source_fresh_surface_policy_version": fresh_policy_metadata.get("policy_version"),
            "caveats": list(CAVEATS),
        },
        "second_candidate_source_expansion_plan_defined": True,
        "generalization_audit_executed": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "evidence_summary": evidence_summary,
        "current_blocker_summary": current_blocker_summary,
        "second_source_expansion_requirements": second_source_expansion_requirements,
        "allowed_expansion_strategies": allowed_expansion_strategies,
        "forbidden_expansion_strategies": forbidden_expansion_strategies,
        "candidate_generation_contract": candidate_generation_contract,
        "learned_probability_contract": learned_probability_contract,
        "labeling_implications": labeling_implications,
        "planned_commit_sequence": planned_commit_sequence,
        "blocked_actions": [
            "database_access",
            "database_writes",
            "ranking_run_creation",
            "openalex_calls",
            "scorer_execution",
            "embedding_generation",
            "training",
            "label_ingest",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_second_candidate_source_expansion_plan(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    blocker = payload["current_blocker_summary"]
    lines = [
        f"# ML Shadow Scorer v1 Second Candidate Source Expansion Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "Local discovery found no distinct second surface that meets the 100-work confirmatory-eligible minimum. This plan defines source expansion only; it is not ML retuning, scorer execution, runtime work, shadow execution, or production authorization.",
        "",
        f"- Status source: `{payload['evidence_summary']['second_surface_status']}`",
        f"- Sources considered: {blocker['sources_considered_count']}",
        f"- Best distinct source: `{blocker['best_distinct_ranking_run_id']}`",
        f"- Best confirmatory-eligible works: {blocker['best_confirmatory_eligible_work_count']} / {blocker['minimum_confirmatory_eligible_work_count']}",
        f"- Candidate gap: {blocker['candidate_gap']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Current Blocker",
        "",
        f"The best distinct source, `{blocker['best_distinct_ranking_run_id']}`, has {blocker['best_candidate_work_count']} pool works and {blocker['best_confirmatory_eligible_work_count']} confirmatory-eligible works after excluding prior surfaces. The policy minimum is {blocker['minimum_confirmatory_eligible_work_count']}, leaving a gap of {blocker['candidate_gap']}.",
        "",
        "## Why This Is Expansion, Not ML Tuning",
        "",
        "The blocker is source supply. The first validated surface remains non-reusable for generalization evidence, and no policy threshold is lowered.",
        "",
        "## Requirements",
        "",
    ]
    for key, value in payload["second_source_expansion_requirements"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Allowed Strategies", "", "| Priority | Strategy | Description |", "| --- | --- | --- |"])
    for strategy in payload["allowed_expansion_strategies"]:
        lines.append(f"| {strategy['priority']} | `{strategy['strategy_id']}` | {strategy['description']} |")
    lines.extend(["", "## Forbidden Strategies", ""])
    lines.extend(f"- {item}" for item in payload["forbidden_expansion_strategies"])
    lines.extend(
        [
            "",
            "## Learned Probability And Labeling Follow-Ons",
            "",
            "- Full learned-probability coverage is required later, but this plan does not generate probabilities or embeddings.",
            "- A pool of at least 100 confirmatory-eligible works still may need labels before audit execution.",
            "- Labels are metric-only and never scoring features.",
            "",
            "## Planned Commit Sequence",
            "",
        ]
    )
    lines.extend(f"{idx}. {item}" for idx, item in enumerate(payload["planned_commit_sequence"], start=1))
    lines.extend(["", "## Blockers", ""])
    for key, value in payload["shadow_and_production_blockers"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_second_candidate_source_expansion_plan(
    *,
    generalization_second_surface_path: Path,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_second_candidate_source_expansion_plan_payload(
        generalization_second_surface_path=generalization_second_surface_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        online_shadow_policy_path=online_shadow_policy_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        output_path=output_path,
        markdown_output_path=markdown_output_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_candidate_source_expansion_plan(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLShadowScorerSecondCandidateSourceExpansionPlanError",
    "build_ml_shadow_scorer_second_candidate_source_expansion_plan_payload",
    "markdown_from_ml_shadow_scorer_second_candidate_source_expansion_plan",
    "write_ml_shadow_scorer_second_candidate_source_expansion_plan",
]
