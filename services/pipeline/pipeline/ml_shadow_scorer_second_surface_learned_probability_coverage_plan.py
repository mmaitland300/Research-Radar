"""Plan learned-probability coverage for the second shadow surface.

This module writes an offline plan artifact only. It validates the frozen
second-surface discovery, v11 labels, second-snapshot embeddings, frozen audit
embedding scorer, and policy inputs, then records how a future commit may apply
the frozen scorer to existing embeddings. It does not query databases, generate
embeddings, apply or refit a scorer, rerun discovery, or authorize shadow or
production behavior.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_learned_probability_coverage_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1"

SECOND_SURFACE_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SECOND_SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v11"
EMBEDDINGS_ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
OFFLINE_AUDIT_SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
OFFLINE_AUDIT_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"

EXPECTED_STATUS = "selected_needs_learned_probability_coverage"
EXPECTED_DISCOVERY_NEXT_STAGE = "create_second_surface_learned_probability_coverage_plan_v1"
EXPECTED_RANKING_RUN_ID = "rank-83787b91ef"
EXPECTED_FAMILY = "emerging"
EXPECTED_CANDIDATE_SHA = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
EXPECTED_CANDIDATE_POOL_WORK_COUNT = 528
EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT = 168
EXPECTED_POSITIVE_COUNT = 94
EXPECTED_NEGATIVE_COUNT = 74
EXPECTED_EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"

RECOMMENDED_NEXT_STAGE = "apply_second_surface_learned_probability_coverage_v1"

CAVEATS = (
    "Plan artifact only; it does not apply the frozen scorer or write learned probabilities.",
    "No database access, database writes, embeddings, ranking, discovery rerun, label ingest, shadow runtime, API/web, or production/default changes.",
    "The v11 labels are metric evidence only and must not be used as scorer features.",
    "Full learned-probability coverage remains required before the second surface can be ready for generalization audit execution.",
)


class MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _dataset_version(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("dataset_version") or _get(payload, "metadata.dataset_version")
    return str(value) if value else None


def _check_passed(payload: Mapping[str, Any], key: str, *, observed: Any | None = None) -> None:
    check = _get(payload, f"threshold_check.{key}")
    if not isinstance(check, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"discovery threshold_check.{key} missing"
        )
    if check.get("passed") is not True:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"discovery threshold_check.{key} must pass"
        )
    if observed is not None and check.get("observed") != observed:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"discovery threshold_check.{key}.observed must be {observed}"
        )


def _validate_discovery(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-second-surface")
    if metadata.get("artifact_type") != SECOND_SURFACE_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "generalization second-surface metadata.artifact_type mismatch"
        )
    if metadata.get("surface_version") != SECOND_SURFACE_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "generalization second-surface surface_version mismatch"
        )
    if _get(payload, "discovery_summary.status") != EXPECTED_STATUS:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"discovery_summary.status must be {EXPECTED_STATUS}"
        )
    if payload.get("recommended_next_stage") != EXPECTED_DISCOVERY_NEXT_STAGE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"discovery recommended_next_stage must be {EXPECTED_DISCOVERY_NEXT_STAGE}"
        )
    selected = payload.get("selected_second_surface")
    if not isinstance(selected, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "selected_second_surface must be populated"
        )
    expected = {
        "ranking_run_id": EXPECTED_RANKING_RUN_ID,
        "family": EXPECTED_FAMILY,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
        "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
    }
    for key, value in expected.items():
        if selected.get(key) != value:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
                f"selected_second_surface.{key} must be {value}"
            )
    if metadata.get("source_label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"discovery metadata.source_label_dataset_version must be {LABEL_DATASET_VERSION}"
        )

    _check_passed(payload, "minimum_confirmatory_labeled_work_count", observed=EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT)
    _check_passed(payload, "minimum_confirmatory_positive_work_count", observed=EXPECTED_POSITIVE_COUNT)
    _check_passed(payload, "minimum_confirmatory_negative_work_count", observed=EXPECTED_NEGATIVE_COUNT)
    _check_passed(payload, "minimum_distinct_negative_work_count", observed=EXPECTED_NEGATIVE_COUNT)
    _check_passed(payload, "minimum_confirmatory_label_coverage_rate", observed=1.0)
    _check_passed(payload, "unresolved_label_conflicts", observed=0)

    if _get(payload, "label_coverage.work_level.conflicting_target_work_group_count") != 0:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "confirmatory conflicting_target_work_group_count must be 0"
        )
    if _get(payload, "learned_probability_coverage.learned_probability_coverage_count") != 0:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "learned_probability_coverage_count must be 0 before this plan"
        )
    if _get(payload, "learned_probability_coverage.missing_learned_probability_count") != EXPECTED_CANDIDATE_POOL_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "missing_learned_probability_count must be 528 before this plan"
        )
    if _get(payload, "threshold_check.learned_probability_coverage.passed") is not False:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "learned_probability_coverage threshold must remain failed"
        )
    return metadata


def _validate_embeddings(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="second-snapshot-embeddings")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "second-snapshot-embeddings metadata.artifact_type mismatch"
        )
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "second-snapshot-embeddings artifact_version mismatch"
        )
    if metadata.get("embedding_version") != EXPECTED_EMBEDDING_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"second-snapshot-embeddings metadata.embedding_version must be {EXPECTED_EMBEDDING_VERSION}"
        )
    if _get(payload, "coverage.snapshot_work_count") != EXPECTED_CANDIDATE_POOL_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError("embedding coverage snapshot_work_count must be 528")
    if _get(payload, "coverage.embedded_work_count") != EXPECTED_CANDIDATE_POOL_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError("embedding coverage embedded_work_count must be 528")
    if _get(payload, "coverage.missing_embedding_count") != 0:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError("embedding coverage must have zero missing embeddings")
    if _get(payload, "embedding_result.full_snapshot_embedding_coverage") is not True:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "embedding_result.full_snapshot_embedding_coverage must be true"
        )
    return metadata


def _validate_scorer(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="offline-audit-embedding-scorer")
    if metadata.get("artifact_type") != OFFLINE_AUDIT_SCORER_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "offline audit embedding scorer artifact_type mismatch"
        )
    if metadata.get("scorer_version") != OFFLINE_AUDIT_SCORER_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "offline audit embedding scorer scorer_version mismatch"
        )
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any], discovery_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="label-dataset")
    if _dataset_version(payload) != LABEL_DATASET_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            f"label dataset version must be {LABEL_DATASET_VERSION}"
        )
    ingest = _get(payload, "metadata.shadow_generalization_second_surface_v1_ingest")
    if not isinstance(ingest, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset metadata.shadow_generalization_second_surface_v1_ingest missing"
        )
    expected_counts = {
        "labeled_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
        "positive_count": EXPECTED_POSITIVE_COUNT,
        "negative_count": EXPECTED_NEGATIVE_COUNT,
        "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
    }
    for key, value in expected_counts.items():
        if ingest.get(key) != value:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
                f"label dataset shadow ingest {key} must be {value}"
            )
    if ingest.get("candidate_pool_work_set_sha256") != EXPECTED_CANDIDATE_SHA:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset shadow ingest candidate_pool_work_set_sha256 mismatch"
        )
    if ingest.get("ranking_run_id") != EXPECTED_RANKING_RUN_ID:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset shadow ingest ranking_run_id mismatch"
        )
    if ingest.get("label_thresholds_passed") is not True:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset shadow ingest label_thresholds_passed must be true"
        )
    if _get(discovery_payload, "label_coverage.work_level.confirmatory_labeled_work_count") != ingest.get("labeled_count"):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset labeled count does not match discovery output"
        )
    if _get(discovery_payload, "label_coverage.work_level.confirmatory_positive_work_count") != ingest.get("positive_count"):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset positive count does not match discovery output"
        )
    if _get(discovery_payload, "label_coverage.work_level.confirmatory_negative_work_count") != ingest.get("negative_count"):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "label dataset negative count does not match discovery output"
        )
    return metadata


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-audit-plan")
    if metadata.get("artifact_type") != GENERALIZATION_PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "generalization audit plan artifact_type mismatch"
        )
    if metadata.get("plan_version") != GENERALIZATION_PLAN_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "generalization audit plan_version mismatch"
        )
    if payload.get("generalization_audit_plan_defined") is not True:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "generalization audit plan must be defined"
        )
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "runtime implementation must remain unauthorized"
        )
    return metadata


def _validate_online_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="online-shadow-policy")
    if metadata.get("artifact_type") != ONLINE_POLICY_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "online shadow policy artifact_type mismatch"
        )
    if metadata.get("policy_version") != ONLINE_POLICY_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "online shadow policy_version mismatch"
        )
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError(
            "online shadow policy runtime_implementation_authorized must be false"
        )
    return metadata


def build_ml_shadow_scorer_second_surface_learned_probability_coverage_plan_payload(
    *,
    generalization_second_surface_path: Path,
    label_dataset_path: Path,
    second_snapshot_embeddings_path: Path,
    offline_audit_embedding_scorer_path: Path,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path | None = None,
    markdown_output_path: Path | None = None,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    discovery_path = Path(generalization_second_surface_path).resolve()
    labels_path = Path(label_dataset_path).resolve()
    embeddings_path = Path(second_snapshot_embeddings_path).resolve()
    scorer_path = Path(offline_audit_embedding_scorer_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()

    discovery_payload = _load_json_object(discovery_path)
    label_payload = _load_json_object(labels_path)
    embeddings_payload = _load_json_object(embeddings_path)
    scorer_payload = _load_json_object(scorer_path)
    audit_plan_payload = _load_json_object(audit_plan_path)
    policy_payload = _load_json_object(policy_path)

    discovery_metadata = _validate_discovery(discovery_payload)
    label_metadata = _validate_label_dataset(label_payload, discovery_payload)
    embeddings_metadata = _validate_embeddings(embeddings_payload)
    scorer_metadata = _validate_scorer(scorer_payload)
    audit_plan_metadata = _validate_generalization_plan(audit_plan_payload)
    policy_metadata = _validate_online_policy(policy_payload)

    inputs = [
        _input_record("generalization_second_surface", discovery_path, repo_root=root),
        _input_record("label_dataset", labels_path, repo_root=root),
        _input_record("second_snapshot_embeddings", embeddings_path, repo_root=root),
        _input_record("offline_audit_embedding_scorer", scorer_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("online_shadow_policy", policy_path, repo_root=root),
    ]
    embeddings_portable_path = portable_repo_path(embeddings_path, repo_root=root)

    selected = discovery_payload["selected_second_surface"]
    label_ingest = _get(label_payload, "metadata.shadow_generalization_second_surface_v1_ingest")
    learned_probability_coverage = discovery_payload["learned_probability_coverage"]

    evidence_summary = {
        "ranking_run_id": selected["ranking_run_id"],
        "family": selected["family"],
        "candidate_pool_work_set_sha256": selected["candidate_pool_work_set_sha256"],
        "candidate_pool_work_count": selected["candidate_pool_work_count"],
        "confirmatory_metric_eligible_work_count": selected["confirmatory_metric_eligible_work_count"],
        "embedding_coverage": {
            "embedding_version": embeddings_metadata.get("embedding_version"),
            "embedded_work_count": _get(embeddings_payload, "coverage.embedded_work_count"),
            "snapshot_work_count": _get(embeddings_payload, "coverage.snapshot_work_count"),
            "missing_embedding_count": _get(embeddings_payload, "coverage.missing_embedding_count"),
            "full_snapshot_embedding_coverage": _get(
                embeddings_payload, "embedding_result.full_snapshot_embedding_coverage"
            ),
        },
        "learned_probability_coverage": {
            "learned_probability_coverage_count": learned_probability_coverage["learned_probability_coverage_count"],
            "missing_learned_probability_count": learned_probability_coverage["missing_learned_probability_count"],
            "probe_status": _get(
                learned_probability_coverage, "approved_upstream_probability_probe.probe_status"
            ),
            "scorer_execution_used": learned_probability_coverage.get("scorer_execution_used"),
        },
        "label_coverage_reference_only": {
            "label_dataset_version": _dataset_version(label_payload),
            "labeled_count": label_ingest["labeled_count"],
            "positive_count": label_ingest["positive_count"],
            "negative_count": label_ingest["negative_count"],
            "confirmatory_label_coverage_rate": _get(
                discovery_payload, "label_coverage.work_level.label_coverage_rate"
            ),
            "labels_are_scorer_inputs": False,
        },
    }

    learned_probability_coverage_contract = {
        "approved_scorer": OFFLINE_AUDIT_SCORER_VERSION,
        "approved_scorer_artifact_type": scorer_metadata.get("artifact_type"),
        "approved_embedding_version": EXPECTED_EMBEDDING_VERSION,
        "approved_embeddings_artifact": embeddings_portable_path,
        "must_not_refit": True,
        "must_not_regenerate_embeddings": True,
        "must_not_use_v11_labels_as_scorer_features": True,
        "target_coverage": {
            "covered_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "coverage_rate": 1.0,
        },
        "output_field": "audit_embedding_probability_work",
        "future_execution_command": "ml-shadow-scorer-second-surface-learned-probability-apply",
        "future_execution_output_artifact": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
        "future_probe_update": (
            "extend discovery _approved_probability_probe to read the new artifact keyed by "
            "ranking_run_id + candidate_pool_work_set_sha256; do not reuse first-surface audit artifacts"
        ),
        "post_execution_discovery_rerun": (
            "ml-shadow-scorer-generalization-second-surface with ml-label-dataset-v11.json; "
            "expected status selected_ready_for_generalization_audit"
        ),
    }

    blocker_semantics_note = (
        "Discovery blocker missing_generalization_second_surface_selected means the selected second surface is not yet "
        "ready for generalization audit execution; it does not mean no surface was selected. selected_second_surface "
        "is populated and readiness_for_generalization_audit.candidate_source_selected is true."
    )
    blockers = {
        "missing_second_surface_learned_probability_coverage": True,
        "missing_generalization_audit_on_second_surface": True,
        "missing_generalization_audit_gates": True,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "blocker_semantics_note": blocker_semantics_note,
    }

    planned_commit_sequence = [
        "feat(eval): add second-surface learned-probability coverage plan for shadow generalization v1",
        (
            "feat(eval): apply frozen ml-offline-audit-embedding-scorer-v2 to 528 existing embeddings; emit "
            "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json"
        ),
        "feat(eval): extend discovery probe and rerun second-surface discovery",
        "feat(eval): audit ml-shadow-scorer-v1 on second fresh surface",
    ]

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "plan_version": plan_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_second_surface_version": discovery_metadata.get("surface_version"),
        "source_label_dataset_version": _dataset_version(label_payload),
        "source_embeddings_artifact_version": embeddings_metadata.get("artifact_version"),
        "source_offline_audit_embedding_scorer_version": scorer_metadata.get("scorer_version"),
        "source_generalization_plan_version": audit_plan_metadata.get("plan_version"),
        "source_online_shadow_policy_version": policy_metadata.get("policy_version"),
        "ranking_run_id": EXPECTED_RANKING_RUN_ID,
        "family": EXPECTED_FAMILY,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "learned_probability_application_in_this_plan_commit": False,
        "scorer_execution_in_this_plan_commit": False,
    }
    return {
        "metadata": metadata,
        "provenance": {
            "inputs": inputs,
            "discovery_status": _get(discovery_payload, "discovery_summary.status"),
            "discovery_recommended_next_stage": discovery_payload.get("recommended_next_stage"),
            "label_dataset_version": _dataset_version(label_payload),
            "embedding_version": embeddings_metadata.get("embedding_version"),
            "offline_audit_embedding_scorer_version": scorer_metadata.get("scorer_version"),
        },
        "learned_probability_coverage_plan_defined": True,
        "learned_probability_application_in_this_plan_commit": False,
        "scorer_execution_in_this_plan_commit": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "evidence_summary": evidence_summary,
        "learned_probability_coverage_contract": learned_probability_coverage_contract,
        "blocked_actions": [
            "database_writes",
            "ranking_run_creation",
            "embedding_generation",
            "scorer_refit/training",
            "learned_probability_application_in_this_plan_commit",
            "scorer_execution_in_this_plan_commit",
            "label_ingest",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": blockers,
        "planned_commit_sequence": planned_commit_sequence,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_second_surface_learned_probability_coverage_plan(
    payload: Mapping[str, Any]
) -> str:
    metadata = payload["metadata"]
    evidence = payload["evidence_summary"]
    contract = payload["learned_probability_coverage_contract"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# Second-Surface Learned-Probability Coverage Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This plan freezes the path to achieve full learned-probability coverage for the selected second shadow-generalization surface. It is plan-only: no scorer is applied, no embeddings are generated, no discovery is rerun, and no shadow or production behavior is enabled.",
        "",
        f"- Ranking run: `{evidence['ranking_run_id']}`",
        f"- Family: `{evidence['family']}`",
        f"- Candidate pool SHA: `{evidence['candidate_pool_work_set_sha256']}`",
        f"- Candidate pool: {evidence['candidate_pool_work_count']}",
        f"- Confirmatory eligible: {evidence['confirmatory_metric_eligible_work_count']}",
        f"- Embedding coverage: {evidence['embedding_coverage']['embedded_work_count']} / {evidence['embedding_coverage']['snapshot_work_count']}",
        f"- Learned-probability coverage: {evidence['learned_probability_coverage']['learned_probability_coverage_count']} / {evidence['candidate_pool_work_count']}",
        f"- Label coverage reference: {evidence['label_coverage_reference_only']['labeled_count']} / {evidence['confirmatory_metric_eligible_work_count']} ({evidence['label_coverage_reference_only']['positive_count']} positive, {evidence['label_coverage_reference_only']['negative_count']} negative)",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Learned-Probability Contract",
        "",
        f"- Approved scorer: `{contract['approved_scorer']}`",
        f"- Approved embedding version: `{contract['approved_embedding_version']}`",
        f"- Approved embeddings artifact: `{contract['approved_embeddings_artifact']}`",
        f"- Output field: `{contract['output_field']}`",
        f"- Target coverage: {contract['target_coverage']['covered_work_count']} / {contract['target_coverage']['candidate_pool_work_count']}",
        f"- Future execution command: `{contract['future_execution_command']}`",
        f"- Future execution artifact: `{contract['future_execution_output_artifact']}`",
        f"- Must not refit: {contract['must_not_refit']}",
        f"- Must not regenerate embeddings: {contract['must_not_regenerate_embeddings']}",
        f"- Must not use v11 labels as scorer features: {contract['must_not_use_v11_labels_as_scorer_features']}",
        "",
        "## Future Probe And Discovery",
        "",
        f"- Probe update: {contract['future_probe_update']}",
        f"- Discovery rerun: {contract['post_execution_discovery_rerun']}",
        "",
        "## Blocker Semantics Note",
        "",
        blockers["blocker_semantics_note"],
        "",
        "## Remaining Blockers",
        "",
    ]
    for key, value in blockers.items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Planned Commit Sequence", ""])
    lines.extend(f"{index}. {item}" for index, item in enumerate(payload["planned_commit_sequence"], start=1))
    lines.extend(["", "## Explicitly Blocked Actions", ""])
    lines.extend(f"- `{item}`" for item in payload["blocked_actions"])
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_second_surface_learned_probability_coverage_plan(
    *,
    generalization_second_surface_path: Path,
    label_dataset_path: Path,
    second_snapshot_embeddings_path: Path,
    offline_audit_embedding_scorer_path: Path,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_second_surface_learned_probability_coverage_plan_payload(
        generalization_second_surface_path=generalization_second_surface_path,
        label_dataset_path=label_dataset_path,
        second_snapshot_embeddings_path=second_snapshot_embeddings_path,
        offline_audit_embedding_scorer_path=offline_audit_embedding_scorer_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        online_shadow_policy_path=online_shadow_policy_path,
        output_path=output_path,
        markdown_output_path=markdown_output_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_surface_learned_probability_coverage_plan(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError",
    "build_ml_shadow_scorer_second_surface_learned_probability_coverage_plan_payload",
    "markdown_from_ml_shadow_scorer_second_surface_learned_probability_coverage_plan",
    "write_ml_shadow_scorer_second_surface_learned_probability_coverage_plan",
]
