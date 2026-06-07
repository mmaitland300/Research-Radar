"""File-only Bridge rank-percentile hybrid serving plan artifact.

This module freezes the serving contract implied by the completed controlled
rollout replay. It does not implement serving, read the database, or change API
or web behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_bridge_rank_pct_hybrid_controlled_rollout_eval import (
    ARTIFACT_TYPE as CONTROLLED_ROLLOUT_ARTIFACT_TYPE,
    ARTIFACT_VERSION as CONTROLLED_ROLLOUT_ARTIFACT_VERSION,
)
from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
    EMBEDDINGS_ARTIFACT_VERSION,
    LABEL_DATASET_VERSION,
    SELECTED_FROZEN_C,
    SENSITIVITY_ARTIFACT_TYPE,
    SENSITIVITY_ARTIFACT_VERSION,
    SHADOW_RANKING_RUN_ID,
    TARGET,
    _as_float,
)
from pipeline.ml_offline_bridge_hybrid_rank_pct_eval_v3 import (
    ARTIFACT_TYPE as RANK_PCT_EVAL_ARTIFACT_TYPE,
    EVAL_VERSION as RANK_PCT_EVAL_VERSION,
    SCOPE_FULL_POOL,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_bridge_rank_pct_hybrid_serving_plan"
PLAN_VERSION = "ml-bridge-rank-pct-hybrid-serving-plan-v1"
TARGET_SURFACE = "bridge"
SCORER = "bridge_recommendable_v3"
PRIMARY_ALPHA = 0.5
EXPLORATORY_ALPHA = 0.7
EXPECTED_CANDIDATE_COUNT = 528
SCORER_SERVED_LIMIT = 20
FORMULA = "alpha * rank_pct(v3_ml_probability) + (1-alpha) * rank_pct(bridge_score)"
RANK_PCT_FORMULA = "1 - average_rank / n"
RECOMMENDED_NEXT_STAGE = "implement_bridge_rank_pct_hybrid_serving_gate_v1"
FAILURE_NEXT_STAGE = "collect_bridge_rollout_review_labels_before_serving"

CONTROLLED_ROLLOUT_NEXT_STAGE = "draft_bridge_rank_pct_hybrid_serving_plan_v1"
RANK_PCT_NEXT_STAGE = "authorize_bridge_hybrid_serving_controlled_rollout_eval"
LINEAR_HYBRID_EVAL_ARTIFACT_TYPE = "ml_offline_bridge_hybrid_eval_v3"
LINEAR_HYBRID_EVAL_VERSION = "ml-offline-bridge-hybrid-eval-v3-v1"
LINEAR_HYBRID_NEXT_STAGE = "do_not_authorize_bridge_hybrid_serving_recheck_alpha_or_formula"

ALGORITHM_REFERENCES = (
    "services/pipeline/pipeline/ml_bridge_rank_pct_hybrid_controlled_rollout_eval.py",
    "services/pipeline/pipeline/ml_offline_bridge_hybrid_rank_pct_eval_v3.py",
)

BRIDGE_ENV_DEFAULTS = {
    "ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED": "missing/false closes Bridge ML serving",
    "ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_ENABLED": "missing/false closes public Bridge ML rollout",
    "ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_PERCENT": "missing/zero closes public Bridge ML rollout",
    "ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP": "missing/zero cap closes Bridge ML serving",
    "ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST": "missing/empty means no cohort canary path",
    "ML_BRIDGE_SCORER_V1_RANKING_RUN_ID": f"must equal {SHADOW_RANKING_RUN_ID}",
}

CAVEATS = (
    "Controlled rollout eval is offline only.",
    "Proposed top-20 has 7 unlabeled papers.",
    "Full top-20 churn means user-visible Bridge behavior would change substantially.",
    "There were 8 demoted labeled positives, all classified as competitive rather than clear losses.",
    "Single-reviewer labels only.",
    "No external validation.",
    "No multi-reviewer agreement.",
    "Serving plan does not itself authorize broad rollout.",
    "Alpha=0.7 is exploratory only; serving v1 should use alpha=0.5 unless a later artifact explicitly changes that.",
)

FAIL_CLOSED_CONDITIONS = (
    "Bridge env flag disabled",
    "rollout cap reached",
    "public rollout disabled",
    "cohort/user not eligible",
    "ranking_run_id mismatch",
    "ranking_version mismatch, if available",
    "corpus_snapshot_version mismatch, if available",
    "family != bridge",
    "limit != 20",
    "bridge_score missing for any candidate needed in full-pool scoring",
    "frozen scorer unavailable",
    "embeddings unavailable",
    "scoring raises",
    "DB read fails",
)


class MLBridgeRankPctHybridServingPlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLBridgeRankPctHybridServingPlanError(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLBridgeRankPctHybridServingPlanError(f"{label} JSON must be an object: {path}")
    return payload


def _required_float(payload: Mapping[str, Any], key: str, *, label: str) -> float:
    value = _as_float(payload.get(key))
    if value is None:
        raise MLBridgeRankPctHybridServingPlanError(f"{label} missing numeric {key!r}")
    return value


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _parse_coverage(value: Any) -> tuple[int, int, str]:
    if isinstance(value, str) and "/" in value:
        left, right = value.split("/", 1)
        try:
            covered = int(left)
            total = int(right)
        except ValueError as exc:
            raise MLBridgeRankPctHybridServingPlanError(
                f"invalid bridge_score_coverage value {value!r}"
            ) from exc
        return covered, total, f"{covered}/{total}"
    if isinstance(value, Mapping):
        covered = _as_int(value.get("non_null_count") or value.get("covered"))
        total = _as_int(value.get("total_count") or value.get("total"))
        if covered is not None and total is not None:
            return covered, total, f"{covered}/{total}"
    raise MLBridgeRankPctHybridServingPlanError(
        f"invalid bridge_score_coverage value {value!r}; expected 'covered/total'"
    )


def _input_record(name: str, path: Path, sha: str) -> dict[str, Any]:
    return {"name": name, "path": portable_repo_path(path), "sha256": sha}


def _controlled_input(controlled_payload: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    inputs = controlled_payload.get("inputs")
    if not isinstance(inputs, list):
        return None
    for item in inputs:
        if isinstance(item, Mapping) and item.get("name") == name:
            return item
    return None


def _resolve_repo_or_absolute(raw_path: Any) -> Path | None:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return default_repo_root() / path


def _load_optional_shadow_pilot(controlled_payload: Mapping[str, Any]) -> dict[str, Any] | None:
    shadow_input = _controlled_input(controlled_payload, "shadow_pilot_artifact")
    if shadow_input is None:
        return None
    shadow_path = _resolve_repo_or_absolute(shadow_input.get("path"))
    if shadow_path is None or not shadow_path.is_file():
        return None
    return _load_json_object(shadow_path, label="shadow pilot artifact")


def _validate_controlled_rollout_eval(
    payload: Mapping[str, Any],
    *,
    path: Path,
    expected_candidate_count: int,
) -> dict[str, Any]:
    if payload.get("artifact_type") != CONTROLLED_ROLLOUT_ARTIFACT_TYPE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; "
            f"expected {CONTROLLED_ROLLOUT_ARTIFACT_TYPE!r}"
        )
    if payload.get("artifact_version") != CONTROLLED_ROLLOUT_ARTIFACT_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_version={payload.get('artifact_version')!r}; "
            f"expected {CONTROLLED_ROLLOUT_ARTIFACT_VERSION!r}"
        )
    if payload.get("controlled_rollout_eval_ready") is not True:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} controlled_rollout_eval_ready is not true")
    if payload.get("recommended_next_stage") != CONTROLLED_ROLLOUT_NEXT_STAGE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} recommended_next_stage={payload.get('recommended_next_stage')!r}; "
            f"expected {CONTROLLED_ROLLOUT_NEXT_STAGE!r}"
        )
    if payload.get("ranking_run_id") != SHADOW_RANKING_RUN_ID:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} ranking_run_id={payload.get('ranking_run_id')!r}; expected {SHADOW_RANKING_RUN_ID!r}"
        )
    if _required_float(payload, "selected_frozen_coefficient_C", label=str(path)) != SELECTED_FROZEN_C:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} selected_frozen_coefficient_C must be {SELECTED_FROZEN_C}"
        )
    if _required_float(payload, "primary_alpha", label=str(path)) != PRIMARY_ALPHA:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} primary_alpha must be {PRIMARY_ALPHA}")

    prereq = payload.get("prerequisite_checks")
    if not isinstance(prereq, Mapping):
        raise MLBridgeRankPctHybridServingPlanError(f"{path} missing prerequisite_checks object")
    candidate_count = _as_int(prereq.get("candidate_count"))
    if candidate_count != expected_candidate_count:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} prerequisite candidate_count={candidate_count!r}; expected {expected_candidate_count}"
        )
    covered, total, coverage_text = _parse_coverage(prereq.get("bridge_score_coverage"))
    if covered != expected_candidate_count or total != expected_candidate_count:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} bridge_score coverage={coverage_text}; expected "
            f"{expected_candidate_count}/{expected_candidate_count}"
        )

    primary = payload.get("primary_alpha_0_5_summary")
    if not isinstance(primary, Mapping):
        raise MLBridgeRankPctHybridServingPlanError(f"{path} missing primary_alpha_0_5_summary")
    if _required_float(primary, "alpha", label="primary_alpha_0_5_summary") != PRIMARY_ALPHA:
        raise MLBridgeRankPctHybridServingPlanError("primary alpha summary is not alpha=0.5")
    risk = primary.get("risk_readouts")
    if not isinstance(risk, Mapping):
        raise MLBridgeRankPctHybridServingPlanError(f"{path} missing primary risk_readouts")
    _expect_zero(risk, "promoted_labeled_negatives_count")
    _expect_zero(risk, "promoted_unlabeled_high_risk_count")
    _expect_zero(risk, "demoted_labeled_positive_clear_loss_count")
    quality_delta = _required_float(primary, "top20_quality_delta_labeled_only", label="primary_alpha_0_5_summary")
    if quality_delta < 0:
        raise MLBridgeRankPctHybridServingPlanError(
            f"top20_quality_delta_labeled_only={quality_delta!r}; expected >= 0"
        )

    groups = primary.get("group_summaries")
    if not isinstance(groups, Mapping):
        raise MLBridgeRankPctHybridServingPlanError(f"{path} missing primary group_summaries")
    proposed_top20 = groups.get("proposed_top20")
    current_top20 = groups.get("current_top20")
    if not isinstance(proposed_top20, Mapping) or not isinstance(current_top20, Mapping):
        raise MLBridgeRankPctHybridServingPlanError("primary group_summaries missing current/proposed top20")

    return {
        "candidate_count": candidate_count,
        "bridge_score_coverage": {"covered": covered, "total": total, "as_text": coverage_text},
        "risk_readouts": {
            "promoted_labeled_negatives_count": risk.get("promoted_labeled_negatives_count"),
            "promoted_unlabeled_high_risk_count": risk.get("promoted_unlabeled_high_risk_count"),
            "demoted_labeled_positive_clear_loss_count": risk.get(
                "demoted_labeled_positive_clear_loss_count"
            ),
            "demoted_labeled_positive_competitive_count": risk.get(
                "demoted_labeled_positive_competitive_count"
            ),
        },
        "top20_quality_delta_labeled_only": quality_delta,
        "current_top20_summary": dict(current_top20),
        "proposed_top20_summary": dict(proposed_top20),
        "churn_count": primary.get("churn_count"),
        "churn_fraction": primary.get("churn_fraction"),
    }


def _expect_zero(payload: Mapping[str, Any], key: str) -> None:
    if payload.get(key) != 0:
        raise MLBridgeRankPctHybridServingPlanError(f"{key}={payload.get(key)!r}; expected 0")


def _validate_rank_pct_eval(payload: Mapping[str, Any], *, path: Path) -> None:
    if payload.get("artifact_type") != RANK_PCT_EVAL_ARTIFACT_TYPE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; expected {RANK_PCT_EVAL_ARTIFACT_TYPE!r}"
        )
    if payload.get("eval_version") != RANK_PCT_EVAL_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} eval_version={payload.get('eval_version')!r}; expected {RANK_PCT_EVAL_VERSION!r}"
        )
    if payload.get("recommended_next_stage") != RANK_PCT_NEXT_STAGE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} recommended_next_stage={payload.get('recommended_next_stage')!r}; "
            f"expected {RANK_PCT_NEXT_STAGE!r}"
        )
    if payload.get("rank_percentile_scope") != SCOPE_FULL_POOL:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} rank_percentile_scope={payload.get('rank_percentile_scope')!r}; expected {SCOPE_FULL_POOL!r}"
        )
    if _required_float(payload, "selected_frozen_coefficient_C", label=str(path)) != SELECTED_FROZEN_C:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} selected_frozen_coefficient_C must be {SELECTED_FROZEN_C}"
        )
    if _as_int(payload.get("pool_candidate_count")) != EXPECTED_CANDIDATE_COUNT:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} pool_candidate_count={payload.get('pool_candidate_count')!r}; "
            f"expected {EXPECTED_CANDIDATE_COUNT}"
        )
    if _required_float(payload, "primary_hybrid_alpha", label=str(path)) != PRIMARY_ALPHA:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} primary_hybrid_alpha must be {PRIMARY_ALPHA}")


def _validate_linear_hybrid_eval(payload: Mapping[str, Any], *, path: Path) -> None:
    if payload.get("artifact_type") != LINEAR_HYBRID_EVAL_ARTIFACT_TYPE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; "
            f"expected {LINEAR_HYBRID_EVAL_ARTIFACT_TYPE!r}"
        )
    if payload.get("eval_version") != LINEAR_HYBRID_EVAL_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} eval_version={payload.get('eval_version')!r}; expected {LINEAR_HYBRID_EVAL_VERSION!r}"
        )
    if payload.get("recommended_next_stage") != LINEAR_HYBRID_NEXT_STAGE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} recommended_next_stage={payload.get('recommended_next_stage')!r}; "
            f"expected {LINEAR_HYBRID_NEXT_STAGE!r}"
        )
    if _required_float(payload, "selected_frozen_coefficient_C", label=str(path)) != SELECTED_FROZEN_C:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} selected_frozen_coefficient_C must be {SELECTED_FROZEN_C}"
        )
    if _required_float(payload, "primary_hybrid_alpha", label=str(path)) != PRIMARY_ALPHA:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} primary_hybrid_alpha must be {PRIMARY_ALPHA}")


def _validate_sensitivity(payload: Mapping[str, Any], *, path: Path) -> Mapping[str, Any]:
    if payload.get("artifact_type") != SENSITIVITY_ARTIFACT_TYPE:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; expected {SENSITIVITY_ARTIFACT_TYPE!r}"
        )
    if payload.get("artifact_version") != SENSITIVITY_ARTIFACT_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_version={payload.get('artifact_version')!r}; expected {SENSITIVITY_ARTIFACT_VERSION!r}"
        )
    if payload.get("ready_for_offline_hybrid_eval") is not True:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} ready_for_offline_hybrid_eval is not true")
    if _required_float(payload, "selected_frozen_coefficient_C", label=str(path)) != SELECTED_FROZEN_C:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} selected_frozen_coefficient_C must be {SELECTED_FROZEN_C}"
        )
    frozen = payload.get("selected_frozen_scorer")
    if not isinstance(frozen, Mapping) or not frozen:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} missing selected_frozen_scorer object")
    if "embedding_version" not in frozen:
        raise MLBridgeRankPctHybridServingPlanError(f"{path} selected_frozen_scorer missing embedding_version")
    return frozen


def _validate_label_and_readiness(
    label_payload: Mapping[str, Any],
    readiness_payload: Mapping[str, Any],
    *,
    label_path: Path,
    readiness_path: Path,
    label_sha: str,
) -> None:
    if label_payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{label_path} dataset_version={label_payload.get('dataset_version')!r}; "
            f"expected {LABEL_DATASET_VERSION!r}"
        )
    provenance = readiness_payload.get("provenance")
    if not isinstance(provenance, Mapping):
        raise MLBridgeRankPctHybridServingPlanError(f"{readiness_path} missing provenance object")
    if provenance.get("label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{readiness_path} label_dataset_version={provenance.get('label_dataset_version')!r}; "
            f"expected {LABEL_DATASET_VERSION!r}"
        )
    if provenance.get("label_dataset_sha256") != label_sha:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{readiness_path} label_dataset_sha256 does not match {label_path}"
        )


def _validate_embeddings(
    embeddings_payload: Mapping[str, Any],
    *,
    path: Path,
    frozen_scorer: Mapping[str, Any],
) -> str:
    metadata = embeddings_payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLBridgeRankPctHybridServingPlanError(f"{path} missing metadata object")
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} artifact_version={metadata.get('artifact_version')!r}; expected {EMBEDDINGS_ARTIFACT_VERSION!r}"
        )
    embedding_version = metadata.get("embedding_version")
    if embedding_version != frozen_scorer.get("embedding_version"):
        raise MLBridgeRankPctHybridServingPlanError(
            f"{path} embedding_version={embedding_version!r} does not match selected_frozen_scorer"
        )
    return str(embedding_version)


def _run_context_from_sources(
    *,
    controlled_payload: Mapping[str, Any],
    shadow_payload: Mapping[str, Any] | None,
    embedding_version: str,
    candidate_count: int,
    bridge_score_coverage: Mapping[str, Any],
) -> dict[str, Any]:
    ranking_version = None
    corpus_snapshot_version = None
    if shadow_payload is not None:
        ranking_version = shadow_payload.get("ranking_version")
        corpus_snapshot_version = shadow_payload.get("corpus_snapshot_version")
        shadow_embedding = shadow_payload.get("embedding_version")
        if shadow_embedding is not None and shadow_embedding != embedding_version:
            raise MLBridgeRankPctHybridServingPlanError(
                f"shadow pilot embedding_version={shadow_embedding!r}; expected {embedding_version!r}"
            )
        shadow_count = _as_int(shadow_payload.get("candidate_count"))
        if shadow_count is not None and shadow_count != candidate_count:
            raise MLBridgeRankPctHybridServingPlanError(
                f"shadow pilot candidate_count={shadow_count!r}; expected {candidate_count}"
            )

    return {
        "ranking_run_id": controlled_payload.get("ranking_run_id"),
        "ranking_run_id_mismatch_behavior": "fail_closed",
        "ranking_version": ranking_version,
        "ranking_version_source": "shadow_pilot_artifact" if ranking_version else "not_present_in_file_artifacts",
        "ranking_version_mismatch_behavior": "fail_closed_if_available",
        "corpus_snapshot_version": corpus_snapshot_version,
        "corpus_snapshot_version_source": (
            "shadow_pilot_artifact" if corpus_snapshot_version else "not_present_in_file_artifacts"
        ),
        "corpus_snapshot_version_mismatch_behavior": "fail_closed_if_available",
        "embedding_version": embedding_version,
        "candidate_count": candidate_count,
        "bridge_score_coverage": dict(bridge_score_coverage),
    }


def build_ml_bridge_rank_pct_hybrid_serving_plan_payload(
    *,
    controlled_rollout_eval_path: Path,
    rank_pct_eval_artifact_path: Path,
    linear_hybrid_eval_v3_path: Path,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    expected_candidate_count: int = EXPECTED_CANDIDATE_COUNT,
) -> dict[str, Any]:
    paths = [
        controlled_rollout_eval_path.resolve(),
        rank_pct_eval_artifact_path.resolve(),
        linear_hybrid_eval_v3_path.resolve(),
        sensitivity_artifact_path.resolve(),
        label_dataset_path.resolve(),
        readiness_matrix_path.resolve(),
        embeddings_provenance_path.resolve(),
    ]
    for path in paths:
        if not path.is_file():
            raise MLBridgeRankPctHybridServingPlanError(f"required input not found: {path}")

    (
        controlled_path,
        rank_pct_path,
        linear_hybrid_path,
        sensitivity_path,
        label_path,
        readiness_path,
        embeddings_path,
    ) = paths
    controlled_payload = _load_json_object(controlled_path, label="controlled rollout eval")
    controlled_summary = _validate_controlled_rollout_eval(
        controlled_payload,
        path=controlled_path,
        expected_candidate_count=expected_candidate_count,
    )
    rank_pct_payload = _load_json_object(rank_pct_path, label="rank-pct eval")
    _validate_rank_pct_eval(rank_pct_payload, path=rank_pct_path)
    linear_hybrid_payload = _load_json_object(linear_hybrid_path, label="linear hybrid eval v3")
    _validate_linear_hybrid_eval(linear_hybrid_payload, path=linear_hybrid_path)
    sensitivity_payload = _load_json_object(sensitivity_path, label="regularization sensitivity")
    frozen_scorer = _validate_sensitivity(sensitivity_payload, path=sensitivity_path)
    label_payload = _load_json_object(label_path, label="label dataset")
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    label_sha = sha256_file(label_path)
    _validate_label_and_readiness(
        label_payload,
        readiness_payload,
        label_path=label_path,
        readiness_path=readiness_path,
        label_sha=label_sha,
    )
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")
    embedding_version = _validate_embeddings(embeddings_payload, path=embeddings_path, frozen_scorer=frozen_scorer)
    shadow_payload = _load_optional_shadow_pilot(controlled_payload)

    shas = {
        "controlled_rollout_eval": sha256_file(controlled_path),
        "rank_pct_eval_artifact": sha256_file(rank_pct_path),
        "linear_hybrid_eval_v3": sha256_file(linear_hybrid_path),
        "sensitivity_artifact": sha256_file(sensitivity_path),
        "label_dataset": label_sha,
        "readiness_matrix": sha256_file(readiness_path),
        "embeddings_provenance": sha256_file(embeddings_path),
    }
    inputs = [
        _input_record("controlled_rollout_eval", controlled_path, shas["controlled_rollout_eval"]),
        _input_record("rank_pct_eval_artifact", rank_pct_path, shas["rank_pct_eval_artifact"]),
        _input_record("linear_hybrid_eval_v3", linear_hybrid_path, shas["linear_hybrid_eval_v3"]),
        _input_record("sensitivity_artifact", sensitivity_path, shas["sensitivity_artifact"]),
        _input_record("label_dataset", label_path, shas["label_dataset"]),
        _input_record("readiness_matrix", readiness_path, shas["readiness_matrix"]),
        _input_record("embeddings_provenance", embeddings_path, shas["embeddings_provenance"]),
    ]

    run_context = _run_context_from_sources(
        controlled_payload=controlled_payload,
        shadow_payload=shadow_payload,
        embedding_version=embedding_version,
        candidate_count=int(controlled_summary["candidate_count"]),
        bridge_score_coverage=controlled_summary["bridge_score_coverage"],
    )

    return {
        "artifact_type": ARTIFACT_TYPE,
        "plan_version": PLAN_VERSION,
        "generated_at": _now_iso_z(),
        "target_surface": TARGET_SURFACE,
        "ranking_run_id": SHADOW_RANKING_RUN_ID,
        "scorer": SCORER,
        "target": TARGET,
        "selected_frozen_coefficient_C": SELECTED_FROZEN_C,
        "primary_alpha": PRIMARY_ALPHA,
        "exploratory_alpha": EXPLORATORY_ALPHA,
        "formula": FORMULA,
        "rank_pct_formula": RANK_PCT_FORMULA,
        "rank_pct_scope": SCOPE_FULL_POOL,
        "future_serving_algorithm_reference": list(ALGORITHM_REFERENCES),
        "inputs": inputs,
        "preconditions": {
            "controlled_rollout_eval_ready": True,
            "controlled_rollout_recommended_next_stage": controlled_payload.get("recommended_next_stage"),
            "rank_pct_eval_recommended_next_stage": rank_pct_payload.get("recommended_next_stage"),
            "linear_hybrid_eval_recommended_next_stage": linear_hybrid_payload.get("recommended_next_stage"),
            "linear_hybrid_eval_included_as_negative_guardrail": True,
            "regularization_sensitivity_ready_for_offline_hybrid_eval": True,
            "selected_frozen_scorer_present": True,
            "selected_frozen_coefficient_C": SELECTED_FROZEN_C,
            "bridge_score_coverage": controlled_summary["bridge_score_coverage"],
            "primary_alpha_source": "primary_alpha_0_5_summary",
            "primary_alpha_is_0_5_not_0_7": True,
            "risk_readouts": controlled_summary["risk_readouts"],
            "top20_quality_delta_labeled_only": controlled_summary["top20_quality_delta_labeled_only"],
            "top20_quality_delta_labeled_only_gte_0": True,
        },
        "controlled_rollout_evidence": {
            "full_top20_churn_count": controlled_summary["churn_count"],
            "full_top20_churn_denominator": SCORER_SERVED_LIMIT,
            "full_top20_churn_fraction": controlled_summary["churn_fraction"],
            "current_top20_labeled_precision": controlled_summary["current_top20_summary"].get(
                "labeled_precision"
            ),
            "proposed_top20_labeled_precision": controlled_summary["proposed_top20_summary"].get(
                "labeled_precision"
            ),
            "proposed_top20_labeled_count": controlled_summary["proposed_top20_summary"].get("labeled_count"),
            "proposed_top20_unlabeled_count": controlled_summary["proposed_top20_summary"].get(
                "unlabeled_count"
            ),
            "demoted_labeled_positive_competitive_count": controlled_summary["risk_readouts"].get(
                "demoted_labeled_positive_competitive_count"
            ),
        },
        "pinned_run_context": run_context,
        "frozen_scorer_load_contract": {
            "sensitivity_artifact_path": portable_repo_path(sensitivity_path),
            "sensitivity_artifact_sha256": shas["sensitivity_artifact"],
            "embeddings_provenance_path": portable_repo_path(embeddings_path),
            "embeddings_provenance_sha256": shas["embeddings_provenance"],
            "selected_frozen_scorer_present": True,
            "embedding_version": embedding_version,
            "selected_frozen_coefficient_C": SELECTED_FROZEN_C,
            "primary_alpha": PRIMARY_ALPHA,
            "scorer_probability_source": "full_pool_frozen_inference_not_oof",
            "full_pool_frozen_inference_statement": (
                "Future serving uses full-pool frozen inference, not OOF probabilities."
            ),
        },
        "serving_scope": {
            "route": "/api/v1/recommendations/ranked",
            "family": "bridge",
            "ranking_run_id": SHADOW_RANKING_RUN_ID,
            "limit": SCORER_SERVED_LIMIT,
            "limit_contract": "20 only for scorer-served Bridge response",
            "no_emerging_changes": True,
            "no_undercited_changes": True,
            "no_broad_fleet_default_rollout": True,
            "no_bridge_scorer_serving_when_flag_env_missing": True,
            "scoring_scope": {
                "load_all_bridge_candidates_for_pinned_run": True,
                "current_pool_scale": expected_candidate_count,
                "score_only_top20": False,
                "recompute_ml_probabilities_over_full_bridge_candidate_pool": True,
                "recompute_ml_rank_percentiles_over_full_bridge_candidate_pool": True,
                "recompute_bridge_score_rank_percentiles_over_full_bridge_candidate_pool": True,
                "return_top_limit_by_hybrid_rank": True,
                "hybrid_reorder_only": True,
                "final_score_and_existing_signal_fields_remain_materialized_metadata": True,
                "ranking_mode_detail_explains_rank_pct_hybrid_ordering": True,
            },
        },
        "bridge_gate_contract": {
            "requires_new_bridge_only_gate_module": True,
            "proposed_gate_module": "apps/api/app/ml_bridge_scorer_rollout_gate.py",
            "required_env_prefix": "ML_BRIDGE_SCORER_V1_",
            "do_not_extend_env_prefix": "ML_SHADOW_SCORER_V1_",
            "do_not_extend_modules": [
                "apps/api/app/ml_scorer_rollout_gate.py",
                "apps/api/app/ml_scorer_rollout.py",
            ],
            "current_emerging_gate_blocks_bridge_requests": True,
            "preserve_current_emerging_gate_behavior": True,
        },
        "required_future_env": {
            "variables": BRIDGE_ENV_DEFAULTS,
            "example_pinned_ranking_run_env": f"ML_BRIDGE_SCORER_V1_RANKING_RUN_ID={SHADOW_RANKING_RUN_ID}",
            "defaults_summary": (
                "All missing, false, zero, empty, or ranking-run-mismatched Bridge env values close the scorer path."
            ),
        },
        "api_response_contract": {
            "RankedRankingMode_future_values": [
                "materialized_heuristic",
                "bounded_ml_scorer",
                "bounded_bridge_ml_scorer",
            ],
            "bridge_ranking_mode": "bounded_bridge_ml_scorer",
            "do_not_reuse_bounded_ml_scorer_for_bridge": True,
            "required_response_fields": [
                "ranking_mode",
                "ranking_mode_detail",
                "scorer_surface",
                "bridge_recommendations_ml_served",
                "bridge_rank_pct_hybrid_alpha",
                "bridge_rank_pct_scope",
                "emitted_to_public_users",
            ],
        },
        "web_display_contract": {
            "must_not_call_bridge_validated": True,
            "allowed_wording": [
                "Bridge order selected by bounded ML scorer rollout",
                "Experimental Bridge ranking",
                "Blends bridge_score with a frozen Bridge ML scorer",
                "Still under evaluation",
            ],
            "display_behavior_matches_emerging": True,
            "ordering_may_change": True,
            "materialized_final_score_and_signal_fields_remain_metadata": True,
            "ranking_mode_detail_explains_scorer_ordering": True,
        },
        "failure_fallback_behavior": {
            "fallback_to": "current_materialized_bridge_ranking",
            "fail_closed_conditions": list(FAIL_CLOSED_CONDITIONS),
        },
        "observability": {
            "metadata_only": True,
            "log_fields": [
                "ranking_mode",
                "family",
                "route",
                "gate decision",
                "reason closed",
                "current_served",
                "cap",
                "ranking_run_id",
                "public_rollout_enabled",
                "public_rollout_percent",
            ],
            "do_not_log": ["user IDs", "full paper payloads"],
        },
        "known_caveats": list(CAVEATS),
        "does_not_authorize": [
            "Bridge ML serving",
            "broad production rollout",
            "production default changes",
            "API behavior changes",
            "web UI behavior changes",
            "database writes",
        ],
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "fallback_next_stage_if_preconditions_fail": FAILURE_NEXT_STAGE,
    }


def markdown_from_ml_bridge_rank_pct_hybrid_serving_plan(payload: Mapping[str, Any]) -> str:
    preconditions = payload.get("preconditions", {})
    evidence = payload.get("controlled_rollout_evidence", {})
    scope = payload.get("serving_scope", {})
    gate = payload.get("bridge_gate_contract", {})
    api = payload.get("api_response_contract", {})
    web = payload.get("web_display_contract", {})
    fallback = payload.get("failure_fallback_behavior", {})
    observability = payload.get("observability", {})
    run_context = payload.get("pinned_run_context", {})
    frozen = payload.get("frozen_scorer_load_contract", {})
    ranking_version = run_context.get("ranking_version") or run_context.get("ranking_version_source")
    corpus_snapshot_version = (
        run_context.get("corpus_snapshot_version") or run_context.get("corpus_snapshot_version_source")
    )

    lines = [
        "# Bridge rank-percentile hybrid serving plan v1",
        "",
        "Planning artifact for the next bounded Bridge serving-gate implementation. "
        "This file does not change production serving behavior.",
        "",
        "## Plan Identity",
        "",
        f"- artifact_type: `{payload.get('artifact_type')}`",
        f"- plan_version: `{payload.get('plan_version')}`",
        f"- target_surface: `{payload.get('target_surface')}`",
        f"- ranking_run_id: `{payload.get('ranking_run_id')}`",
        f"- scorer: `{payload.get('scorer')}`",
        f"- selected_frozen_coefficient_C: `{payload.get('selected_frozen_coefficient_C')}`",
        f"- primary_alpha: `{payload.get('primary_alpha')}`",
        f"- exploratory_alpha: `{payload.get('exploratory_alpha')}`",
        f"- formula: `{payload.get('formula')}`",
        f"- rank_pct_formula: `{payload.get('rank_pct_formula')}`",
        f"- rank_pct_scope: `{payload.get('rank_pct_scope')}`",
        "",
        "## Validated Preconditions",
        "",
        "| check | value |",
        "| --- | --- |",
        f"| controlled_rollout_eval_ready | `{preconditions.get('controlled_rollout_eval_ready')}` |",
        f"| controlled rollout next stage | `{preconditions.get('controlled_rollout_recommended_next_stage')}` |",
        f"| rank-pct eval next stage | `{preconditions.get('rank_pct_eval_recommended_next_stage')}` |",
        f"| linear hybrid guardrail next stage | `{preconditions.get('linear_hybrid_eval_recommended_next_stage')}` |",
        f"| sensitivity ready | `{preconditions.get('regularization_sensitivity_ready_for_offline_hybrid_eval')}` |",
        f"| selected frozen scorer present | `{preconditions.get('selected_frozen_scorer_present')}` |",
        f"| bridge_score coverage | `{preconditions.get('bridge_score_coverage', {}).get('as_text')}` |",
        f"| primary alpha source | `{preconditions.get('primary_alpha_source')}` |",
        f"| top20_quality_delta_labeled_only | `{preconditions.get('top20_quality_delta_labeled_only')}` |",
        "",
        "## Controlled Rollout Evidence",
        "",
        f"- Full top-20 churn: `{evidence.get('full_top20_churn_count')}/"
        f"{evidence.get('full_top20_churn_denominator')}`",
        f"- Current top-20 labeled precision: `{evidence.get('current_top20_labeled_precision')}`",
        f"- Proposed hybrid top-20 labeled precision: `{evidence.get('proposed_top20_labeled_precision')}`",
        f"- Proposed hybrid top-20 labels: `{evidence.get('proposed_top20_labeled_count')}` labeled, "
        f"`{evidence.get('proposed_top20_unlabeled_count')}` unlabeled",
        f"- Demoted labeled positives classified competitive: "
        f"`{evidence.get('demoted_labeled_positive_competitive_count')}`",
        "",
        "## Pinned Run Context",
        "",
        f"- ranking_run_id: `{run_context.get('ranking_run_id')}`",
        f"- ranking_version: `{ranking_version}` "
        f"({run_context.get('ranking_version_mismatch_behavior')})",
        f"- corpus_snapshot_version: `{corpus_snapshot_version}` "
        f"({run_context.get('corpus_snapshot_version_mismatch_behavior')})",
        f"- embedding_version: `{run_context.get('embedding_version')}`",
        f"- candidate_count: `{run_context.get('candidate_count')}`",
        f"- bridge_score_coverage: `{run_context.get('bridge_score_coverage', {}).get('as_text')}`",
        "",
        "Future serving must fail closed on `ranking_run_id` mismatch. If "
        "`ranking_version` or `corpus_snapshot_version` are available in later "
        "artifacts or API context, serving must also fail closed on those mismatches.",
        "",
        "## Frozen Scorer Load Contract",
        "",
        f"- sensitivity artifact: `{frozen.get('sensitivity_artifact_path')}`",
        f"- sensitivity SHA256: `{frozen.get('sensitivity_artifact_sha256')}`",
        f"- embeddings provenance: `{frozen.get('embeddings_provenance_path')}`",
        f"- embeddings SHA256: `{frozen.get('embeddings_provenance_sha256')}`",
        f"- scorer_probability_source: `{frozen.get('scorer_probability_source')}`",
        f"- {frozen.get('full_pool_frozen_inference_statement')}",
        "",
        "## Serving Scope",
        "",
        f"- route: `{scope.get('route')}`",
        f"- family: `{scope.get('family')}` only",
        f"- ranking_run_id: `{scope.get('ranking_run_id')}` only, unless explicitly extended later",
        f"- limit: `{scope.get('limit')}` only for scorer-served Bridge responses",
        "- No Emerging changes, no Undercited changes, and no broad/fleet/default rollout.",
        "- Load all Bridge candidates for the pinned run, recompute both rank-percentile inputs over the full pool, and return the top limit by hybrid rank.",
        "- Hybrid reorder only: `final_score` and existing signal fields remain materialized metadata while `ranking_mode_detail` explains the rank-pct hybrid ordering.",
        "",
        "## Bridge Gate Contract",
        "",
        f"- New Bridge-only gate module required: `{gate.get('proposed_gate_module')}`",
        "- Use `ML_BRIDGE_SCORER_V1_*` environment variables.",
        "- Do not extend `ML_SHADOW_SCORER_V1_*`, `ml_scorer_rollout_gate.py`, or `ml_scorer_rollout.py` for Bridge.",
        "- Preserve the current Emerging gate behavior that blocks Bridge requests.",
        "",
        "## API Response Contract",
        "",
        f"- `RankedRankingMode` future values: `{api.get('RankedRankingMode_future_values')}`",
        "- Bridge scorer responses use `bounded_bridge_ml_scorer`, not `bounded_ml_scorer`.",
        f"- Required response fields: `{api.get('required_response_fields')}`",
        "",
        "## Web Display Contract",
        "",
        "- Web copy must not call Bridge \"validated.\"",
        "- Allowed wording:",
    ]
    for wording in web.get("allowed_wording", []):
        lines.append(f"  - {wording}")
    lines.extend(
        [
            "- Ordering may change; materialized score/signal fields remain metadata.",
            "",
            "## Failure And Fallback",
            "",
            f"- Fallback: `{fallback.get('fallback_to')}`",
        ]
    )
    for condition in fallback.get("fail_closed_conditions", []):
        lines.append(f"- Fail closed when: {condition}")
    lines.extend(
        [
            "",
            "## Observability",
            "",
            "- Log metadata only.",
            f"- Fields: `{observability.get('log_fields')}`",
            f"- Do not log: `{observability.get('do_not_log')}`",
            "",
            "## Caveats",
            "",
        ]
    )
    for caveat in payload.get("known_caveats", []):
        lines.append(f"- {caveat}")
    lines.extend(
        [
            "",
            "## Next Stage",
            "",
            f"- **recommended_next_stage:** `{payload.get('recommended_next_stage')}`",
            f"- If preconditions fail: `{payload.get('fallback_next_stage_if_preconditions_fail')}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_bridge_rank_pct_hybrid_serving_plan(
    *,
    controlled_rollout_eval_path: Path,
    rank_pct_eval_artifact_path: Path,
    linear_hybrid_eval_v3_path: Path,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
) -> dict[str, Any]:
    payload = build_ml_bridge_rank_pct_hybrid_serving_plan_payload(
        controlled_rollout_eval_path=controlled_rollout_eval_path,
        rank_pct_eval_artifact_path=rank_pct_eval_artifact_path,
        linear_hybrid_eval_v3_path=linear_hybrid_eval_v3_path,
        sensitivity_artifact_path=sensitivity_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_output is not None:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(
            markdown_from_ml_bridge_rank_pct_hybrid_serving_plan(payload),
            encoding="utf-8",
        )
    return payload


def run_ml_bridge_rank_pct_hybrid_serving_plan_cli(
    *,
    controlled_rollout_eval_path: Path,
    rank_pct_eval_artifact_path: Path,
    linear_hybrid_eval_v3_path: Path,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
) -> dict[str, Any]:
    return write_ml_bridge_rank_pct_hybrid_serving_plan(
        controlled_rollout_eval_path=controlled_rollout_eval_path,
        rank_pct_eval_artifact_path=rank_pct_eval_artifact_path,
        linear_hybrid_eval_v3_path=linear_hybrid_eval_v3_path,
        sensitivity_artifact_path=sensitivity_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        output_json=output_json,
        markdown_output=markdown_output,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLBridgeRankPctHybridServingPlanError",
    "build_ml_bridge_rank_pct_hybrid_serving_plan_payload",
    "markdown_from_ml_bridge_rank_pct_hybrid_serving_plan",
    "run_ml_bridge_rank_pct_hybrid_serving_plan_cli",
    "write_ml_bridge_rank_pct_hybrid_serving_plan",
]
