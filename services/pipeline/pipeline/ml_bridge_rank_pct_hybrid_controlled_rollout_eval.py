"""Offline controlled rollout replay for Bridge rank-percentile hybrid scoring.

Compares the current Bridge top-20 from the shadow-pilot artifact with a proposed
rank-percentile hybrid top-20 using the frozen v3 C=0.001 scorer. This is file-only
apart from SELECT-only embedding reads; it does not write to the database or change
serving/API behavior.
"""

from __future__ import annotations

import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
    EMBEDDINGS_ARTIFACT_VERSION,
    LABEL_DATASET_VERSION,
    SELECTED_FROZEN_C,
    SENSITIVITY_ARTIFACT_VERSION,
    SHADOW_POOL_VARIANT,
    SHADOW_RANKING_RUN_ID,
    TARGET,
    _as_float,
    _load_json_object,
    _validate_embeddings_provenance,
    _validate_prerequisite_sensitivity_artifact,
    _validate_readiness_matrix,
    _work_id_token,
)
from pipeline.ml_offline_bridge_hybrid_rank_pct_eval_v3 import (
    ARTIFACT_TYPE as RANK_PCT_EVAL_ARTIFACT_TYPE,
    EVAL_VERSION as RANK_PCT_EVAL_VERSION,
    SCOPE_FULL_POOL,
    _compute_rank_percentiles_from_pool,
    _load_shadow_pilot_candidates,
    _score_pool_ml_probabilities,
)
from pipeline.repo_paths import portable_repo_path

SCORER_VERSION = "ml-offline-bridge-hybrid-eval-v3-v1"
ARTIFACT_TYPE = "ml_bridge_rank_pct_hybrid_controlled_rollout_eval"
ARTIFACT_VERSION = "ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1"
SHADOW_PILOT_ARTIFACT_TYPE = "ml_bridge_shadow_pilot"
SHADOW_PILOT_VERSION = "ml-bridge-shadow-pilot-v1"
EXPECTED_POOL_CANDIDATES = 528
PRIMARY_ALPHA = 0.5
EXPLORATORY_ALPHA = 0.7
HYBRID_ALPHAS = (PRIMARY_ALPHA, EXPLORATORY_ALPHA)
TOP_K = 20
HIGH_RISK_ML_PROB_THRESHOLD = 0.25
HIGH_RISK_ML_RANK_PCT_THRESHOLD = 0.10

POOL_NEG = "ml_bridge_negative_mining_audit"
POOL_TOP = "ml_bridge_top_ranked_validation_audit"
POOL_SHADOW = SHADOW_POOL_VARIANT
ALLOWED_LABEL_POOLS = (POOL_NEG, POOL_TOP, POOL_SHADOW)
POOL_PRIORITY = {POOL_SHADOW: 0, POOL_TOP: 1, POOL_NEG: 2}

CAVEATS = (
    "Offline controlled rollout replay only.",
    "Does not enable Bridge serving.",
    "Uses frozen v3 C=0.001 scorer.",
    "Full-pool ML probabilities are frozen-model inference, not OOF estimates.",
    "Top-20 labeled precision is underpowered; most pool papers are unlabeled.",
    "Label overlay is incomplete; unlabeled promoted papers may need review.",
    "No API/web/production behavior changed.",
)


class MLBridgeRankPctHybridControlledRolloutEvalError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _alpha_key(alpha: float) -> str:
    return f"alpha_{str(alpha).replace('.', '_')}"


def _validate_rank_pct_eval_artifact(payload: dict[str, Any], *, path: Path) -> None:
    if payload.get("artifact_type") != RANK_PCT_EVAL_ARTIFACT_TYPE:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; "
            f"expected {RANK_PCT_EVAL_ARTIFACT_TYPE!r}"
        )
    if payload.get("eval_version") != RANK_PCT_EVAL_VERSION:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} eval_version={payload.get('eval_version')!r}; expected {RANK_PCT_EVAL_VERSION!r}"
        )
    if payload.get("rank_percentile_scope") != SCOPE_FULL_POOL:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} rank_percentile_scope={payload.get('rank_percentile_scope')!r}; "
            f"expected {SCOPE_FULL_POOL!r}"
        )
    if payload.get("recommended_next_stage") != "authorize_bridge_hybrid_serving_controlled_rollout_eval":
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} recommended_next_stage does not authorize controlled rollout eval"
        )
    if payload.get("hybrid_rescue_confirmed") is not True:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} hybrid_rescue_confirmed is not true"
        )


def _validate_shadow_pilot_artifact(
    payload: dict[str, Any],
    *,
    path: Path,
    expected_candidate_count: int = EXPECTED_POOL_CANDIDATES,
) -> list[dict[str, Any]]:
    if expected_candidate_count == EXPECTED_POOL_CANDIDATES:
        _load_shadow_pilot_candidates(payload, path=path)
    if payload.get("artifact_type") != SHADOW_PILOT_ARTIFACT_TYPE:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; expected {SHADOW_PILOT_ARTIFACT_TYPE!r}"
        )
    if payload.get("pilot_version") != SHADOW_PILOT_VERSION:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} pilot_version={payload.get('pilot_version')!r}; expected {SHADOW_PILOT_VERSION!r}"
        )
    if payload.get("ranking_run_id") != SHADOW_RANKING_RUN_ID:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} ranking_run_id={payload.get('ranking_run_id')!r}; expected {SHADOW_RANKING_RUN_ID!r}"
        )
    candidates = payload.get("all_candidates")
    if not isinstance(candidates, list):
        raise MLBridgeRankPctHybridControlledRolloutEvalError(f"{path} missing all_candidates array")
    if payload.get("candidate_count") != expected_candidate_count or len(candidates) != expected_candidate_count:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"{path} candidate_count={payload.get('candidate_count')!r} len(all_candidates)={len(candidates)}; "
            f"expected {expected_candidate_count}"
        )

    out: list[dict[str, Any]] = []
    for idx, row in enumerate(candidates, start=1):
        if not isinstance(row, dict):
            raise MLBridgeRankPctHybridControlledRolloutEvalError(f"all_candidates[{idx}] is not an object")
        token = _work_id_token(row.get("work_id_token") or row.get("openalex_id"))
        work_id_int = row.get("work_id_int")
        rank = row.get("current_family_rank")
        if not isinstance(work_id_int, int) or isinstance(work_id_int, bool):
            raise MLBridgeRankPctHybridControlledRolloutEvalError(
                f"all_candidates[{idx}] missing integer work_id_int"
            )
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise MLBridgeRankPctHybridControlledRolloutEvalError(
                f"all_candidates[{idx}] missing integer current_family_rank"
            )
        out.append(
            {
                "work_id_int": work_id_int,
                "work_id_token": token,
                "openalex_id": row.get("openalex_id"),
                "title": str(row.get("title") or ""),
                "bridge_score_raw": _as_float(row.get("bridge_score")),
                "current_family_rank": rank,
                "historical_shadow_pilot_ml_probability": _as_float(row.get("ml_probability")),
                "historical_shadow_pilot_ml_rank_pct": _as_float(row.get("ml_rank_pct")),
                "historical_shadow_pilot_hybrid_score": _as_float(row.get("hybrid_score")),
                "historical_shadow_pilot_hybrid_rank": row.get("hybrid_rank"),
            }
        )
    covered = sum(1 for row in out if row["bridge_score_raw"] is not None)
    if covered != expected_candidate_count:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"shadow pilot bridge_score coverage must be complete: {covered}/{expected_candidate_count}"
        )
    return out


def _validate_label_dataset_version(label_payload: dict[str, Any]) -> None:
    if label_payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLBridgeRankPctHybridControlledRolloutEvalError(
            f"label dataset version={label_payload.get('dataset_version')!r}; expected {LABEL_DATASET_VERSION!r}"
        )


def _label_overlay_by_work_id(label_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    _validate_label_dataset_version(label_payload)
    rows = label_payload.get("rows")
    if not isinstance(rows, list):
        raise MLBridgeRankPctHybridControlledRolloutEvalError("label dataset missing rows array")
    by_work: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("split") != "audit_only" or row.get("family") != "bridge":
            continue
        pool = row.get("review_pool_variant")
        if pool not in ALLOWED_LABEL_POOLS:
            continue
        token = _work_id_token(row.get("work_id") or row.get("openalex_work_id") or row.get("paper_id"))
        by_work.setdefault(token, []).append(row)

    overlay: dict[str, dict[str, Any]] = {}
    for token, group in by_work.items():
        winner = min(group, key=lambda row: POOL_PRIORITY.get(str(row.get("review_pool_variant")), 999))
        label = winner.get(TARGET)
        overlay[token] = {
            "bridge_recommendable": label if label in (True, False) else None,
            "label_source_review_pool_variant": winner.get("review_pool_variant"),
            "label_source_row_id": winner.get("row_id"),
            "label_conflict_count": len({row.get(TARGET) for row in group if row.get(TARGET) in (True, False)}),
            "duplicate_label_row_count": len(group),
        }
    return overlay


def _rank_from_score(candidates: list[dict[str, Any]], *, score_field: str, rank_field: str) -> None:
    ranked = sorted(
        candidates,
        key=lambda row: (
            -float(row[score_field]) if _as_float(row.get(score_field)) is not None else float("inf"),
            str(row.get("work_id_token") or ""),
        ),
    )
    for rank, row in enumerate(ranked, start=1):
        row[rank_field] = rank


def _recompute_candidate_scores(
    candidates: list[dict[str, Any]],
    *,
    ml_prob_by_token: Mapping[str, float],
) -> list[dict[str, Any]]:
    pool_for_pct = [
        {
            "work_id_token": row["work_id_token"],
            "bridge_score": row["bridge_score_raw"],
        }
        for row in candidates
    ]
    ml_rank_pct_by_token, bridge_rank_pct_by_token = _compute_rank_percentiles_from_pool(
        pool_for_pct,
        ml_prob_by_token=ml_prob_by_token,
    )
    out = [dict(row) for row in candidates]
    for row in out:
        token = str(row["work_id_token"])
        if token not in ml_prob_by_token:
            raise MLBridgeRankPctHybridControlledRolloutEvalError(
                f"missing recomputed v3 ML probability for candidate {token!r}"
            )
        row["v3_ml_probability"] = float(ml_prob_by_token[token])
        row["v3_ml_rank_pct"] = float(ml_rank_pct_by_token[token])
        row["bridge_score_rank_pct"] = float(bridge_rank_pct_by_token[token])
    _rank_from_score(out, score_field="v3_ml_probability", rank_field="v3_ml_rank")
    _rank_from_score(out, score_field="bridge_score_raw", rank_field="bridge_score_rank")
    for alpha in HYBRID_ALPHAS:
        score_field = f"hybrid_rank_pct_score_{_alpha_key(alpha)}"
        rank_field = f"hybrid_rank_{_alpha_key(alpha)}"
        for row in out:
            row[score_field] = (
                alpha * float(row["v3_ml_rank_pct"])
                + (1.0 - alpha) * float(row["bridge_score_rank_pct"])
            )
        _rank_from_score(out, score_field=score_field, rank_field=rank_field)
    return out


def _apply_label_overlay(candidates: list[dict[str, Any]], overlay: Mapping[str, Mapping[str, Any]]) -> None:
    for row in candidates:
        label = overlay.get(str(row["work_id_token"]))
        if label is None:
            row["bridge_recommendable"] = None
            row["label_source_review_pool_variant"] = None
            row["label_source_row_id"] = None
        else:
            row.update(label)


def _compact_row(row: Mapping[str, Any], *, alpha: float | None = None) -> dict[str, Any]:
    out = {
        "work_id_token": row.get("work_id_token"),
        "work_id_int": row.get("work_id_int"),
        "title": row.get("title"),
        "current_family_rank": row.get("current_family_rank"),
        "bridge_recommendable": row.get("bridge_recommendable"),
        "label_source_review_pool_variant": row.get("label_source_review_pool_variant"),
        "v3_ml_probability": row.get("v3_ml_probability"),
        "v3_ml_rank_pct": row.get("v3_ml_rank_pct"),
        "bridge_score_raw": row.get("bridge_score_raw"),
        "bridge_score_rank_pct": row.get("bridge_score_rank_pct"),
    }
    if alpha is not None:
        key = _alpha_key(alpha)
        out[f"hybrid_rank_pct_score_{key}"] = row.get(f"hybrid_rank_pct_score_{key}")
        out[f"hybrid_rank_{key}"] = row.get(f"hybrid_rank_{key}")
    return out


def _group_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pos = sum(1 for row in rows if row.get("bridge_recommendable") is True)
    neg = sum(1 for row in rows if row.get("bridge_recommendable") is False)
    labeled = pos + neg
    return {
        "row_count": len(rows),
        "labeled_count": labeled,
        "labeled_positive_count": pos,
        "labeled_negative_count": neg,
        "unlabeled_count": len(rows) - labeled,
        "labeled_precision": (pos / labeled) if labeled else None,
    }


def _rows_by_tokens(candidates: Sequence[Mapping[str, Any]], tokens: set[str]) -> list[dict[str, Any]]:
    return [dict(row) for row in candidates if str(row.get("work_id_token")) in tokens]


def _top_tokens(candidates: Sequence[Mapping[str, Any]], *, rank_field: str, top_k: int) -> set[str]:
    return {
        str(row["work_id_token"])
        for row in candidates
        if isinstance(row.get(rank_field), int) and int(row[rank_field]) <= top_k
    }


def _arm_top20_quality(candidates: Sequence[Mapping[str, Any]], *, top_k: int) -> dict[str, Any]:
    arms = {
        "current_bridge": "current_family_rank",
        "pure_ml": "v3_ml_rank",
        "pure_bridge": "bridge_score_rank",
    }
    for alpha in HYBRID_ALPHAS:
        arms[f"hybrid_{_alpha_key(alpha)}"] = f"hybrid_rank_{_alpha_key(alpha)}"
    out: dict[str, Any] = {}
    for arm, rank_field in arms.items():
        rows = _rows_by_tokens(candidates, _top_tokens(candidates, rank_field=rank_field, top_k=top_k))
        out[arm] = _group_summary(rows)
    return out


def _classify_demoted_labeled_positives(
    demoted_rows: Sequence[Mapping[str, Any]],
    *,
    alpha: float,
) -> list[dict[str, Any]]:
    ml_probs = [
        float(row["v3_ml_probability"])
        for row in demoted_rows
        if _as_float(row.get("v3_ml_probability")) is not None
    ]
    median_ml = statistics.median(ml_probs) if ml_probs else None
    out: list[dict[str, Any]] = []
    for row in demoted_rows:
        if row.get("bridge_recommendable") is not True:
            continue
        item = _compact_row(row, alpha=alpha)
        competitive = median_ml is not None and float(row["v3_ml_probability"]) >= median_ml
        item["demotion_classification"] = "competitive" if competitive else "clear_loss"
        item["demoted_rows_v3_ml_probability_median"] = median_ml
        out.append(item)
    return out


def _risk_readouts(
    *,
    promoted_rows: Sequence[Mapping[str, Any]],
    demoted_rows: Sequence[Mapping[str, Any]],
    stable_rows: Sequence[Mapping[str, Any]],
    alpha: float,
) -> dict[str, Any]:
    promoted_labeled_negatives = [
        _compact_row(row, alpha=alpha)
        for row in promoted_rows
        if row.get("bridge_recommendable") is False
    ]
    demoted_labeled_positives = _classify_demoted_labeled_positives(demoted_rows, alpha=alpha)
    promoted_unlabeled_high_risk = [
        _compact_row(row, alpha=alpha)
        for row in promoted_rows
        if row.get("bridge_recommendable") is None
        and (
            float(row["v3_ml_probability"]) < HIGH_RISK_ML_PROB_THRESHOLD
            or float(row["v3_ml_rank_pct"]) < HIGH_RISK_ML_RANK_PCT_THRESHOLD
        )
    ]
    stable_labeled_positives = [
        _compact_row(row, alpha=alpha)
        for row in stable_rows
        if row.get("bridge_recommendable") is True
    ]
    return {
        "risk_thresholds": {
            "promoted_unlabeled_high_risk_v3_ml_probability_lt": HIGH_RISK_ML_PROB_THRESHOLD,
            "promoted_unlabeled_high_risk_v3_ml_rank_pct_lt": HIGH_RISK_ML_RANK_PCT_THRESHOLD,
        },
        "promoted_labeled_negatives": promoted_labeled_negatives,
        "promoted_labeled_negatives_count": len(promoted_labeled_negatives),
        "demoted_labeled_positives": demoted_labeled_positives,
        "demoted_labeled_positives_count": len(demoted_labeled_positives),
        "demoted_labeled_positive_clear_loss_count": sum(
            1 for row in demoted_labeled_positives if row["demotion_classification"] == "clear_loss"
        ),
        "demoted_labeled_positive_competitive_count": sum(
            1 for row in demoted_labeled_positives if row["demotion_classification"] == "competitive"
        ),
        "promoted_unlabeled_high_risk": promoted_unlabeled_high_risk,
        "promoted_unlabeled_high_risk_count": len(promoted_unlabeled_high_risk),
        "stable_labeled_positives": stable_labeled_positives,
        "stable_labeled_positives_count": len(stable_labeled_positives),
    }


def _recommended_next_stage_for_comparison(comparison: Mapping[str, Any]) -> tuple[str, str, bool]:
    risk = comparison["risk_readouts"]
    current_precision = comparison["group_summaries"]["current_top20"].get("labeled_precision")
    proposed_precision = comparison["group_summaries"]["proposed_top20"].get("labeled_precision")
    precision_ok = (
        current_precision is not None
        and proposed_precision is not None
        and proposed_precision >= current_precision
    )
    no_clear_loss = risk.get("demoted_labeled_positive_clear_loss_count") == 0
    if (
        risk.get("promoted_labeled_negatives_count") == 0
        and risk.get("promoted_unlabeled_high_risk_count") == 0
        and precision_ok
        and no_clear_loss
    ):
        return (
            "draft_bridge_rank_pct_hybrid_serving_plan_v1",
            "Primary alpha=0.5 introduces no promoted labeled negatives or high-risk unlabeled promotions, "
            "does not reduce labeled top-20 precision, and has no clear-loss demoted positives.",
            True,
        )
    return (
        "collect_bridge_rollout_review_labels_before_serving",
        "Primary alpha=0.5 has rollout-review risk: promoted negatives/high-risk unlabeled papers, "
        "lower labeled precision, or clear-loss demoted positives.",
        False,
    )


def _top20_comparison_for_alpha(
    candidates: Sequence[Mapping[str, Any]],
    *,
    alpha: float,
    top_k: int,
) -> dict[str, Any]:
    key = _alpha_key(alpha)
    rank_field = f"hybrid_rank_{key}"
    current = _top_tokens(candidates, rank_field="current_family_rank", top_k=top_k)
    proposed = _top_tokens(candidates, rank_field=rank_field, top_k=top_k)
    stable = current & proposed
    promoted = proposed - current
    demoted = current - proposed

    current_rows = sorted(_rows_by_tokens(candidates, current), key=lambda row: int(row["current_family_rank"]))
    proposed_rows = sorted(_rows_by_tokens(candidates, proposed), key=lambda row: int(row[rank_field]))
    promoted_rows = sorted(_rows_by_tokens(candidates, promoted), key=lambda row: int(row[rank_field]))
    demoted_rows = sorted(_rows_by_tokens(candidates, demoted), key=lambda row: int(row["current_family_rank"]))
    stable_rows = sorted(_rows_by_tokens(candidates, stable), key=lambda row: int(row["current_family_rank"]))

    group_summaries = {
        "current_top20": _group_summary(current_rows),
        "proposed_top20": _group_summary(proposed_rows),
        "promoted": _group_summary(promoted_rows),
        "demoted": _group_summary(demoted_rows),
        "stable": _group_summary(stable_rows),
    }
    current_precision = group_summaries["current_top20"]["labeled_precision"]
    proposed_precision = group_summaries["proposed_top20"]["labeled_precision"]
    top20_delta = (
        proposed_precision - current_precision
        if proposed_precision is not None and current_precision is not None
        else None
    )
    risk = _risk_readouts(
        promoted_rows=promoted_rows,
        demoted_rows=demoted_rows,
        stable_rows=stable_rows,
        alpha=alpha,
    )
    comparison = {
        "alpha": alpha,
        "top_k": top_k,
        "current_top20": [_compact_row(row, alpha=alpha) for row in current_rows],
        "proposed_top20": [_compact_row(row, alpha=alpha) for row in proposed_rows],
        "stable": [_compact_row(row, alpha=alpha) for row in stable_rows],
        "promoted": [_compact_row(row, alpha=alpha) for row in promoted_rows],
        "demoted": [_compact_row(row, alpha=alpha) for row in demoted_rows],
        "churn_count": top_k - len(stable),
        "churn_fraction": (top_k - len(stable)) / top_k,
        "stable_count": len(stable),
        "promoted_count": len(promoted),
        "demoted_count": len(demoted),
        "group_summaries": group_summaries,
        "risk_readouts": risk,
        "top20_quality_delta_labeled_only": top20_delta,
    }
    stage, rationale, pass_gate = _recommended_next_stage_for_comparison(comparison)
    comparison["recommended_next_stage"] = stage
    comparison["recommended_next_stage_rationale"] = rationale
    comparison["decision_gate_passed"] = pass_gate
    return comparison


def _stale_field_spot_checks(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    differing_ml = [
        {
            "work_id_token": row.get("work_id_token"),
            "historical_shadow_pilot_ml_probability": row.get("historical_shadow_pilot_ml_probability"),
            "v3_ml_probability": row.get("v3_ml_probability"),
        }
        for row in candidates
        if _as_float(row.get("historical_shadow_pilot_ml_probability")) is not None
        and abs(float(row["historical_shadow_pilot_ml_probability"]) - float(row["v3_ml_probability"])) > 1e-12
    ]
    differing_rank = [
        {
            "work_id_token": row.get("work_id_token"),
            "historical_shadow_pilot_hybrid_rank": row.get("historical_shadow_pilot_hybrid_rank"),
            "hybrid_rank_alpha_0_5": row.get("hybrid_rank_alpha_0_5"),
        }
        for row in candidates
        if isinstance(row.get("historical_shadow_pilot_hybrid_rank"), int)
        and row.get("historical_shadow_pilot_hybrid_rank") != row.get("hybrid_rank_alpha_0_5")
    ]
    return {
        "historical_shadow_pilot_ml_fields_ignored": True,
        "historical_shadow_pilot_hybrid_rank_fields_ignored": True,
        "recomputed_ml_probability_diff_count": len(differing_ml),
        "recomputed_hybrid_rank_diff_count": len(differing_rank),
        "example_recomputed_ml_probability_diff": differing_ml[0] if differing_ml else None,
        "example_recomputed_hybrid_rank_diff": differing_rank[0] if differing_rank else None,
    }


def _promoted_stale_ml_probability_spot_check(
    candidates: Sequence[Mapping[str, Any]],
    *,
    promoted_tokens: set[str],
) -> dict[str, Any] | None:
    for row in candidates:
        if str(row.get("work_id_token")) not in promoted_tokens:
            continue
        historical = _as_float(row.get("historical_shadow_pilot_ml_probability"))
        current = _as_float(row.get("v3_ml_probability"))
        if historical is None or current is None or abs(historical - current) <= 1e-12:
            continue
        return {
            "work_id_token": row.get("work_id_token"),
            "historical_shadow_pilot_ml_probability": historical,
            "recomputed_v3_ml_probability": current,
            "current_family_rank": row.get("current_family_rank"),
            "hybrid_rank_alpha_0_5": row.get("hybrid_rank_alpha_0_5"),
        }
    return None


def build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload(
    *,
    shadow_pilot_artifact_path: Path,
    sensitivity_artifact_path: Path,
    rank_pct_eval_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    database_url: str,
    expected_candidate_count: int = EXPECTED_POOL_CANDIDATES,
    top_k: int = TOP_K,
) -> dict[str, Any]:
    paths = [
        shadow_pilot_artifact_path.resolve(),
        sensitivity_artifact_path.resolve(),
        rank_pct_eval_artifact_path.resolve(),
        label_dataset_path.resolve(),
        readiness_matrix_path.resolve(),
        embeddings_provenance_path.resolve(),
    ]
    for path in paths:
        if not path.is_file():
            raise MLBridgeRankPctHybridControlledRolloutEvalError(f"required input not found: {path}")
    if not (database_url or "").strip():
        raise MLBridgeRankPctHybridControlledRolloutEvalError("database_url is required for full-pool replay")

    shadow_path, sens_path, rank_pct_path, label_path, readiness_path, embeddings_path = paths
    shadow_payload = _load_json_object(shadow_path, label="shadow pilot artifact")
    candidates = _validate_shadow_pilot_artifact(
        shadow_payload,
        path=shadow_path,
        expected_candidate_count=expected_candidate_count,
    )
    sensitivity_payload = _load_json_object(sens_path, label="sensitivity artifact")
    _validate_prerequisite_sensitivity_artifact(sensitivity_payload, path=sens_path)
    frozen_scorer = sensitivity_payload["selected_frozen_scorer"]
    rank_pct_payload = _load_json_object(rank_pct_path, label="rank-pct eval artifact")
    _validate_rank_pct_eval_artifact(rank_pct_payload, path=rank_pct_path)
    label_payload = _load_json_object(label_path, label="label dataset")
    label_sha = sha256_file(label_path)
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    _validate_readiness_matrix(readiness_payload, label_dataset_sha256=label_sha)
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")
    _validate_embeddings_provenance(embeddings_payload, frozen_scorer=frozen_scorer)

    pool_for_scoring = [
        {
            "work_id_int": row["work_id_int"],
            "work_id_token": row["work_id_token"],
            "bridge_score": row["bridge_score_raw"],
        }
        for row in candidates
    ]
    ml_prob_by_token = _score_pool_ml_probabilities(
        pool_for_scoring,
        frozen_scorer=frozen_scorer,
        database_url=database_url,
    )
    scored_candidates = _recompute_candidate_scores(candidates, ml_prob_by_token=ml_prob_by_token)
    overlay = _label_overlay_by_work_id(label_payload)
    _apply_label_overlay(scored_candidates, overlay)

    comparisons: dict[str, Any] = {
        _alpha_key(alpha): _top20_comparison_for_alpha(scored_candidates, alpha=alpha, top_k=top_k)
        for alpha in HYBRID_ALPHAS
    }
    primary = comparisons[_alpha_key(PRIMARY_ALPHA)]
    exploratory = comparisons[_alpha_key(EXPLORATORY_ALPHA)]
    stale_checks = _stale_field_spot_checks(scored_candidates)
    primary_promoted_tokens = {str(row["work_id_token"]) for row in primary["promoted"]}
    stale_checks["example_promoted_recomputed_ml_probability_diff"] = _promoted_stale_ml_probability_spot_check(
        scored_candidates,
        promoted_tokens=primary_promoted_tokens,
    )

    inputs = [
        {"name": "shadow_pilot_artifact", "path": portable_repo_path(shadow_path), "sha256": sha256_file(shadow_path)},
        {"name": "sensitivity_artifact", "path": portable_repo_path(sens_path), "sha256": sha256_file(sens_path)},
        {"name": "rank_pct_eval_artifact", "path": portable_repo_path(rank_pct_path), "sha256": sha256_file(rank_pct_path)},
        {"name": "label_dataset", "path": portable_repo_path(label_path), "sha256": label_sha},
        {"name": "readiness_matrix", "path": portable_repo_path(readiness_path), "sha256": sha256_file(readiness_path)},
        {
            "name": "embeddings_provenance",
            "path": portable_repo_path(embeddings_path),
            "sha256": sha256_file(embeddings_path),
        },
    ]

    bridge_score_coverage = sum(1 for row in scored_candidates if row["bridge_score_raw"] is not None)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "artifact_version": ARTIFACT_VERSION,
        "scorer_version": SCORER_VERSION,
        "generated_at": _now_iso_z(),
        "target": TARGET,
        "ranking_run_id": SHADOW_RANKING_RUN_ID,
        "selected_frozen_coefficient_C": SELECTED_FROZEN_C,
        "primary_alpha": PRIMARY_ALPHA,
        "exploratory_alpha": EXPLORATORY_ALPHA,
        "hybrid_formula": "alpha * rank_pct(ml_probability) + (1-alpha) * rank_pct(bridge_score)",
        "rank_pct_formula": "1 - average_rank / n",
        "rank_percentile_scope": SCOPE_FULL_POOL,
        "inputs": inputs,
        "prerequisite_checks": {
            "shadow_pilot_artifact_type": SHADOW_PILOT_ARTIFACT_TYPE,
            "shadow_pilot_version": SHADOW_PILOT_VERSION,
            "candidate_count": expected_candidate_count,
            "bridge_score_coverage": f"{bridge_score_coverage}/{expected_candidate_count}",
            "rank_pct_eval_artifact_type": RANK_PCT_EVAL_ARTIFACT_TYPE,
            "rank_pct_eval_recommended_next_stage": rank_pct_payload.get("recommended_next_stage"),
            "rank_pct_eval_hybrid_rescue_confirmed": rank_pct_payload.get("hybrid_rescue_confirmed"),
            "sensitivity_artifact_version": SENSITIVITY_ARTIFACT_VERSION,
            "ready_for_offline_hybrid_eval": True,
            "readiness_matrix_label_dataset_sha256_matched": True,
            "embeddings_artifact_version": EMBEDDINGS_ARTIFACT_VERSION,
        },
        "scoring": {
            "v3_ml_probability_source": "selected_frozen_scorer_full_pool_inference",
            "stale_shadow_pilot_fields_ignored": stale_checks,
        },
        "arm_top20_quality": _arm_top20_quality(scored_candidates, top_k=top_k),
        "top20_comparison_by_alpha": comparisons,
        "primary_alpha_0_5_summary": primary,
        "alpha_0_7_recommended_next_stage": exploratory["recommended_next_stage"],
        "recommended_next_stage": primary["recommended_next_stage"],
        "recommended_next_stage_rationale": primary["recommended_next_stage_rationale"],
        "controlled_rollout_eval_ready": primary["decision_gate_passed"],
        "candidate_scores": [_compact_row(row, alpha=PRIMARY_ALPHA) | {
            "v3_ml_rank": row.get("v3_ml_rank"),
            "bridge_score_rank": row.get("bridge_score_rank"),
            "hybrid_rank_pct_score_alpha_0_7": row.get("hybrid_rank_pct_score_alpha_0_7"),
            "hybrid_rank_alpha_0_7": row.get("hybrid_rank_alpha_0_7"),
            "historical_shadow_pilot_ml_probability": row.get("historical_shadow_pilot_ml_probability"),
            "historical_shadow_pilot_hybrid_rank": row.get("historical_shadow_pilot_hybrid_rank"),
        } for row in scored_candidates],
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_bridge_rank_pct_hybrid_controlled_rollout_eval(payload: dict[str, Any]) -> str:
    sens_input = next((item for item in payload.get("inputs", []) if item.get("name") == "sensitivity_artifact"), {})
    primary = payload.get("primary_alpha_0_5_summary", {})
    comparisons = payload.get("top20_comparison_by_alpha", {})
    arm_quality = payload.get("arm_top20_quality", {})
    risk = primary.get("risk_readouts", {})
    group_summaries = primary.get("group_summaries", {})
    lines = [
        "# Bridge rank-percentile hybrid controlled rollout replay v1",
        "",
        "Offline controlled rollout replay for replacing the current Bridge top-20 with the "
        "rank-percentile hybrid top-20. No serving, API, web, or database-write behavior changed.",
        "",
        "## Prerequisite",
        "",
        f"- Sensitivity artifact: `{sens_input.get('path')}`",
        f"- SHA256: `{sens_input.get('sha256')}`",
        f"- Selected C: `{payload.get('selected_frozen_coefficient_C')}`",
        "",
        "## Arm Comparison",
        "",
        "| arm | labeled | pos | neg | unlabeled | labeled precision |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for arm, summary in arm_quality.items():
        lines.append(
            f"| `{arm}` | {summary.get('labeled_count')} | {summary.get('labeled_positive_count')} | "
            f"{summary.get('labeled_negative_count')} | {summary.get('unlabeled_count')} | "
            f"{summary.get('labeled_precision')} |"
        )
    lines.extend(
        [
            "",
            "## Top-20 Churn",
            "",
            "| alpha | stable | promoted | demoted | churn fraction | proposed labeled precision |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for key, comparison in comparisons.items():
        proposed = comparison.get("group_summaries", {}).get("proposed_top20", {})
        lines.append(
            f"| {comparison.get('alpha')} | {comparison.get('stable_count')} | "
            f"{comparison.get('promoted_count')} | {comparison.get('demoted_count')} | "
            f"{comparison.get('churn_fraction')} | {proposed.get('labeled_precision')} |"
        )
    lines.extend(
        [
            "",
            "## Verdict Table",
            "",
            "| readout | count / value |",
            "|---|---:|",
            f"| promoted_labeled_negatives | {risk.get('promoted_labeled_negatives_count')} |",
            f"| promoted_unlabeled_high_risk | {risk.get('promoted_unlabeled_high_risk_count')} |",
            f"| demoted_labeled_positive_clear_loss | {risk.get('demoted_labeled_positive_clear_loss_count')} |",
            f"| stable_labeled_positives | {risk.get('stable_labeled_positives_count')} |",
            f"| current labeled precision | {group_summaries.get('current_top20', {}).get('labeled_precision')} |",
            f"| proposed labeled precision | {group_summaries.get('proposed_top20', {}).get('labeled_precision')} |",
            f"| top20_quality_delta_labeled_only | {primary.get('top20_quality_delta_labeled_only')} |",
            "",
            "## Recommendation",
            "",
            f"- **recommended_next_stage:** `{payload.get('recommended_next_stage')}`",
            f"- {payload.get('recommended_next_stage_rationale')}",
            "",
            "## Promoted",
            "",
        ]
    )
    for row in primary.get("promoted", []):
        lines.append(
            f"- `{row.get('work_id_token')}` current={row.get('current_family_rank')} "
            f"hybrid={row.get('hybrid_rank_alpha_0_5')} label={row.get('bridge_recommendable')}"
        )
    lines.extend(["", "## Demoted", ""])
    for row in primary.get("demoted", []):
        lines.append(
            f"- `{row.get('work_id_token')}` current={row.get('current_family_rank')} "
            f"hybrid={row.get('hybrid_rank_alpha_0_5')} label={row.get('bridge_recommendable')}"
        )
    lines.extend(["", "## Caveats", ""])
    for caveat in payload.get("caveats", []):
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def write_ml_bridge_rank_pct_hybrid_controlled_rollout_eval(
    *,
    shadow_pilot_artifact_path: Path,
    sensitivity_artifact_path: Path,
    rank_pct_eval_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    database_url: str,
    output_json: Path,
    markdown_output: Path | None,
) -> dict[str, Any]:
    payload = build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload(
        shadow_pilot_artifact_path=shadow_pilot_artifact_path,
        sensitivity_artifact_path=sensitivity_artifact_path,
        rank_pct_eval_artifact_path=rank_pct_eval_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        database_url=database_url,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_output is not None:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(
            markdown_from_ml_bridge_rank_pct_hybrid_controlled_rollout_eval(payload),
            encoding="utf-8",
        )
    return payload


def run_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_cli(
    *,
    shadow_pilot_artifact_path: Path,
    sensitivity_artifact_path: Path,
    rank_pct_eval_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    database_url: str | None,
    output_json: Path,
    markdown_output: Path | None,
) -> dict[str, Any]:
    return write_ml_bridge_rank_pct_hybrid_controlled_rollout_eval(
        shadow_pilot_artifact_path=shadow_pilot_artifact_path,
        sensitivity_artifact_path=sensitivity_artifact_path,
        rank_pct_eval_artifact_path=rank_pct_eval_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        database_url=database_url or database_url_from_env(),
        output_json=output_json,
        markdown_output=markdown_output,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "ARTIFACT_VERSION",
    "MLBridgeRankPctHybridControlledRolloutEvalError",
    "build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload",
    "markdown_from_ml_bridge_rank_pct_hybrid_controlled_rollout_eval",
    "run_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_cli",
    "write_ml_bridge_rank_pct_hybrid_controlled_rollout_eval",
]
