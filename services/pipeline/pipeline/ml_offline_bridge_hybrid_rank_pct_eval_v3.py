"""Offline rank-percentile hybrid evaluation: v3 ML + bridge_score on shadow-pilot rows.

Uses the shadow-pilot rank-percentile discipline:
  alpha * rank_pct(ml_probability) + (1-alpha) * rank_pct(bridge_score)

Prefer ``full_bridge_candidate_pool`` scope (528 Bridge candidates from the shadow pilot
artifact + frozen v3 C=0.001 inference via SELECT-only DB embeddings). Falls back to
``labeled_shadow_slice_only`` when the shadow pilot artifact or database URL is absent.
"""

from __future__ import annotations

import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from sklearn.metrics import average_precision_score

from pipeline.ml_bridge_shadow_pilot import _fetch_embeddings, _rank_pct_from_pairs, _score_with_frozen_model
from pipeline.ml_offline_baseline_eval import pairwise_accuracy, precision_at_k, roc_auc_mann_whitney, sha256_file
from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
    ARMS,
    BUCKET_EXPECTED,
    BRIDGE_SCORE_MIN_COVERAGE,
    EMBEDDINGS_ARTIFACT_VERSION,
    EXPECTED_SHADOW_NEGATIVE,
    EXPECTED_SHADOW_POSITIVE,
    EXPECTED_SHADOW_ROWS,
    FAMILY,
    HYBRID_ALPHAS,
    LABEL_DATASET_VERSION,
    PRIMARY_ALPHA,
    SELECTED_FROZEN_C,
    SENSITIVITY_ARTIFACT_TYPE,
    SENSITIVITY_ARTIFACT_VERSION,
    SHADOW_POOL_VARIANT,
    SHADOW_RANKING_RUN_ID,
    TARGET,
    _disagreement_bucket,
    _load_oof_by_work_id,
    _slice_shadow_rows,
    _validate_embeddings_provenance,
    _validate_prerequisite_sensitivity_artifact,
    _validate_readiness_matrix,
    _work_id_token,
)
from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
    _as_float,
    _load_json_object,
)
from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
    MLOfflineBridgeHybridEvalV3Error as MLOfflineBridgeHybridRankPctEvalV3Error,
)
from pipeline.repo_paths import portable_repo_path

EVAL_VERSION = "ml-offline-bridge-hybrid-rank-pct-eval-v3-v1"
ARTIFACT_TYPE = "ml_offline_bridge_hybrid_rank_pct_eval_v3"
LINEAR_HYBRID_EVAL_V3_VERSION = "ml-offline-bridge-hybrid-eval-v3-v1"
SHADOW_PILOT_ARTIFACT_TYPE = "ml_bridge_shadow_pilot"
SHADOW_PILOT_VERSION = "ml-bridge-shadow-pilot-v1"
EXPECTED_POOL_CANDIDATES = 528
POOL_BRIDGE_SCORE_MIN_COVERAGE = 500
HYBRID_FORMULA = "alpha * rank_pct(ml_probability) + (1-alpha) * rank_pct(bridge_score)"
RANK_PCT_METHOD = "shadow_pilot_average_rank_over_n"

SCOPE_FULL_POOL = "full_bridge_candidate_pool"
SCOPE_LABELED_SLICE = "labeled_shadow_slice_only"

CAVEATS = (
    "Offline rank-percentile hybrid eval diagnostic only; does not enable Bridge serving.",
    "Rank percentiles use the shadow-pilot formula (1 - average_rank/n), not min-max linear blend.",
    "Full-pool scope scores all Bridge candidates with frozen v3 C=0.001 inference (not OOF).",
    "Labeled-slice-only scope computes rank percentiles over the 60 shadow rows only.",
    "60-row labeled evaluation; not powered for precision on 10-row buckets.",
    "Alpha sweep is diagnostic; no alpha is authorized for production without further review.",
    "Does not change production default, API/web surface, or user-visible ranking.",
    "Bridge recommendations remain subject to controlled rollout authorization.",
)


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_frozen_scorer(frozen: Mapping[str, Any]) -> None:
    for key in ("scaler_mean", "scaler_scale", "coef", "intercept", "embedding_version"):
        if key not in frozen:
            raise MLOfflineBridgeHybridRankPctEvalV3Error(
                f"selected_frozen_scorer missing {key!r}"
            )


def _load_shadow_pilot_candidates(payload: dict[str, Any], *, path: Path) -> list[dict[str, Any]]:
    if payload.get("artifact_type") != SHADOW_PILOT_ARTIFACT_TYPE:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"{path} artifact_type={payload.get('artifact_type')!r}; "
            f"expected {SHADOW_PILOT_ARTIFACT_TYPE!r}"
        )
    if payload.get("pilot_version") != SHADOW_PILOT_VERSION:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"{path} pilot_version={payload.get('pilot_version')!r}; expected {SHADOW_PILOT_VERSION!r}"
        )
    if payload.get("ranking_run_id") != SHADOW_RANKING_RUN_ID:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"{path} ranking_run_id={payload.get('ranking_run_id')!r}; "
            f"expected {SHADOW_RANKING_RUN_ID!r}"
        )
    candidates = payload.get("all_candidates")
    if not isinstance(candidates, list):
        raise MLOfflineBridgeHybridRankPctEvalV3Error(f"{path} missing all_candidates array")
    count = payload.get("candidate_count")
    if count != EXPECTED_POOL_CANDIDATES or len(candidates) != EXPECTED_POOL_CANDIDATES:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"{path} candidate_count={count!r} len(all_candidates)={len(candidates)}; "
            f"expected {EXPECTED_POOL_CANDIDATES}"
        )
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(candidates, start=1):
        if not isinstance(row, dict):
            raise MLOfflineBridgeHybridRankPctEvalV3Error(f"all_candidates[{idx}] is not an object")
        token = _work_id_token(row.get("work_id_token") or row.get("openalex_id"))
        work_id_int = row.get("work_id_int")
        if not isinstance(work_id_int, int):
            raise MLOfflineBridgeHybridRankPctEvalV3Error(
                f"all_candidates[{idx}] missing integer work_id_int"
            )
        out.append(
            {
                "work_id_int": work_id_int,
                "work_id_token": token,
                "bridge_score": _as_float(row.get("bridge_score")),
            }
        )
    covered = sum(1 for row in out if row["bridge_score"] is not None)
    if covered < POOL_BRIDGE_SCORE_MIN_COVERAGE:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"shadow pilot pool bridge_score coverage too low: {covered}/{EXPECTED_POOL_CANDIDATES}"
        )
    return out


def _score_pool_ml_probabilities(
    candidates: Sequence[Mapping[str, Any]],
    *,
    frozen_scorer: Mapping[str, Any],
    database_url: str,
) -> dict[str, float]:
    import psycopg

    _validate_frozen_scorer(frozen_scorer)
    work_ids = [int(row["work_id_int"]) for row in candidates]
    embedding_version = str(frozen_scorer["embedding_version"])
    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
        embeddings = _fetch_embeddings(
            conn,
            work_ids=work_ids,
            embedding_version=embedding_version,
        )

    missing = [wid for wid in work_ids if wid not in embeddings]
    if missing:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"{len(missing)} pool candidates missing embeddings for {embedding_version!r}; "
            f"first few: {missing[:5]}"
        )

    scaler_mean = frozen_scorer["scaler_mean"]
    scaler_scale = frozen_scorer["scaler_scale"]
    coef = frozen_scorer["coef"]
    intercept = float(frozen_scorer["intercept"])
    out: dict[str, float] = {}
    for row in candidates:
        token = str(row["work_id_token"])
        wid = int(row["work_id_int"])
        prob = _score_with_frozen_model(
            embeddings[wid],
            scaler_mean=scaler_mean,
            scaler_scale=scaler_scale,
            coef=coef,
            intercept=intercept,
        )
        if token in out:
            raise MLOfflineBridgeHybridRankPctEvalV3Error(f"duplicate pool work_id_token {token!r}")
        out[token] = float(prob)
    return out


def _compute_rank_percentiles_from_pool(
    candidates: Sequence[Mapping[str, Any]],
    *,
    ml_prob_by_token: Mapping[str, float],
) -> tuple[dict[str, float], dict[str, float]]:
    ml_pairs = [
        (str(row["work_id_token"]), float(ml_prob_by_token[str(row["work_id_token"])]))
        for row in candidates
    ]
    bridge_pairs = [
        (str(row["work_id_token"]), float(row["bridge_score"]))
        for row in candidates
        if row.get("bridge_score") is not None
    ]
    return _rank_pct_from_pairs(ml_pairs), _rank_pct_from_pairs(bridge_pairs)


def _compute_rank_percentiles_slice_only(
    shadow_rows: Sequence[Mapping[str, Any]],
    *,
    oof_by_work_id: Mapping[str, float],
) -> tuple[dict[str, float], dict[str, float], dict[str, float | None]]:
    ml_pairs: list[tuple[str, float]] = []
    bridge_pairs: list[tuple[str, float]] = []
    bridge_raw_by_token: dict[str, float | None] = {}
    for row in shadow_rows:
        token = _work_id_token(row.get("work_id") or row.get("openalex_work_id") or row.get("paper_id"))
        if token not in oof_by_work_id:
            raise MLOfflineBridgeHybridRankPctEvalV3Error(
                f"missing OOF probability for shadow work_id={token!r}"
            )
        ml_pairs.append((token, float(oof_by_work_id[token])))
        ctx = row.get("bridge_shadow_pilot_context")
        raw = _as_float(ctx.get("bridge_score")) if isinstance(ctx, Mapping) else None
        bridge_raw_by_token[token] = raw
        if raw is not None:
            bridge_pairs.append((token, raw))
    return (
        _rank_pct_from_pairs(ml_pairs),
        _rank_pct_from_pairs(bridge_pairs),
        bridge_raw_by_token,
    )


def _build_scored_rows(
    shadow_rows: Sequence[Mapping[str, Any]],
    *,
    oof_by_work_id: Mapping[str, float],
    ml_rank_pct_by_token: Mapping[str, float],
    bridge_rank_pct_by_token: Mapping[str, float],
    bridge_score_by_token: Mapping[str, float | None],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    covered = sum(
        1
        for row in shadow_rows
        if bridge_score_by_token.get(
            _work_id_token(row.get("work_id") or row.get("openalex_work_id") or row.get("paper_id"))
        )
        is not None
    )
    if covered < BRIDGE_SCORE_MIN_COVERAGE:
        raise MLOfflineBridgeHybridRankPctEvalV3Error(
            f"bridge_score coverage too low: {covered}/{EXPECTED_SHADOW_ROWS}; "
            f"minimum {BRIDGE_SCORE_MIN_COVERAGE}"
        )

    scored: list[dict[str, Any]] = []
    for row in shadow_rows:
        token = _work_id_token(row.get("work_id") or row.get("openalex_work_id") or row.get("paper_id"))
        if token not in oof_by_work_id:
            raise MLOfflineBridgeHybridRankPctEvalV3Error(f"missing OOF probability for shadow work_id={token!r}")
        if token not in ml_rank_pct_by_token:
            raise MLOfflineBridgeHybridRankPctEvalV3Error(f"missing ml_rank_pct for shadow work_id={token!r}")
        label = row.get(TARGET)
        if label not in (True, False):
            raise MLOfflineBridgeHybridRankPctEvalV3Error(f"shadow row {token} has non-boolean {TARGET}")
        bridge_raw = bridge_score_by_token.get(token)
        bridge_pct = bridge_rank_pct_by_token.get(token)
        scored.append(
            {
                "row_id": row.get("row_id"),
                "work_id": token,
                "disagreement_bucket": _disagreement_bucket(row),
                TARGET: label,
                "ml_prob_oof": float(oof_by_work_id[token]),
                "ml_rank_pct": float(ml_rank_pct_by_token[token]),
                "bridge_score_raw": bridge_raw,
                "bridge_rank_pct": float(bridge_pct) if bridge_pct is not None else None,
            }
        )
    coverage = {
        "shadow_rows": EXPECTED_SHADOW_ROWS,
        "bridge_score_covered_rows": covered,
        "bridge_score_coverage_fraction": covered / EXPECTED_SHADOW_ROWS,
        "ml_prob_oof_covered_rows": len(scored),
        "ml_rank_pct_covered_rows": len(scored),
        "bridge_rank_pct_covered_rows": sum(1 for r in scored if r["bridge_rank_pct"] is not None),
    }
    return scored, coverage


def _score_for_arm(row: Mapping[str, Any], *, arm: str, alpha: float) -> float | None:
    if arm == "pure_ml":
        return _as_float(row.get("ml_rank_pct"))
    if arm == "pure_bridge":
        return _as_float(row.get("bridge_rank_pct"))
    if arm == "hybrid":
        ml_pct = _as_float(row.get("ml_rank_pct"))
        bridge_pct = _as_float(row.get("bridge_rank_pct"))
        if ml_pct is None or bridge_pct is None:
            return None
        return alpha * ml_pct + (1.0 - alpha) * bridge_pct
    raise MLOfflineBridgeHybridRankPctEvalV3Error(f"unknown arm {arm!r}")


def _arm_metrics(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float) -> dict[str, Any]:
    pairs: list[tuple[float, bool]] = []
    sortable: list[tuple[float, str, bool]] = []
    for row in rows:
        score = _score_for_arm(row, arm=arm, alpha=alpha)
        if score is None:
            continue
        label = bool(row[TARGET])
        pairs.append((score, label))
        sortable.append((score, str(row.get("work_id") or ""), label))
    row_count = len(pairs)
    pos = sum(1 for _, label in pairs if label)
    neg = row_count - pos
    if pos == 0 or neg == 0:
        return {
            "arm": arm,
            "alpha": alpha,
            "status": "not_applicable",
            "row_count": row_count,
            "roc_auc": None,
            "average_precision": None,
            "precision_at_10": None,
            "precision_at_20": None,
            "pairwise_accuracy": None,
        }
    desc = sorted(sortable, key=lambda t: (-t[0], t[1]))
    desc_pairs = [(score, label) for score, _, label in desc]
    scores = [score for score, _ in pairs]
    labels = [label for _, label in pairs]
    return {
        "arm": arm,
        "alpha": alpha,
        "status": "ok",
        "row_count": row_count,
        "roc_auc": roc_auc_mann_whitney(pairs),
        "average_precision": float(average_precision_score(labels, scores)),
        "precision_at_10": precision_at_k(desc_pairs, 10),
        "precision_at_20": precision_at_k(desc_pairs, 20),
        "pairwise_accuracy": pairwise_accuracy(pairs),
    }


def _pairwise_pos_above_neg(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float) -> float | None:
    positives = [r for r in rows if r.get(TARGET) is True]
    negatives = [r for r in rows if r.get(TARGET) is False]
    if not positives or not negatives:
        return None
    correct = 0
    total = 0
    for pos in positives:
        pos_score = _score_for_arm(pos, arm=arm, alpha=alpha)
        if pos_score is None:
            continue
        for neg in negatives:
            neg_score = _score_for_arm(neg, arm=arm, alpha=alpha)
            if neg_score is None:
                continue
            total += 1
            if pos_score > neg_score:
                correct += 1
    if total == 0:
        return None
    return correct / total


def _precision_at_k_within_subset(
    rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float, k: int
) -> float | None:
    ranked: list[tuple[bool, float]] = []
    for row in rows:
        score = _score_for_arm(row, arm=arm, alpha=alpha)
        if score is None:
            continue
        ranked.append((bool(row[TARGET]), score))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[1], reverse=True)
    top = ranked[: min(k, len(ranked))]
    if not top:
        return None
    return sum(1 for label, _ in top if label) / len(top)


def _pos_above_neg_median(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float) -> int | None:
    pos_rows = [r for r in rows if r.get(TARGET) is True]
    neg_rows = [r for r in rows if r.get(TARGET) is False]
    neg_scores = [_score_for_arm(r, arm=arm, alpha=alpha) for r in neg_rows]
    neg_scores = [s for s in neg_scores if s is not None]
    if not neg_scores:
        return None
    neg_median = statistics.median(neg_scores)
    count = 0
    for row in pos_rows:
        score = _score_for_arm(row, arm=arm, alpha=alpha)
        if score is not None and score > neg_median:
            count += 1
    return count


def _mean_scores_by_label(
    rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float
) -> dict[str, float | None]:
    pos_scores: list[float] = []
    neg_scores: list[float] = []
    for row in rows:
        score = _score_for_arm(row, arm=arm, alpha=alpha)
        if score is None:
            continue
        if row.get(TARGET) is True:
            pos_scores.append(score)
        else:
            neg_scores.append(score)
    return {
        "positive_mean": float(statistics.fmean(pos_scores)) if pos_scores else None,
        "negative_mean": float(statistics.fmean(neg_scores)) if neg_scores else None,
    }


def _verdict_high_bridge(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float) -> str:
    pairwise = _pairwise_pos_above_neg(rows, arm=arm, alpha=alpha)
    if pairwise is None:
        return "not_applicable"
    if pairwise >= 0.75:
        return "rescues_high_bridge_positives"
    if pairwise >= 0.5:
        return "partial"
    return "fails"


def _verdict_promoted(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float) -> str:
    p10 = _precision_at_k_within_subset(rows, arm=arm, alpha=alpha, k=10)
    pos_above = _pos_above_neg_median(rows, arm=arm, alpha=alpha)
    if p10 is None or pos_above is None:
        return "not_applicable"
    if p10 >= 0.7 and pos_above >= 10:
        return "maintains_hybrid_promotion"
    if p10 >= 0.5:
        return "partial"
    return "fails"


def _verdict_demoted(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float) -> dict[str, Any]:
    ml_probs = [_as_float(r.get("ml_prob_oof")) for r in rows]
    ml_probs = [p for p in ml_probs if p is not None]
    if not ml_probs:
        return {"verdict": "not_applicable", "competitive_mean": None, "correct_rejection_mean": None}
    ml_median = statistics.median(ml_probs)
    competitive: list[dict[str, Any]] = []
    rejection: list[dict[str, Any]] = []
    for row in rows:
        ml_prob = _as_float(row.get("ml_prob_oof"))
        if ml_prob is None:
            continue
        if row.get(TARGET) is True and ml_prob > ml_median:
            competitive.append(row)
        elif row.get(TARGET) is False and ml_prob < ml_median:
            rejection.append(row)
    comp_mean = _mean_scores_by_label(competitive, arm=arm, alpha=alpha)
    rej_mean = _mean_scores_by_label(rejection, arm=arm, alpha=alpha)
    comp_score = comp_mean["positive_mean"]
    rej_score = rej_mean["negative_mean"]
    verdict = "fails"
    if comp_score is not None and rej_score is not None and comp_score > rej_score:
        verdict = "separates_demotions"
    return {
        "verdict": verdict,
        "competitive_demotion_subgroup_count": len(competitive),
        "correct_rejection_subgroup_count": len(rejection),
        "competitive_mean": comp_score,
        "correct_rejection_mean": rej_score,
        "ml_prob_median_within_demoted": ml_median,
    }


def _top_k_count(rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float, k: int) -> int:
    ranked: list[tuple[str, float]] = []
    for row in rows:
        score = _score_for_arm(row, arm=arm, alpha=alpha)
        if score is None:
            continue
        ranked.append((str(row.get("work_id")), score))
    ranked.sort(key=lambda item: item[1], reverse=True)
    top_ids = {wid for wid, _ in ranked[:k]}
    return sum(1 for row in rows if str(row.get("work_id")) in top_ids)


def _targeted_readouts(scored_rows: Sequence[Mapping[str, Any]], *, alpha: float) -> dict[str, Any]:
    by_bucket: dict[str, list[dict[str, Any]]] = {}
    for row in scored_rows:
        by_bucket.setdefault(str(row["disagreement_bucket"]), []).append(row)

    for bucket, expected in BUCKET_EXPECTED.items():
        rows = by_bucket.get(bucket, [])
        if len(rows) != expected[0]:
            raise MLOfflineBridgeHybridRankPctEvalV3Error(
                f"bucket {bucket} has {len(rows)} rows; expected {expected[0]}"
            )

    readouts: dict[str, Any] = {}
    high_bridge = by_bucket["high_bridge_score_low_ml"]
    high_bridge_block: dict[str, Any] = {}
    for arm in ARMS:
        means = _mean_scores_by_label(high_bridge, arm=arm, alpha=alpha)
        pairwise = _pairwise_pos_above_neg(high_bridge, arm=arm, alpha=alpha)
        high_bridge_block[arm] = {
            "positive_mean": means["positive_mean"],
            "negative_mean": means["negative_mean"],
            "pairwise_pos_above_neg": pairwise,
            "verdict": _verdict_high_bridge(high_bridge, arm=arm, alpha=alpha),
        }
    readouts["high_bridge_score_low_ml"] = high_bridge_block

    promoted = by_bucket["promoted_by_hybrid"]
    promoted_block: dict[str, Any] = {}
    for arm in ARMS:
        promoted_block[arm] = {
            "precision_at_10_within_bucket": _precision_at_k_within_subset(
                promoted, arm=arm, alpha=alpha, k=10
            ),
            "positives_above_negative_median": _pos_above_neg_median(promoted, arm=arm, alpha=alpha),
            "verdict": _verdict_promoted(promoted, arm=arm, alpha=alpha),
        }
    readouts["promoted_by_hybrid"] = promoted_block

    demoted = by_bucket["demoted_by_hybrid"]
    demoted_block: dict[str, Any] = {arm: _verdict_demoted(demoted, arm=arm, alpha=alpha) for arm in ARMS}
    readouts["demoted_by_hybrid"] = demoted_block

    high_ml = by_bucket["high_ml_low_bridge_score"]
    high_ml_block: dict[str, Any] = {
        "row_count": len(high_ml),
        "positive_count": sum(1 for r in high_ml if r.get(TARGET) is True),
        "negative_count": sum(1 for r in high_ml if r.get(TARGET) is False),
    }
    for arm in ARMS:
        pos_scores = [
            _score_for_arm(r, arm=arm, alpha=alpha)
            for r in high_ml
            if r.get(TARGET) is True
        ]
        pos_scores = [s for s in pos_scores if s is not None]
        high_ml_block[arm] = {
            "positive_mean": float(statistics.fmean(pos_scores)) if pos_scores else None,
            "top_20_count_within_shadow_slice": _top_k_count(scored_rows, arm=arm, alpha=alpha, k=20),
        }
    readouts["high_ml_low_bridge_score"] = high_ml_block

    hybrid_pairwise = _pairwise_pos_above_neg(high_bridge, arm="hybrid", alpha=alpha)
    ml_pairwise = _pairwise_pos_above_neg(high_bridge, arm="pure_ml", alpha=alpha)
    readouts["high_bridge_hybrid_vs_ml"] = {
        "hybrid_pairwise": hybrid_pairwise,
        "pure_ml_pairwise": ml_pairwise,
        "hybrid_rescues_vs_pure_ml": (
            hybrid_pairwise is not None and ml_pairwise is not None and hybrid_pairwise > ml_pairwise
        ),
    }
    return readouts


def _hybrid_lift(scored_rows: Sequence[Mapping[str, Any]], *, alpha: float) -> dict[str, Any]:
    hybrid = _arm_metrics(scored_rows, arm="hybrid", alpha=alpha)
    pure_ml = _arm_metrics(scored_rows, arm="pure_ml", alpha=alpha)
    pure_bridge = _arm_metrics(scored_rows, arm="pure_bridge", alpha=alpha)
    p20_h = _as_float(hybrid.get("precision_at_20"))
    p20_m = _as_float(pure_ml.get("precision_at_20"))
    p20_b = _as_float(pure_bridge.get("precision_at_20"))
    auc_h = _as_float(hybrid.get("roc_auc"))
    auc_m = _as_float(pure_ml.get("roc_auc"))
    auc_b = _as_float(pure_bridge.get("roc_auc"))
    delta_p20_ml = (p20_h - p20_m) if p20_h is not None and p20_m is not None else None
    delta_p20_bridge = (p20_h - p20_b) if p20_h is not None and p20_b is not None else None
    delta_auc_ml = (auc_h - auc_m) if auc_h is not None and auc_m is not None else None
    delta_auc_bridge = (auc_h - auc_b) if auc_h is not None and auc_b is not None else None
    hurts = delta_p20_ml is not None and delta_p20_ml < -0.05
    return {
        "precision_at_20_delta_hybrid_minus_pure_ml": delta_p20_ml,
        "precision_at_20_delta_hybrid_minus_pure_bridge": delta_p20_bridge,
        "roc_auc_delta_hybrid_minus_pure_ml": delta_auc_ml,
        "roc_auc_delta_hybrid_minus_pure_bridge": delta_auc_bridge,
        "hybrid_hurts_ml_precision": hurts,
    }


def _recommended_next_stage(
    *,
    targeted: Mapping[str, Any],
    hybrid_lift: Mapping[str, Any],
) -> tuple[str, bool, str]:
    high_bridge = targeted.get("high_bridge_score_low_ml", {})
    promoted = targeted.get("promoted_by_hybrid", {})
    demoted = targeted.get("demoted_by_hybrid", {})
    hybrid_high = high_bridge.get("hybrid", {}) if isinstance(high_bridge, dict) else {}
    hybrid_prom = promoted.get("hybrid", {}) if isinstance(promoted, dict) else {}
    hybrid_dem = demoted.get("hybrid", {}) if isinstance(demoted, dict) else {}

    high_verdict = str(hybrid_high.get("verdict") or "")
    prom_verdict = str(hybrid_prom.get("verdict") or "")
    dem_verdict = str(hybrid_dem.get("verdict") or "")
    hurts = hybrid_lift.get("hybrid_hurts_ml_precision") is True

    if (
        high_verdict == "rescues_high_bridge_positives"
        and prom_verdict in {"maintains_hybrid_promotion", "partial"}
        and dem_verdict == "separates_demotions"
        and not hurts
    ):
        return (
            "authorize_bridge_hybrid_serving_controlled_rollout_eval",
            True,
            "Primary alpha=0.5 rank-percentile hybrid rescues high-bridge positives, maintains "
            "promoted bucket signal, separates demotion subgroups, and does not materially hurt ML P@20.",
        )
    if high_verdict == "partial" and prom_verdict != "fails" and dem_verdict != "fails":
        return (
            "collect_more_high_bridge_score_low_ml_labels_before_serving",
            False,
            "Rank-percentile hybrid shows partial rescue on high-bridge bucket but not enough "
            "confidence for controlled rollout authorization.",
        )
    return (
        "do_not_authorize_bridge_hybrid_serving_recheck_alpha_or_formula",
        False,
        "Primary alpha=0.5 rank-percentile hybrid fails one or more targeted readouts or hurts "
        "ML precision; recheck alpha or hybrid formula before serving.",
    )


def build_ml_offline_bridge_hybrid_rank_pct_eval_v3_payload(
    *,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    shadow_pilot_artifact_path: Path | None = None,
    database_url: str | None = None,
    linear_hybrid_eval_v3_path: Path | None = None,
) -> dict[str, Any]:
    sens_path = sensitivity_artifact_path.resolve()
    label_path = label_dataset_path.resolve()
    readiness_path = readiness_matrix_path.resolve()
    embeddings_path = embeddings_provenance_path.resolve()
    for path in (sens_path, label_path, readiness_path, embeddings_path):
        if not path.is_file():
            raise MLOfflineBridgeHybridRankPctEvalV3Error(f"required input not found: {path}")

    sensitivity_payload = _load_json_object(sens_path, label="sensitivity artifact")
    _validate_prerequisite_sensitivity_artifact(sensitivity_payload, path=sens_path)
    frozen_scorer = sensitivity_payload["selected_frozen_scorer"]
    _validate_frozen_scorer(frozen_scorer)

    label_payload = _load_json_object(label_path, label="label dataset")
    label_sha = sha256_file(label_path)
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    _validate_readiness_matrix(readiness_payload, label_dataset_sha256=label_sha)
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")
    _validate_embeddings_provenance(embeddings_payload, frozen_scorer=frozen_scorer)

    oof_by_work_id = _load_oof_by_work_id(sensitivity_payload)
    shadow_rows = _slice_shadow_rows(label_payload)

    db_url = (database_url or "").strip()
    shadow_path = shadow_pilot_artifact_path.resolve() if shadow_pilot_artifact_path is not None else None
    use_full_pool = shadow_path is not None and shadow_path.is_file() and bool(db_url)

    scope_fallback_reason: str | None = None
    pool_candidates: list[dict[str, Any]] | None = None
    ml_prob_source: str
    if use_full_pool:
        assert shadow_path is not None
        shadow_pilot_payload = _load_json_object(shadow_path, label="shadow pilot artifact")
        pool_candidates = _load_shadow_pilot_candidates(shadow_pilot_payload, path=shadow_path)
        ml_prob_by_token = _score_pool_ml_probabilities(
            pool_candidates,
            frozen_scorer=frozen_scorer,
            database_url=db_url,
        )
        ml_rank_pct_by_token, bridge_rank_pct_by_token = _compute_rank_percentiles_from_pool(
            pool_candidates,
            ml_prob_by_token=ml_prob_by_token,
        )
        bridge_score_by_token = {
            str(row["work_id_token"]): _as_float(row.get("bridge_score")) for row in pool_candidates
        }
        rank_percentile_scope = SCOPE_FULL_POOL
        ml_prob_source = "frozen_v3_C0_001_full_fit"
        pool_candidate_count = len(pool_candidates)
    else:
        if shadow_path is not None and shadow_path.is_file() and not db_url:
            scope_fallback_reason = "shadow_pilot_artifact_without_database_url"
        elif shadow_path is not None and not shadow_path.is_file():
            scope_fallback_reason = "shadow_pilot_artifact_not_found"
        else:
            scope_fallback_reason = "shadow_pilot_artifact_not_provided"
        ml_rank_pct_by_token, bridge_rank_pct_by_token, bridge_raw_by_token = (
            _compute_rank_percentiles_slice_only(shadow_rows, oof_by_work_id=oof_by_work_id)
        )
        bridge_score_by_token = bridge_raw_by_token
        rank_percentile_scope = SCOPE_LABELED_SLICE
        ml_prob_source = "oof_sensitivity_C0_001"
        pool_candidate_count = EXPECTED_SHADOW_ROWS

    scored_rows, bridge_coverage = _build_scored_rows(
        shadow_rows,
        oof_by_work_id=oof_by_work_id,
        ml_rank_pct_by_token=ml_rank_pct_by_token,
        bridge_rank_pct_by_token=bridge_rank_pct_by_token,
        bridge_score_by_token=bridge_score_by_token,
    )

    alpha_results: dict[str, Any] = {}
    for alpha in HYBRID_ALPHAS:
        key = f"hybrid_alpha_{str(alpha).replace('.', '_')}"
        arm_metrics = {arm: _arm_metrics(scored_rows, arm=arm, alpha=alpha) for arm in ARMS}
        alpha_results[key] = {
            "alpha": alpha,
            "arm_metrics": arm_metrics,
            "targeted_readouts": _targeted_readouts(scored_rows, alpha=alpha),
            "hybrid_lift": _hybrid_lift(scored_rows, alpha=alpha),
        }

    primary_key = f"hybrid_alpha_{str(PRIMARY_ALPHA).replace('.', '_')}"
    primary = alpha_results[primary_key]
    stage, rescue, rationale = _recommended_next_stage(
        targeted=primary["targeted_readouts"],
        hybrid_lift=primary["hybrid_lift"],
    )

    inputs = [
        {"name": "sensitivity_artifact", "path": portable_repo_path(sens_path), "sha256": sha256_file(sens_path)},
        {"name": "label_dataset", "path": portable_repo_path(label_path), "sha256": label_sha},
        {"name": "readiness_matrix", "path": portable_repo_path(readiness_path), "sha256": sha256_file(readiness_path)},
        {
            "name": "embeddings_provenance",
            "path": portable_repo_path(embeddings_path),
            "sha256": sha256_file(embeddings_path),
        },
    ]
    if shadow_path is not None and shadow_path.is_file():
        inputs.append(
            {
                "name": "shadow_pilot_artifact",
                "path": portable_repo_path(shadow_path),
                "sha256": sha256_file(shadow_path),
            }
        )
    if linear_hybrid_eval_v3_path is not None and linear_hybrid_eval_v3_path.is_file():
        inputs.append(
            {
                "name": "linear_hybrid_eval_v3",
                "path": portable_repo_path(linear_hybrid_eval_v3_path.resolve()),
                "sha256": sha256_file(linear_hybrid_eval_v3_path),
            }
        )

    caveats = list(CAVEATS)
    if rank_percentile_scope == SCOPE_LABELED_SLICE:
        caveats.append(
            f"rank_percentile_scope={SCOPE_LABELED_SLICE}; percentiles computed over "
            f"{EXPECTED_SHADOW_ROWS} labeled shadow rows only."
        )
        if scope_fallback_reason:
            caveats.append(f"Full-pool scope unavailable: {scope_fallback_reason}.")

    return {
        "artifact_type": ARTIFACT_TYPE,
        "eval_version": EVAL_VERSION,
        "generated_at": _now_iso_z(),
        "target": TARGET,
        "shadow_ranking_run_id": SHADOW_RANKING_RUN_ID,
        "shadow_pool_variant": SHADOW_POOL_VARIANT,
        "selected_frozen_coefficient_C": SELECTED_FROZEN_C,
        "primary_hybrid_alpha": PRIMARY_ALPHA,
        "hybrid_formula": HYBRID_FORMULA,
        "rank_pct_method": RANK_PCT_METHOD,
        "rank_percentile_scope": rank_percentile_scope,
        "scope_fallback_reason": scope_fallback_reason,
        "pool_candidate_count": pool_candidate_count,
        "ml_probability_source_for_rank_pct": ml_prob_source,
        "inputs": inputs,
        "prerequisite": {
            "sensitivity_artifact_version": SENSITIVITY_ARTIFACT_VERSION,
            "ready_for_offline_hybrid_eval": True,
            "selected_frozen_scorer_reference_only": True,
        },
        "bridge_score_coverage": bridge_coverage,
        "shadow_slice_summary": {
            "row_count": EXPECTED_SHADOW_ROWS,
            "positive_count": EXPECTED_SHADOW_POSITIVE,
            "negative_count": EXPECTED_SHADOW_NEGATIVE,
        },
        "alpha_results": alpha_results,
        "primary_summary": {
            "alpha": PRIMARY_ALPHA,
            "arm_metrics": primary["arm_metrics"],
            "targeted_readouts": primary["targeted_readouts"],
            "hybrid_lift": primary["hybrid_lift"],
        },
        "recommended_next_stage": stage,
        "hybrid_rescue_confirmed": rescue,
        "recommended_next_stage_rationale": rationale,
        "comparison_to_linear_hybrid_eval_v3": {
            "linear_eval_version": LINEAR_HYBRID_EVAL_V3_VERSION,
            "note": (
                "Linear min-max hybrid eval v3 on the same 60-row slice is the guardrail baseline; "
                "this artifact tests rank-percentile formula alignment with shadow pilot."
            ),
        },
        "caveats": caveats,
        "labeled_row_scores": scored_rows,
    }


def markdown_from_ml_offline_bridge_hybrid_rank_pct_eval_v3(payload: dict[str, Any]) -> str:
    sens_input = next((i for i in payload.get("inputs", []) if i.get("name") == "sensitivity_artifact"), {})
    primary = payload.get("primary_summary", {})
    arm_metrics = primary.get("arm_metrics", {})
    targeted = primary.get("targeted_readouts", {})
    lift = primary.get("hybrid_lift", {})

    lines = [
        "# Offline bridge hybrid rank-percentile eval v3",
        "",
        "Offline diagnostic: rank-percentile blend of v3 ML + bridge_score on the shadow-pilot "
        "labeled slice. Not validation; no serving change.",
        "",
        "## Scope",
        "",
        f"- rank_percentile_scope: `{payload.get('rank_percentile_scope')}`",
        f"- pool_candidate_count: `{payload.get('pool_candidate_count')}`",
        f"- ml_probability_source_for_rank_pct: `{payload.get('ml_probability_source_for_rank_pct')}`",
        f"- hybrid_formula: `{payload.get('hybrid_formula')}`",
        f"- rank_pct_method: `{payload.get('rank_pct_method')}`",
    ]
    if payload.get("scope_fallback_reason"):
        lines.append(f"- scope_fallback_reason: `{payload.get('scope_fallback_reason')}`")
    lines.extend(
        [
            "",
            "## Prerequisite",
            "",
            f"- Sensitivity artifact: `{sens_input.get('path', '(unknown)')}`",
            f"- SHA256: `{sens_input.get('sha256', '(unknown)')}`",
            f"- Selected C: `{payload.get('selected_frozen_coefficient_C')}`",
            "",
            "## Shadow slice",
            "",
            f"- Ranking run: `{payload.get('shadow_ranking_run_id')}`",
            f"- Rows: `{payload['shadow_slice_summary']['row_count']}` "
            f"({payload['shadow_slice_summary']['positive_count']} pos / "
            f"{payload['shadow_slice_summary']['negative_count']} neg)",
            f"- bridge_score coverage: "
            f"{payload['bridge_score_coverage']['bridge_score_covered_rows']}/"
            f"{payload['bridge_score_coverage']['shadow_rows']}",
            "",
            "## Arm comparison (primary alpha=0.5, rank percentiles)",
            "",
            "| arm | ROC AUC | AP | P@10 | P@20 | pairwise |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for arm in ARMS:
        metrics = arm_metrics.get(arm, {})
        lines.append(
            f"| `{arm}` | {metrics.get('roc_auc')} | {metrics.get('average_precision')} | "
            f"{metrics.get('precision_at_10')} | {metrics.get('precision_at_20')} | "
            f"{metrics.get('pairwise_accuracy')} |"
        )

    lines.extend(
        [
            "",
            "## Targeted readout verdicts (primary alpha=0.5)",
            "",
            "| bucket | arm | verdict / key metric |",
            "|---|---|---|",
        ]
    )
    for bucket in ("high_bridge_score_low_ml", "promoted_by_hybrid", "demoted_by_hybrid"):
        bucket_data = targeted.get(bucket, {})
        if not isinstance(bucket_data, dict):
            continue
        for arm in ARMS:
            arm_data = bucket_data.get(arm, {})
            if not isinstance(arm_data, dict):
                continue
            verdict = arm_data.get("verdict")
            if verdict is None and bucket == "high_bridge_score_low_ml":
                verdict = arm_data.get("pairwise_pos_above_neg")
            lines.append(f"| `{bucket}` | `{arm}` | {verdict} |")

    lines.extend(
        [
            "",
            "## Hybrid lift (primary alpha=0.5)",
            "",
            f"- P@20 delta vs pure_ml: `{lift.get('precision_at_20_delta_hybrid_minus_pure_ml')}`",
            f"- P@20 delta vs pure_bridge: `{lift.get('precision_at_20_delta_hybrid_minus_pure_bridge')}`",
            f"- hybrid_hurts_ml_precision: `{lift.get('hybrid_hurts_ml_precision')}`",
            "",
            "## Recommendation",
            "",
            f"- **recommended_next_stage:** `{payload.get('recommended_next_stage')}`",
            f"- **hybrid_rescue_confirmed:** `{payload.get('hybrid_rescue_confirmed')}`",
            f"- {payload.get('recommended_next_stage_rationale')}",
            "",
            "## Caveats",
            "",
        ]
    )
    for caveat in payload.get("caveats", []):
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def write_ml_offline_bridge_hybrid_rank_pct_eval_v3(
    *,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
    shadow_pilot_artifact_path: Path | None = None,
    database_url: str | None = None,
    linear_hybrid_eval_v3_path: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_bridge_hybrid_rank_pct_eval_v3_payload(
        sensitivity_artifact_path=sensitivity_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        shadow_pilot_artifact_path=shadow_pilot_artifact_path,
        database_url=database_url,
        linear_hybrid_eval_v3_path=linear_hybrid_eval_v3_path,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_output is not None:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(
            markdown_from_ml_offline_bridge_hybrid_rank_pct_eval_v3(payload), encoding="utf-8"
        )
    return payload


def run_ml_offline_bridge_hybrid_rank_pct_eval_v3_cli(
    *,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
    shadow_pilot_artifact_path: Path | None = None,
    database_url: str | None = None,
    linear_hybrid_eval_v3_path: Path | None = None,
) -> dict[str, Any]:
    return write_ml_offline_bridge_hybrid_rank_pct_eval_v3(
        sensitivity_artifact_path=sensitivity_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        output_json=output_json,
        markdown_output=markdown_output,
        shadow_pilot_artifact_path=shadow_pilot_artifact_path,
        database_url=database_url,
        linear_hybrid_eval_v3_path=linear_hybrid_eval_v3_path,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "EVAL_VERSION",
    "MLOfflineBridgeHybridRankPctEvalV3Error",
    "SCOPE_FULL_POOL",
    "SCOPE_LABELED_SLICE",
    "build_ml_offline_bridge_hybrid_rank_pct_eval_v3_payload",
    "markdown_from_ml_offline_bridge_hybrid_rank_pct_eval_v3",
    "run_ml_offline_bridge_hybrid_rank_pct_eval_v3_cli",
    "write_ml_offline_bridge_hybrid_rank_pct_eval_v3",
]
