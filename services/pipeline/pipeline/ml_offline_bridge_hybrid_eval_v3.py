"""Offline hybrid evaluation: C=0.001 v3 OOF probabilities + bridge_score on shadow-pilot rows.

Evaluates the 60-row rank-5a7efa5ca3 shadow-pilot slice only. No DB access.
"""

from __future__ import annotations

import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from sklearn.metrics import average_precision_score

from pipeline.ml_offline_baseline_eval import pairwise_accuracy, precision_at_k, roc_auc_mann_whitney, sha256_file
from pipeline.openalex_ids import normalize_w_token
from pipeline.repo_paths import portable_repo_path

EVAL_VERSION = "ml-offline-bridge-hybrid-eval-v3-v1"
ARTIFACT_TYPE = "ml_offline_bridge_hybrid_eval_v3"
SENSITIVITY_ARTIFACT_VERSION = "ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1"
SENSITIVITY_ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity"
LABEL_DATASET_VERSION = "ml-label-dataset-v14"
READINESS_MATRIX_VERSION = "ml-label-readiness-matrix-v11"
EMBEDDINGS_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
TARGET = "bridge_recommendable"
SHADOW_RANKING_RUN_ID = "rank-5a7efa5ca3"
SHADOW_POOL_VARIANT = "ml_bridge_shadow_pilot_audit"
FAMILY = "bridge"
SELECTED_FROZEN_C = 0.001

EXPECTED_SHADOW_ROWS = 60
EXPECTED_SHADOW_POSITIVE = 34
EXPECTED_SHADOW_NEGATIVE = 26
BRIDGE_SCORE_MIN_COVERAGE = 55

HYBRID_ALPHAS = (0.3, 0.5, 0.7)
PRIMARY_ALPHA = 0.5
ARMS = ("pure_ml", "pure_bridge", "hybrid")

BUCKET_EXPECTED = {
    "promoted_by_hybrid": (20, 14, 6),
    "demoted_by_hybrid": (20, 8, 12),
    "high_ml_low_bridge_score": (10, 10, 0),
    "high_bridge_score_low_ml": (10, 2, 8),
}

CAVEATS = (
    "Offline hybrid eval diagnostic only; does not enable Bridge serving or production output.",
    "OOF probabilities from the C=0.001 sensitivity sweep, not new scorer inference.",
    "bridge_score normalization is min-max over the 60-row shadow slice only; "
    "production normalization may differ.",
    "60-row shadow slice evaluation; not powered for precision on 10-row buckets.",
    "alpha sweep is diagnostic; no alpha is authorized for production without further review.",
    "Does not change production default, API/web surface, or user-visible ranking.",
    "Bridge recommendations remain subject to controlled rollout authorization.",
)


class MLOfflineBridgeHybridEvalV3Error(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineBridgeHybridEvalV3Error(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineBridgeHybridEvalV3Error(f"{label} JSON must be an object: {path}")
    return payload


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _work_id_token(value: Any) -> str:
    token = normalize_w_token(str(value or ""))
    if not token:
        raise MLOfflineBridgeHybridEvalV3Error(f"invalid work_id token: {value!r}")
    return token


def _validate_prerequisite_sensitivity_artifact(payload: dict[str, Any], *, path: Path) -> None:
    version = payload.get("artifact_version") or payload.get("scorer_version")
    if version != SENSITIVITY_ARTIFACT_VERSION:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"{path} artifact_version={version!r}; expected {SENSITIVITY_ARTIFACT_VERSION!r}"
        )
    artifact_type = payload.get("artifact_type")
    if artifact_type != SENSITIVITY_ARTIFACT_TYPE:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"{path} artifact_type={artifact_type!r}; expected {SENSITIVITY_ARTIFACT_TYPE!r}"
        )
    if payload.get("ready_for_offline_hybrid_eval") is not True:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"{path} ready_for_offline_hybrid_eval is not true; run regularization sensitivity first"
        )
    selected_c = _as_float(payload.get("selected_frozen_coefficient_C"))
    if selected_c != SELECTED_FROZEN_C:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"{path} selected_frozen_coefficient_C={selected_c!r}; expected {SELECTED_FROZEN_C!r}"
        )
    frozen = payload.get("selected_frozen_scorer")
    if not isinstance(frozen, dict) or not frozen:
        raise MLOfflineBridgeHybridEvalV3Error(f"{path} missing selected_frozen_scorer object")


def _sweep_entry_for_c(payload: dict[str, Any], *, c_value: float) -> dict[str, Any]:
    sweep = payload.get("regularization_sweep")
    if not isinstance(sweep, list):
        raise MLOfflineBridgeHybridEvalV3Error("sensitivity artifact missing regularization_sweep array")
    for entry in sweep:
        if isinstance(entry, dict) and _as_float(entry.get("C")) == c_value:
            return entry
    raise MLOfflineBridgeHybridEvalV3Error(
        f"sensitivity artifact has no regularization_sweep entry for C={c_value!r}"
    )


def _load_oof_by_work_id(sensitivity_payload: dict[str, Any]) -> dict[str, float]:
    entry = _sweep_entry_for_c(sensitivity_payload, c_value=SELECTED_FROZEN_C)
    learned = entry.get("learned_cv_primary_deduped")
    if not isinstance(learned, dict):
        raise MLOfflineBridgeHybridEvalV3Error("C=0.001 sweep entry missing learned_cv_primary_deduped")
    preds = learned.get("oof_predictions_deduped")
    if not isinstance(preds, list):
        raise MLOfflineBridgeHybridEvalV3Error("C=0.001 block missing oof_predictions_deduped")
    out: dict[str, float] = {}
    for idx, pred in enumerate(preds, start=1):
        if not isinstance(pred, dict):
            raise MLOfflineBridgeHybridEvalV3Error(f"oof prediction {idx} is not an object")
        token = _work_id_token(pred.get("work_id"))
        prob = _as_float(pred.get("probability"))
        if prob is None or prob < 0.0 or prob > 1.0:
            raise MLOfflineBridgeHybridEvalV3Error(f"invalid OOF probability for work_id={token!r}")
        if token in out:
            raise MLOfflineBridgeHybridEvalV3Error(f"duplicate OOF work_id {token!r}")
        out[token] = prob
    return out


def _validate_readiness_matrix(
    readiness_payload: dict[str, Any],
    *,
    label_dataset_sha256: str,
) -> None:
    prov = readiness_payload.get("provenance")
    if not isinstance(prov, dict):
        raise MLOfflineBridgeHybridEvalV3Error("readiness matrix missing provenance object")
    if prov.get("label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"readiness matrix must point at {LABEL_DATASET_VERSION}"
        )
    if prov.get("label_dataset_sha256") != label_dataset_sha256:
        raise MLOfflineBridgeHybridEvalV3Error(
            "readiness matrix label_dataset_sha256 does not match label dataset"
        )
    groups = readiness_payload.get("groups")
    if not isinstance(groups, list):
        raise MLOfflineBridgeHybridEvalV3Error("readiness matrix missing groups array")
    shadow_group = None
    for group in groups:
        if not isinstance(group, dict):
            continue
        if (
            group.get("ranking_run_id") == SHADOW_RANKING_RUN_ID
            and group.get("family") == FAMILY
            and group.get("target") == TARGET
        ):
            shadow_group = group
            break
    if shadow_group is None:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"readiness matrix missing ({SHADOW_RANKING_RUN_ID}, bridge, bridge_recommendable) group"
        )
    expected = {
        "total_labeled_rows": EXPECTED_SHADOW_ROWS,
        "positive_count": EXPECTED_SHADOW_POSITIVE,
        "negative_count": EXPECTED_SHADOW_NEGATIVE,
        "paper_scores_joinable_count": EXPECTED_SHADOW_ROWS,
    }
    for key, value in expected.items():
        if shadow_group.get(key) != value:
            raise MLOfflineBridgeHybridEvalV3Error(
                f"readiness shadow group {key}={shadow_group.get(key)!r}; expected {value!r}"
            )


def _validate_embeddings_provenance(payload: dict[str, Any], *, frozen_scorer: Mapping[str, Any]) -> None:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise MLOfflineBridgeHybridEvalV3Error("embeddings provenance missing metadata object")
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"embeddings provenance artifact_version mismatch; expected {EMBEDDINGS_ARTIFACT_VERSION!r}"
        )
    if metadata.get("embedding_version") != frozen_scorer.get("embedding_version"):
        raise MLOfflineBridgeHybridEvalV3Error(
            "embeddings provenance embedding_version does not match selected_frozen_scorer"
        )


def _disagreement_bucket(row: Mapping[str, Any]) -> str:
    ctx = row.get("bridge_shadow_pilot_context")
    if isinstance(ctx, Mapping):
        bucket = ctx.get("disagreement_bucket")
        if isinstance(bucket, str) and bucket.strip():
            return bucket.strip()
    sample_reason = row.get("sample_reason")
    if isinstance(sample_reason, str) and sample_reason.strip():
        return sample_reason.strip()
    raise MLOfflineBridgeHybridEvalV3Error(
        f"shadow row {row.get('row_id')!r} missing disagreement_bucket"
    )


def _slice_shadow_rows(label_payload: dict[str, Any]) -> list[dict[str, Any]]:
    version = label_payload.get("dataset_version")
    if version != LABEL_DATASET_VERSION:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"label dataset version={version!r}; expected {LABEL_DATASET_VERSION!r}"
        )
    rows_in = label_payload.get("rows")
    if not isinstance(rows_in, list):
        raise MLOfflineBridgeHybridEvalV3Error("label dataset missing rows array")
    out: list[dict[str, Any]] = []
    for row in rows_in:
        if not isinstance(row, dict):
            continue
        if row.get("split") != "audit_only":
            continue
        if row.get("family") != FAMILY:
            continue
        if row.get("review_pool_variant") != SHADOW_POOL_VARIANT:
            continue
        out.append(row)
    if len(out) != EXPECTED_SHADOW_ROWS:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"shadow slice has {len(out)} rows; expected {EXPECTED_SHADOW_ROWS}"
        )
    pos = sum(1 for r in out if r.get(TARGET) is True)
    neg = sum(1 for r in out if r.get(TARGET) is False)
    if pos != EXPECTED_SHADOW_POSITIVE or neg != EXPECTED_SHADOW_NEGATIVE:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"shadow slice bridge_recommendable={pos}/{neg}; expected {EXPECTED_SHADOW_POSITIVE}/{EXPECTED_SHADOW_NEGATIVE}"
        )
    return out


def _min_max_norm(values: Sequence[float | None]) -> list[float | None]:
    clean = [float(v) for v in values if isinstance(v, (int, float)) and math.isfinite(float(v))]
    if not clean:
        return [None for _ in values]
    lo = min(clean)
    hi = max(clean)
    if math.isclose(lo, hi):
        return [0.5 if v is not None else None for v in values]
    span = hi - lo
    out: list[float | None] = []
    for v in values:
        if v is None:
            out.append(None)
        else:
            out.append((float(v) - lo) / span)
    return out


def _build_scored_rows(
    shadow_rows: Sequence[Mapping[str, Any]],
    *,
    oof_by_work_id: Mapping[str, float],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    bridge_raw: list[float | None] = []
    for row in shadow_rows:
        ctx = row.get("bridge_shadow_pilot_context")
        raw = None
        if isinstance(ctx, Mapping):
            raw = _as_float(ctx.get("bridge_score"))
        bridge_raw.append(raw)
    covered = sum(1 for v in bridge_raw if v is not None)
    if covered < BRIDGE_SCORE_MIN_COVERAGE:
        raise MLOfflineBridgeHybridEvalV3Error(
            f"bridge_score coverage too low: {covered}/{EXPECTED_SHADOW_ROWS}; "
            f"minimum {BRIDGE_SCORE_MIN_COVERAGE}"
        )
    bridge_normed = _min_max_norm(bridge_raw)

    scored: list[dict[str, Any]] = []
    for row, raw, normed in zip(shadow_rows, bridge_raw, bridge_normed, strict=True):
        token = _work_id_token(row.get("work_id") or row.get("openalex_work_id") or row.get("paper_id"))
        if token not in oof_by_work_id:
            raise MLOfflineBridgeHybridEvalV3Error(f"missing OOF probability for shadow work_id={token!r}")
        ml_prob = oof_by_work_id[token]
        label = row.get(TARGET)
        if label not in (True, False):
            raise MLOfflineBridgeHybridEvalV3Error(f"shadow row {token} has non-boolean {TARGET}")
        scored.append(
            {
                "row_id": row.get("row_id"),
                "work_id": token,
                "disagreement_bucket": _disagreement_bucket(row),
                TARGET: label,
                "ml_prob_oof": ml_prob,
                "bridge_score_raw": raw,
                "bridge_score_normed": normed,
            }
        )
    coverage = {
        "shadow_rows": EXPECTED_SHADOW_ROWS,
        "bridge_score_covered_rows": covered,
        "bridge_score_coverage_fraction": covered / EXPECTED_SHADOW_ROWS,
        "ml_prob_oof_covered_rows": len(scored),
    }
    return scored, coverage


def _hybrid_score(ml_prob: float, bridge_normed: float | None, *, alpha: float) -> float | None:
    if bridge_normed is None:
        return None
    return alpha * ml_prob + (1.0 - alpha) * bridge_normed


def _score_for_arm(row: Mapping[str, Any], *, arm: str, alpha: float) -> float | None:
    if arm == "pure_ml":
        return _as_float(row.get("ml_prob_oof"))
    if arm == "pure_bridge":
        return _as_float(row.get("bridge_score_raw"))
    if arm == "hybrid":
        ml_prob = _as_float(row.get("ml_prob_oof"))
        bridge_normed = _as_float(row.get("bridge_score_normed"))
        if ml_prob is None or bridge_normed is None:
            return None
        return _hybrid_score(ml_prob, bridge_normed, alpha=alpha)
    raise MLOfflineBridgeHybridEvalV3Error(f"unknown arm {arm!r}")


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


def _pos_above_neg_median(
    rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float
) -> int | None:
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


def _verdict_demoted(
    rows: Sequence[Mapping[str, Any]], *, arm: str, alpha: float
) -> dict[str, Any]:
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


def _targeted_readouts(
    scored_rows: Sequence[Mapping[str, Any]], *, alpha: float
) -> dict[str, Any]:
    by_bucket: dict[str, list[dict[str, Any]]] = {}
    for row in scored_rows:
        by_bucket.setdefault(str(row["disagreement_bucket"]), []).append(row)

    readouts: dict[str, Any] = {}
    for bucket, expected in BUCKET_EXPECTED.items():
        rows = by_bucket.get(bucket, [])
        if len(rows) != expected[0]:
            raise MLOfflineBridgeHybridEvalV3Error(
                f"bucket {bucket} has {len(rows)} rows; expected {expected[0]}"
            )

    high_bridge = by_bucket["high_bridge_score_low_ml"]
    high_bridge_block: dict[str, Any] = {}
    for arm in ARMS:
        means = _mean_scores_by_label(high_bridge, arm=arm, alpha=alpha)
        pairwise = _pairwise_pos_above_neg(high_bridge, arm=arm, alpha=alpha)
        verdict = _verdict_high_bridge(high_bridge, arm=arm, alpha=alpha)
        high_bridge_block[arm] = {
            "positive_mean": means["positive_mean"],
            "negative_mean": means["negative_mean"],
            "pairwise_pos_above_neg": pairwise,
            "verdict": verdict,
        }
    readouts["high_bridge_score_low_ml"] = high_bridge_block

    promoted = by_bucket["promoted_by_hybrid"]
    promoted_block: dict[str, Any] = {}
    for arm in ARMS:
        promoted_block[arm] = {
            "precision_at_10_within_bucket": _precision_at_k_within_subset(
                promoted, arm=arm, alpha=alpha, k=10
            ),
            "positives_above_negative_median": _pos_above_neg_median(
                promoted, arm=arm, alpha=alpha
            ),
            "verdict": _verdict_promoted(promoted, arm=arm, alpha=alpha),
        }
    readouts["promoted_by_hybrid"] = promoted_block

    demoted = by_bucket["demoted_by_hybrid"]
    demoted_block: dict[str, Any] = {}
    for arm in ARMS:
        detail = _verdict_demoted(demoted, arm=arm, alpha=alpha)
        demoted_block[arm] = detail
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
            "top_20_count_within_shadow_slice": _top_k_count(
                scored_rows, arm=arm, alpha=alpha, k=20
            ),
        }
    readouts["high_ml_low_bridge_score"] = high_ml_block

    hybrid_pairwise = _pairwise_pos_above_neg(high_bridge, arm="hybrid", alpha=alpha)
    ml_pairwise = _pairwise_pos_above_neg(high_bridge, arm="pure_ml", alpha=alpha)
    readouts["high_bridge_hybrid_vs_ml"] = {
        "hybrid_pairwise": hybrid_pairwise,
        "pure_ml_pairwise": ml_pairwise,
        "hybrid_rescues_vs_pure_ml": (
            hybrid_pairwise is not None
            and ml_pairwise is not None
            and hybrid_pairwise > ml_pairwise
        ),
    }
    return readouts


def _hybrid_lift(
    scored_rows: Sequence[Mapping[str, Any]], *, alpha: float
) -> dict[str, Any]:
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
        stage = "authorize_bridge_hybrid_serving_controlled_rollout_eval"
        rescue = True
        rationale = (
            "Primary alpha=0.5 hybrid rescues high-bridge positives, maintains promoted "
            "bucket signal, separates demotion subgroups, and does not materially hurt ML P@20."
        )
    elif (
        high_verdict == "partial"
        and prom_verdict != "fails"
        and dem_verdict != "fails"
    ):
        stage = "collect_more_high_bridge_score_low_ml_labels_before_serving"
        rescue = False
        rationale = (
            "Hybrid shows partial rescue on high-bridge bucket but not enough confidence "
            "for controlled rollout authorization."
        )
    else:
        stage = "do_not_authorize_bridge_hybrid_serving_recheck_alpha_or_formula"
        rescue = False
        rationale = (
            "Primary alpha=0.5 hybrid fails one or more targeted readouts or hurts ML precision; "
            "recheck alpha or hybrid formula before serving."
        )
    return stage, rescue, rationale


def build_ml_offline_bridge_hybrid_eval_v3_payload(
    *,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v2_scorer_path: Path | None = None,
) -> dict[str, Any]:
    sens_path = sensitivity_artifact_path.resolve()
    label_path = label_dataset_path.resolve()
    readiness_path = readiness_matrix_path.resolve()
    embeddings_path = embeddings_provenance_path.resolve()
    for path in (sens_path, label_path, readiness_path, embeddings_path):
        if not path.is_file():
            raise MLOfflineBridgeHybridEvalV3Error(f"required input not found: {path}")

    sensitivity_payload = _load_json_object(sens_path, label="sensitivity artifact")
    _validate_prerequisite_sensitivity_artifact(sensitivity_payload, path=sens_path)
    frozen_scorer = sensitivity_payload["selected_frozen_scorer"]

    label_payload = _load_json_object(label_path, label="label dataset")
    label_sha = sha256_file(label_path)
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    _validate_readiness_matrix(readiness_payload, label_dataset_sha256=label_sha)
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")
    _validate_embeddings_provenance(embeddings_payload, frozen_scorer=frozen_scorer)

    oof_by_work_id = _load_oof_by_work_id(sensitivity_payload)
    shadow_rows = _slice_shadow_rows(label_payload)
    scored_rows, bridge_coverage = _build_scored_rows(shadow_rows, oof_by_work_id=oof_by_work_id)

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
    if v2_scorer_path is not None and v2_scorer_path.is_file():
        inputs.append(
            {
                "name": "v2_baseline_scorer",
                "path": portable_repo_path(v2_scorer_path.resolve()),
                "sha256": sha256_file(v2_scorer_path),
            }
        )

    return {
        "artifact_type": ARTIFACT_TYPE,
        "eval_version": EVAL_VERSION,
        "generated_at": _now_iso_z(),
        "target": TARGET,
        "shadow_ranking_run_id": SHADOW_RANKING_RUN_ID,
        "shadow_pool_variant": SHADOW_POOL_VARIANT,
        "selected_frozen_coefficient_C": SELECTED_FROZEN_C,
        "primary_hybrid_alpha": PRIMARY_ALPHA,
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
        "caveats": list(CAVEATS),
        "labeled_row_scores": scored_rows,
    }


def markdown_from_ml_offline_bridge_hybrid_eval_v3(payload: dict[str, Any]) -> str:
    sens_input = next((i for i in payload.get("inputs", []) if i.get("name") == "sensitivity_artifact"), {})
    primary = payload.get("primary_summary", {})
    arm_metrics = primary.get("arm_metrics", {})
    targeted = primary.get("targeted_readouts", {})
    lift = primary.get("hybrid_lift", {})

    lines = [
        "# Offline bridge hybrid eval v3",
        "",
        "Offline diagnostic: C=0.001 v3 OOF probabilities combined with bridge_score on the "
        "60-row shadow-pilot slice. Not validation; no serving change.",
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
        "## Arm comparison (primary alpha=0.5)",
        "",
        "| arm | ROC AUC | AP | P@10 | P@20 | pairwise |",
        "|---|---:|---:|---:|---:|---:|",
    ]
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


def write_ml_offline_bridge_hybrid_eval_v3(
    *,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
    v2_scorer_path: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_bridge_hybrid_eval_v3_payload(
        sensitivity_artifact_path=sensitivity_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        v2_scorer_path=v2_scorer_path,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_output is not None:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(
            markdown_from_ml_offline_bridge_hybrid_eval_v3(payload), encoding="utf-8"
        )
    return payload


def run_ml_offline_bridge_hybrid_eval_v3_cli(
    *,
    sensitivity_artifact_path: Path,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
    v2_scorer_path: Path | None = None,
) -> dict[str, Any]:
    return write_ml_offline_bridge_hybrid_eval_v3(
        sensitivity_artifact_path=sensitivity_artifact_path,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        output_json=output_json,
        markdown_output=markdown_output,
        v2_scorer_path=v2_scorer_path,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "EVAL_VERSION",
    "MLOfflineBridgeHybridEvalV3Error",
    "build_ml_offline_bridge_hybrid_eval_v3_payload",
    "markdown_from_ml_offline_bridge_hybrid_eval_v3",
    "run_ml_offline_bridge_hybrid_eval_v3_cli",
    "write_ml_offline_bridge_hybrid_eval_v3",
]
