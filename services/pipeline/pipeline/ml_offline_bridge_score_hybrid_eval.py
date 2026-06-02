"""Offline hybrid evaluation: v2 OOF probabilities combined with bridge_score from a
clustering-enabled ranking run.

This is the first eval that has a real ``bridge_score`` signal (not NULL), produced by
running ``cluster-works`` and then ``ranking-run --cluster-version`` on the same snapshot
and embedding version as ``rank-83787b91ef``.

Inputs:
  - ml-label-dataset-v13.json (100 labeled rows)
  - ml-offline-bridge-recommendable-scorer-v2.json (OOF predictions for those 100 rows)
  - A clustering-enabled ranking run ID (new run; bridge_score populated on bridge rows)
  - DATABASE_URL for a SELECT-only query of paper_scores + works

Arms compared:
  - learned_v2_oof              rank_pct(oof_probability) [all 100 rows]
  - bridge_score_heuristic      rank_pct(bridge_score) [bridge_score-covered rows only]
  - hybrid_bridge_score_50_50   0.5 * rank_pct(bridge_score) + 0.5 * rank_pct(oof_prob) [covered rows]
  - hybrid_bridge_score_70_30_ml  0.7 * rank_pct(bridge_score) + 0.3 * rank_pct(oof_prob)
  - hybrid_bridge_score_30_70_ml  0.3 * rank_pct(bridge_score) + 0.7 * rank_pct(oof_prob)

Primary confirmatory arm: ``hybrid_bridge_score_50_50``

No DB writes, no ranking writes, no serving changes.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row
from sklearn.metrics import average_precision_score

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_baseline_eval import pairwise_accuracy, precision_at_k, roc_auc_mann_whitney, sha256_file
from pipeline.openalex_ids import normalize_w_token
from pipeline.repo_paths import portable_repo_path

EVAL_VERSION = "ml-offline-bridge-score-hybrid-eval-v1"
ARTIFACT_TYPE = "ml_offline_bridge_score_hybrid_eval"
V2_SCORER_ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer_v2"
V2_SCORER_VERSION = "ml-offline-bridge-recommendable-scorer-v2"
LABEL_DATASET_VERSION = "ml-label-dataset-v13"
EXPECTED_SLICE_ROWS = 100
EXPECTED_POSITIVE_COUNT = 53
EXPECTED_NEGATIVE_COUNT = 47
TARGET = "bridge_recommendable"
FAMILY = "bridge"
ALLOWED_REVIEW_POOL_VARIANTS = frozenset({
    "ml_bridge_negative_mining_audit",
    "ml_bridge_top_ranked_validation_audit",
})
PRIMARY_CONFIRMATORY_ARM = "hybrid_bridge_score_50_50"
BRIDGE_SCORE_MIN_COVERAGE = 80  # minimum non-null bridge_score rows to proceed

ARM_FORMULAS: dict[str, str] = {
    "learned_v2_oof": "rank_pct(oof_probability)",
    "bridge_score_heuristic": "rank_pct(bridge_score)",
    "hybrid_bridge_score_50_50": "0.5 * rank_pct(bridge_score) + 0.5 * rank_pct(oof_probability)",
    "hybrid_bridge_score_70_30_ml": "0.7 * rank_pct(bridge_score) + 0.3 * rank_pct(oof_probability)",
    "hybrid_bridge_score_30_70_ml": "0.3 * rank_pct(bridge_score) + 0.7 * rank_pct(oof_probability)",
}

METRIC_FIELDS = (
    "roc_auc",
    "average_precision",
    "pairwise_accuracy",
    "precision_at_5",
    "precision_at_10",
    "precision_at_20",
    "top_20_positive_count",
)

CAVEATS = (
    "This is not validation.",
    "This is a worksheet-selected two-slice offline diagnostic (100 rows).",
    "Rank percentiles are labeled_slice_only for covered rows; not full-pool production scores.",
    "bridge_score arm and hybrids use only rows where bridge_score is non-null in the new run.",
    "learned_v2_oof uses all 100 rows.",
    "Primary confirmatory arm is hybrid_bridge_score_50_50.",
    "No DB writes, ranking writes, serving changes, or production authorization.",
)

WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|merge|grant|revoke|vacuum|reindex|copy)\b"
)


class MLOfflineBridgeScoreHybridEvalError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineBridgeScoreHybridEvalError(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineBridgeScoreHybridEvalError(f"{label} JSON must be an object: {path}")
    return payload


def _execute_select(cur: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    stripped = sql.strip()
    lowered = stripped.lower()
    if not lowered.startswith("select"):
        raise MLOfflineBridgeScoreHybridEvalError("DB safety violation: SQL must start with SELECT")
    if WRITE_SQL_RE.search(lowered):
        raise MLOfflineBridgeScoreHybridEvalError("DB safety violation: SQL contains write/DDL verb")
    return cur.execute(sql, tuple(params or ()))


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_label_dataset(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"label dataset must be {LABEL_DATASET_VERSION}; got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineBridgeScoreHybridEvalError("label dataset missing rows array")
    slice_rows = [
        r for r in rows
        if isinstance(r, dict) and r.get("review_pool_variant") in ALLOWED_REVIEW_POOL_VARIANTS
    ]
    if len(slice_rows) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"expected {EXPECTED_SLICE_ROWS} slice rows; got {len(slice_rows)}"
        )
    pos = sum(1 for r in slice_rows if r.get(TARGET) is True)
    neg = sum(1 for r in slice_rows if r.get(TARGET) is False)
    if pos != EXPECTED_POSITIVE_COUNT or neg != EXPECTED_NEGATIVE_COUNT:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"slice label counts mismatch: pos={pos} (expected {EXPECTED_POSITIVE_COUNT}), "
            f"neg={neg} (expected {EXPECTED_NEGATIVE_COUNT})"
        )
    return slice_rows


def _validate_v2_scorer(
    scorer_payload: dict[str, Any],
    *,
    slice_rows: Sequence[Mapping[str, Any]],
) -> dict[str, float]:
    if scorer_payload.get("artifact_type") != V2_SCORER_ARTIFACT_TYPE:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"v2 scorer artifact_type must be {V2_SCORER_ARTIFACT_TYPE!r}; "
            f"got {scorer_payload.get('artifact_type')!r}"
        )
    if scorer_payload.get("scorer_version") != V2_SCORER_VERSION:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"v2 scorer scorer_version must be {V2_SCORER_VERSION!r}; "
            f"got {scorer_payload.get('scorer_version')!r}"
        )
    learned_cv = (scorer_payload.get("evaluation") or {}).get("learned_cv")
    if not isinstance(learned_cv, dict):
        raise MLOfflineBridgeScoreHybridEvalError("v2 scorer missing evaluation.learned_cv")
    oof_predictions = learned_cv.get("oof_predictions")
    if not isinstance(oof_predictions, list):
        raise MLOfflineBridgeScoreHybridEvalError("v2 scorer missing evaluation.learned_cv.oof_predictions")
    if len(oof_predictions) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"v2 scorer OOF count={len(oof_predictions)}; expected {EXPECTED_SLICE_ROWS}"
        )
    slice_row_ids = {str(r.get("row_id") or "") for r in slice_rows}
    if "" in slice_row_ids:
        raise MLOfflineBridgeScoreHybridEvalError("labeled slice contains blank row_id")

    oof_by_row: dict[str, float] = {}
    for pred in oof_predictions:
        if not isinstance(pred, dict):
            raise MLOfflineBridgeScoreHybridEvalError("v2 scorer OOF entry must be an object")
        row_id = str(pred.get("row_id") or "")
        if not row_id:
            raise MLOfflineBridgeScoreHybridEvalError("v2 scorer OOF entry missing row_id")
        if row_id in oof_by_row:
            raise MLOfflineBridgeScoreHybridEvalError(f"duplicate OOF row_id: {row_id!r}")
        prob = _as_float(pred.get("probability"))
        if prob is None or prob < 0.0 or prob > 1.0:
            raise MLOfflineBridgeScoreHybridEvalError(f"invalid OOF probability for row_id={row_id!r}")
        oof_by_row[row_id] = prob

    if set(oof_by_row) != slice_row_ids:
        missing = sorted(slice_row_ids - set(oof_by_row))[:5]
        extra = sorted(set(oof_by_row) - slice_row_ids)[:5]
        raise MLOfflineBridgeScoreHybridEvalError(
            f"OOF row_id set does not match labeled slice; missing={missing}, extra={extra}"
        )
    return oof_by_row


# ---------------------------------------------------------------------------
# DB query
# ---------------------------------------------------------------------------

def _fetch_bridge_scores(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
) -> dict[str, float | None]:
    """Return a mapping of normalized OpenAlex W-token → bridge_score (None = null in DB)."""
    sql = """
        SELECT w.openalex_id, ps.bridge_score
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = %s
    """
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(cur, sql.strip(), (ranking_run_id, FAMILY))
        rows = cur.fetchall()

    if not rows:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"no bridge family rows found for ranking_run_id={ranking_run_id!r}; "
            "run cluster-works + ranking-run first"
        )
    out: dict[str, float | None] = {}
    for row in rows:
        token = normalize_w_token(str(row.get("openalex_id") or ""))
        if token:
            score = row.get("bridge_score")
            out[token] = _as_float(score)
    return out


def _match_bridge_scores(
    slice_rows: Sequence[Mapping[str, Any]],
    bridge_scores_by_token: Mapping[str, float | None],
) -> dict[str, float | None]:
    """Return row_id → bridge_score for each labeled row (None if not matched or null)."""
    result: dict[str, float | None] = {}
    for row in slice_rows:
        row_id = str(row.get("row_id") or "")
        wid = str(row.get("work_id") or "")
        token = normalize_w_token(wid)
        if token and token in bridge_scores_by_token:
            result[row_id] = bridge_scores_by_token[token]
        else:
            result[row_id] = None
    return result


# ---------------------------------------------------------------------------
# Rank-percentile helpers
# ---------------------------------------------------------------------------

def _rank_pct_from_list(values: list[tuple[str, float]]) -> dict[str, float]:
    """Compute rank percentile for a list of (work_id, score) pairs.

    Higher score → higher rank percentile (1.0 = best). Ties get average rank.
    Returns a dict keyed by work_id.
    """
    n = len(values)
    if n == 0:
        return {}
    if n == 1:
        return {values[0][0]: 1.0}
    ordered = sorted(values, key=lambda t: (-t[1], t[0]))
    out: dict[str, float] = {}
    i = 0
    while i < n:
        j = i + 1
        score = ordered[i][1]
        while j < n and ordered[j][1] == score:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        pct = 1.0 - (avg_rank - 1.0) / (n - 1.0)
        for k in range(i, j):
            out[ordered[k][0]] = float(pct)
        i = j
    return out


def _compute_arm_scores(
    slice_rows: Sequence[Mapping[str, Any]],
    *,
    oof_by_row: Mapping[str, float],
    bridge_score_by_row: Mapping[str, float | None],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Assemble per-row arm scores and coverage statistics.

    Returns (scored_rows, coverage_summary).
    scored_rows: each dict has row fields + arm_scores dict.
    """
    row_ids = [str(r.get("row_id") or "") for r in slice_rows]
    dup = sorted(k for k, v in Counter(row_ids).items() if k and v > 1)
    if dup:
        raise MLOfflineBridgeScoreHybridEvalError(f"duplicate row_ids in slice: {dup[:5]}")

    # Build coverage info
    covered_rows = [
        r for r in slice_rows
        if bridge_score_by_row.get(str(r.get("row_id") or "")) is not None
    ]
    covered_count = len(covered_rows)
    if covered_count < BRIDGE_SCORE_MIN_COVERAGE:
        raise MLOfflineBridgeScoreHybridEvalError(
            f"bridge_score coverage too low: {covered_count}/{len(slice_rows)} rows have non-null "
            f"bridge_score (minimum {BRIDGE_SCORE_MIN_COVERAGE}). "
            "Ensure cluster-works + ranking-run have been run for the target ranking_run_id."
        )

    # Rank percentiles for OOF probs (all 100 rows, keyed by work_id for the rank_pct helper)
    # We use row_id as the key since work_id is what we have
    oof_pairs: list[tuple[str, float]] = [
        (str(r.get("row_id") or ""), float(oof_by_row[str(r.get("row_id") or "")]))
        for r in slice_rows
    ]
    oof_rank_pct = _rank_pct_from_list(oof_pairs)

    # Rank percentiles for bridge_score (covered rows only)
    bridge_pairs: list[tuple[str, float]] = [
        (str(r.get("row_id") or ""), float(bridge_score_by_row[str(r.get("row_id") or "")]))  # type: ignore[arg-type]
        for r in slice_rows
        if bridge_score_by_row.get(str(r.get("row_id") or "")) is not None
    ]
    bridge_rank_pct = _rank_pct_from_list(bridge_pairs)

    scored: list[dict[str, Any]] = []
    for row in slice_rows:
        row_id = str(row.get("row_id") or "")
        oof_prob = float(oof_by_row[row_id])
        bs = bridge_score_by_row.get(row_id)
        oof_pct = float(oof_rank_pct[row_id])

        arm_scores: dict[str, float | None] = {
            "learned_v2_oof": oof_pct,
        }
        if bs is not None:
            bp = float(bridge_rank_pct[row_id])
            arm_scores["bridge_score_heuristic"] = bp
            arm_scores["hybrid_bridge_score_50_50"] = 0.5 * bp + 0.5 * oof_pct
            arm_scores["hybrid_bridge_score_70_30_ml"] = 0.7 * bp + 0.3 * oof_pct
            arm_scores["hybrid_bridge_score_30_70_ml"] = 0.3 * bp + 0.7 * oof_pct
        else:
            arm_scores["bridge_score_heuristic"] = None
            arm_scores["hybrid_bridge_score_50_50"] = None
            arm_scores["hybrid_bridge_score_70_30_ml"] = None
            arm_scores["hybrid_bridge_score_30_70_ml"] = None

        scored.append({
            "row_id": row_id,
            "work_id": str(row.get("work_id") or ""),
            "paper_id": row.get("paper_id"),
            "title": row.get("title"),
            TARGET: bool(row[TARGET]),
            "bridge_like_label": row.get("bridge_like_label"),
            "relevance_label": row.get("relevance_label"),
            "review_pool_variant": row.get("review_pool_variant"),
            "oof_probability": oof_prob,
            "bridge_score": bs,
            "oof_rank_pct": oof_pct,
            "bridge_score_rank_pct": float(bridge_rank_pct[row_id]) if bs is not None else None,
            "arm_scores": arm_scores,
        })

    coverage_summary = {
        "total_rows": len(slice_rows),
        "bridge_score_covered_rows": covered_count,
        "bridge_score_null_rows": len(slice_rows) - covered_count,
        "coverage_fraction": round(covered_count / len(slice_rows), 4),
    }
    return scored, coverage_summary


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _arm_metrics(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    arm_name: str,
) -> dict[str, Any]:
    pairs: list[tuple[float, bool]] = []
    sortable: list[tuple[float, str, bool]] = []
    skipped = 0
    for row in scored_rows:
        arm_scores = row.get("arm_scores") or {}
        score = _as_float(arm_scores.get(arm_name))
        label = bool(row[TARGET])
        work_id = str(row.get("work_id") or "")
        if score is None:
            skipped += 1
            continue
        pairs.append((score, label))
        sortable.append((score, work_id, label))

    row_count = len(pairs)
    pos = sum(1 for _, l in pairs if l)
    neg = row_count - pos

    if pos == 0 or neg == 0:
        return {
            "status": "not_applicable",
            "reason": "insufficient class coverage",
            "row_count": row_count,
            "positive_count": pos,
            "negative_count": neg,
            "skipped_null_count": skipped,
            "roc_auc": None,
            "average_precision": None,
            "pairwise_accuracy": None,
            "precision_at_5": None,
            "precision_at_10": None,
            "precision_at_20": None,
            "top_20_positive_count": None,
        }

    desc = sorted(sortable, key=lambda t: (-t[0], t[1]))
    desc_pairs = [(s, l) for s, _, l in desc]
    scores = [s for s, _ in pairs]
    labels = [l for _, l in pairs]
    return {
        "status": "ok",
        "formula": ARM_FORMULAS[arm_name],
        "row_count": row_count,
        "positive_count": pos,
        "negative_count": neg,
        "skipped_null_count": skipped,
        "roc_auc": roc_auc_mann_whitney(pairs),
        "average_precision": float(average_precision_score(labels, scores)),
        "pairwise_accuracy": pairwise_accuracy(pairs),
        "precision_at_5": precision_at_k(desc_pairs, 5),
        "precision_at_10": precision_at_k(desc_pairs, 10),
        "precision_at_20": precision_at_k(desc_pairs, 20),
        "top_20_positive_count": int(sum(1 for _, l in desc_pairs[:20] if l)),
    }


def _metric_deltas(
    arm_metrics: Mapping[str, Mapping[str, Any]],
    *,
    baseline_arm: str,
) -> dict[str, dict[str, float | None]]:
    baseline = arm_metrics.get(baseline_arm, {})
    out: dict[str, dict[str, float | None]] = {}
    for arm_name, metrics in arm_metrics.items():
        deltas: dict[str, float | None] = {}
        for key in METRIC_FIELDS:
            v = metrics.get(key)
            b = baseline.get(key)
            if isinstance(v, (int, float)) and not isinstance(v, bool) and isinstance(b, (int, float)) and not isinstance(b, bool):
                deltas[key] = float(v) - float(b)
            else:
                deltas[key] = None
        out[arm_name] = deltas
    return out


def _best_arm_by(arm_metrics: Mapping[str, Mapping[str, Any]], *, metric: str) -> dict[str, Any]:
    best_name: str | None = None
    best_val: float | None = None
    for arm_name in ARM_FORMULAS:
        v = arm_metrics.get(arm_name, {}).get(metric)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if best_val is None or float(v) > best_val:
                best_name = arm_name
                best_val = float(v)
    return {"arm": best_name, "metric": metric, "value": best_val, "exploratory_only": True}


def _recommended_next_stage(arm_metrics: Mapping[str, Mapping[str, Any]]) -> str:
    learned = arm_metrics.get("learned_v2_oof", {})
    bridge_h = arm_metrics.get("bridge_score_heuristic", {})
    primary = arm_metrics.get(PRIMARY_CONFIRMATORY_ARM, {})

    l_auc = learned.get("roc_auc")
    b_auc = bridge_h.get("roc_auc")
    p_auc = primary.get("roc_auc")
    l_ap = learned.get("average_precision")
    b_ap = bridge_h.get("average_precision")
    p_ap = primary.get("average_precision")

    # bridge_score not available
    if b_auc is None or b_ap is None:
        return "bridge_score_not_populated_rerun_cluster_works"

    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (l_auc, b_auc, p_auc, l_ap, b_ap, p_ap)):
        return "insufficient_data_collect_more_labels"

    l_auc, b_auc, p_auc = float(l_auc), float(b_auc), float(p_auc)
    l_ap, b_ap, p_ap = float(l_ap), float(b_ap), float(p_ap)

    if p_auc > l_auc and p_auc > b_auc and p_ap > l_ap and p_ap > b_ap:
        return "bridge_shadow_offline_pilot_plan_v1"
    if l_auc > p_auc and l_ap > p_ap:
        return "ml_only_beats_hybrid_collect_more_labels"
    if b_auc > l_auc and b_ap > l_ap:
        return "bridge_score_beats_ml_consider_weight_tuning"
    return "collect_more_labels_or_tune_hybrid_weights"


def _disagreement_preview(scored_rows: Sequence[Mapping[str, Any]], *, limit: int = 8) -> dict[str, Any]:
    covered = [r for r in scored_rows if r.get("bridge_score") is not None]
    if not covered:
        return {"status": "no_bridge_score_coverage", "rows": []}

    def _preview(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "row_id": row.get("row_id"),
            "work_id": row.get("work_id"),
            "title": row.get("title"),
            TARGET: row.get(TARGET),
            "bridge_like_label": row.get("bridge_like_label"),
            "oof_probability": row.get("oof_probability"),
            "bridge_score": row.get("bridge_score"),
            "oof_rank_pct": row.get("oof_rank_pct"),
            "bridge_score_rank_pct": row.get("bridge_score_rank_pct"),
            "review_pool_variant": row.get("review_pool_variant"),
        }

    high_ml = sorted(
        covered,
        key=lambda r: (
            -(float(r["oof_rank_pct"]) - float(r["bridge_score_rank_pct"])),  # type: ignore[arg-type]
            str(r.get("work_id") or ""),
        ),
    )
    high_bridge = sorted(
        covered,
        key=lambda r: (
            -(float(r["bridge_score_rank_pct"]) - float(r["oof_rank_pct"])),  # type: ignore[arg-type]
            str(r.get("work_id") or ""),
        ),
    )
    return {
        "status": "ok",
        "high_ml_low_bridge_score": [_preview(r) for r in high_ml[:limit]],
        "high_bridge_score_low_ml": [_preview(r) for r in high_bridge[:limit]],
    }


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def _input_record(name: str, path: Path) -> dict[str, str]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise MLOfflineBridgeScoreHybridEvalError(f"required input not found: {resolved}")
    return {"name": name, "path": portable_repo_path(resolved), "sha256": sha256_file(resolved)}


def build_ml_offline_bridge_score_hybrid_eval_payload(
    *,
    label_dataset_path: Path,
    v2_scorer_path: Path,
    ranking_run_id: str,
    database_url: str,
) -> dict[str, Any]:
    ranking_run_id = ranking_run_id.strip()
    if not ranking_run_id:
        raise MLOfflineBridgeScoreHybridEvalError("ranking_run_id must be non-empty")

    label_path = label_dataset_path.resolve()
    scorer_path = v2_scorer_path.resolve()
    inputs = [
        _input_record("label_dataset", label_path),
        _input_record("v2_scorer", scorer_path),
    ]
    input_sha256s = {r["name"]: r["sha256"] for r in inputs}

    label_payload = _load_json_object(label_path, label="label dataset")
    scorer_payload = _load_json_object(scorer_path, label="v2 scorer")

    slice_rows = _validate_label_dataset(label_payload)
    oof_by_row = _validate_v2_scorer(scorer_payload, slice_rows=slice_rows)

    # DB query
    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
        bridge_scores_by_token = _fetch_bridge_scores(conn, ranking_run_id=ranking_run_id)

    bridge_score_by_row = _match_bridge_scores(slice_rows, bridge_scores_by_token)

    scored_rows, coverage = _compute_arm_scores(
        slice_rows,
        oof_by_row=oof_by_row,
        bridge_score_by_row=bridge_score_by_row,
    )

    arm_metrics = {arm: _arm_metrics(scored_rows, arm_name=arm) for arm in ARM_FORMULAS}
    deltas_vs_learned = _metric_deltas(arm_metrics, baseline_arm="learned_v2_oof")
    deltas_vs_bridge = _metric_deltas(arm_metrics, baseline_arm="bridge_score_heuristic")
    for arm, metrics in arm_metrics.items():
        metrics["delta_vs_learned_v2_oof"] = deltas_vs_learned[arm]
        metrics["delta_vs_bridge_score_heuristic"] = deltas_vs_bridge[arm]

    slice_pos = sum(1 for r in scored_rows if r[TARGET] is True)
    slice_neg = len(scored_rows) - slice_pos

    payload: dict[str, Any] = {
        "artifact_type": ARTIFACT_TYPE,
        "eval_version": EVAL_VERSION,
        "generated_at": _now_iso_z(),
        "ranking_run_id_with_bridge_score": ranking_run_id,
        "inputs": inputs,
        "input_sha256s": input_sha256s,
        "slice_counts": {
            "total_rows": len(scored_rows),
            "positive_count": slice_pos,
            "negative_count": slice_neg,
        },
        "bridge_score_coverage": coverage,
        "arm_formulas": ARM_FORMULAS,
        "primary_confirmatory_arm": PRIMARY_CONFIRMATORY_ARM,
        "arm_metrics": arm_metrics,
        "best_arm_by_roc_auc": _best_arm_by(arm_metrics, metric="roc_auc"),
        "best_arm_by_average_precision": _best_arm_by(arm_metrics, metric="average_precision"),
        "deltas_vs_learned_v2_oof": deltas_vs_learned,
        "deltas_vs_bridge_score_heuristic": deltas_vs_bridge,
        "disagreement_analysis": _disagreement_preview(scored_rows),
        "recommended_next_stage": _recommended_next_stage(arm_metrics),
        "caveats": list(CAVEATS),
        "db_access": "SELECT-only on paper_scores + works",
        "db_writes": False,
        "production_authorization": False,
        "labeled_row_scores": scored_rows,
    }
    return payload


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------

def markdown_from_ml_offline_bridge_score_hybrid_eval(payload: Mapping[str, Any]) -> str:
    counts = payload["slice_counts"]
    cov = payload["bridge_score_coverage"]
    arm_metrics = payload["arm_metrics"]
    best_auc = payload["best_arm_by_roc_auc"]
    best_ap = payload["best_arm_by_average_precision"]
    run_id = payload.get("ranking_run_id_with_bridge_score", "?")

    lines = [
        "# Offline bridge_score hybrid eval v1",
        "",
        "Offline diagnostic: ML v2 OOF probabilities vs `bridge_score` from a clustering-enabled "
        "ranking run. Not validation; no serving change.",
        "",
        f"- Ranking run (bridge_score source): `{run_id}`",
        f"- Label dataset: `{LABEL_DATASET_VERSION}`",
        f"- V2 scorer: `{V2_SCORER_VERSION}`",
        f"- Total labeled rows: {counts.get('total_rows')} "
        f"({counts.get('positive_count')} positive / {counts.get('negative_count')} negative)",
        f"- bridge_score coverage: {cov.get('bridge_score_covered_rows')}/{cov.get('total_rows')} "
        f"({cov.get('coverage_fraction', 0.0):.1%})",
        f"- Primary confirmatory arm: `{payload.get('primary_confirmatory_arm')}`",
        "",
        "## Arms",
        "",
        "| arm | rows | ROC AUC | AP | Pairwise | P@5 | P@10 | P@20 | top20+ |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm_name in ARM_FORMULAS:
        m = arm_metrics.get(arm_name, {})
        status = m.get("status", "?")
        if status != "ok":
            lines.append(f"| `{arm_name}` | {m.get('row_count', 'N/A')} | N/A | N/A | N/A | N/A | N/A | N/A | N/A |")
        else:
            def _fmt(v: Any) -> str:
                return f"{v:.4f}" if isinstance(v, float) else str(v)
            lines.append(
                f"| `{arm_name}` | {m.get('row_count')} | {_fmt(m.get('roc_auc'))} | "
                f"{_fmt(m.get('average_precision'))} | {_fmt(m.get('pairwise_accuracy'))} | "
                f"{_fmt(m.get('precision_at_5'))} | {_fmt(m.get('precision_at_10'))} | "
                f"{_fmt(m.get('precision_at_20'))} | {m.get('top_20_positive_count')} |"
            )
    lines.extend([
        "",
        "## Readout",
        "",
        f"- Best arm by ROC AUC: `{best_auc.get('arm')}` = `{best_auc.get('value')}` (exploratory only)",
        f"- Best arm by average precision: `{best_ap.get('arm')}` = `{best_ap.get('value')}` (exploratory only)",
        f"- Recommended next stage: `{payload.get('recommended_next_stage')}`",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
    ])
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Writer + CLI runner
# ---------------------------------------------------------------------------

def write_ml_offline_bridge_score_hybrid_eval(
    *,
    label_dataset_path: Path,
    v2_scorer_path: Path,
    ranking_run_id: str,
    database_url: str,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_offline_bridge_score_hybrid_eval_payload(
        label_dataset_path=label_dataset_path,
        v2_scorer_path=v2_scorer_path,
        ranking_run_id=ranking_run_id,
        database_url=database_url,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_ml_offline_bridge_score_hybrid_eval(payload), encoding="utf-8"
        )
    return payload


def run_ml_offline_bridge_score_hybrid_eval_cli(
    *,
    label_dataset_path: Path,
    v2_scorer_path: Path,
    ranking_run_id: str,
    database_url: str | None,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    url = database_url or database_url_from_env()
    write_ml_offline_bridge_score_hybrid_eval(
        label_dataset_path=label_dataset_path,
        v2_scorer_path=v2_scorer_path,
        ranking_run_id=ranking_run_id,
        database_url=url,
        json_path=output_json,
        markdown_path=markdown_output,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "EVAL_VERSION",
    "MLOfflineBridgeScoreHybridEvalError",
    "build_ml_offline_bridge_score_hybrid_eval_payload",
    "markdown_from_ml_offline_bridge_score_hybrid_eval",
    "run_ml_offline_bridge_score_hybrid_eval_cli",
    "write_ml_offline_bridge_score_hybrid_eval",
]
