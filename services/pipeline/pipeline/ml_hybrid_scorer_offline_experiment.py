"""Execute the pre-registered hybrid scorer offline experiment.

This command computes fixed, label-blind hybrid score arms from an existing
production-candidate scoring v3 JSON artifact. It does not train, fit, query a
database, regenerate scores, write ranking rows, or authorize shadow/prod.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment"
EXPERIMENT_VERSION = "ml-hybrid-scorer-offline-experiment-v1"
SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
SCORING_VERSION = "ml-offline-production-candidate-scoring-v3"
SCORING_MODE = "heuristic_and_holdout_embedding_scorer"
METRIC_GATES_VERSION = "ml-offline-production-candidate-metric-gates-v3"
SPEC_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment_spec"
SPEC_VERSION = "ml-hybrid-scorer-offline-experiment-v1-spec"
HOLDOUT_ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"
RECOMMENDED_NEXT_STAGE = "create_hybrid_scorer_offline_experiment_v1"
K_VALUES = (5, 10, 20)
MATERIAL_LIFT_ROC_AUC = 0.03
MATERIAL_LIFT_AVERAGE_PRECISION = 0.02

EXPECTED_ARMS: tuple[tuple[str, str], ...] = (
    ("heuristic_final_score_baseline", "final_score"),
    ("holdout_embedding_probability_baseline", "audit_embedding_probability_work"),
    (
        "hybrid_rank_mean_50_50",
        "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
    ),
    (
        "hybrid_rank_mean_75_25_heuristic",
        "0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)",
    ),
    (
        "hybrid_rank_mean_25_75_heuristic",
        "0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)",
    ),
)
HYBRID_ARM_IDS = {
    "hybrid_rank_mean_50_50",
    "hybrid_rank_mean_75_25_heuristic",
    "hybrid_rank_mean_25_75_heuristic",
}

CAVEATS = (
    "Not live recommender validation.",
    "Pre-registered arms are evaluated on already-seen v3 eval surface.",
    "Best-arm selection is exploratory only.",
    "Single reviewer.",
    "One ranking run/family.",
    "Positive-heavy P@k.",
    "No shadow/production authorization.",
)


class MLHybridScorerOfflineExperimentError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLHybridScorerOfflineExperimentError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLHybridScorerOfflineExperimentError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLHybridScorerOfflineExperimentError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _float_or_none(value: Any) -> float | None:
    if _is_number(value):
        return float(value)
    return None


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLHybridScorerOfflineExperimentError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_scoring(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION:
        raise MLHybridScorerOfflineExperimentError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("scoring_mode") != SCORING_MODE:
        raise MLHybridScorerOfflineExperimentError(
            f"expected scoring metadata.scoring_mode={SCORING_MODE!r}, got {metadata.get('scoring_mode')!r}"
        )
    if _get(payload, "leakage_report.train_rows_used_in_metrics") != 0:
        raise MLHybridScorerOfflineExperimentError("scoring leakage_report.train_rows_used_in_metrics must be 0")
    if _get(payload, "leakage_report.train_works_used_in_metrics") != 0:
        raise MLHybridScorerOfflineExperimentError("scoring leakage_report.train_works_used_in_metrics must be 0")
    if _get(payload, "leakage_report.candidate_pool_work_set_matches_eval_set") is not True:
        raise MLHybridScorerOfflineExperimentError(
            "scoring leakage_report.candidate_pool_work_set_matches_eval_set must be true"
        )
    eval_only = _get(payload, "learned_or_embedding_metrics.eval_only")
    if eval_only is None:
        eval_only = _get(payload, "scoring_mode_details.eval_only")
    if eval_only is not True:
        raise MLHybridScorerOfflineExperimentError("scoring learned metrics must be eval_only")
    rows = payload.get("candidate_pool_rows")
    if not isinstance(rows, list) or not rows:
        raise MLHybridScorerOfflineExperimentError("scoring candidate_pool_rows[] must exist")
    _require_unique_canonical_ids(rows, field_name="candidate_pool_rows")
    eval_rows = payload.get("labeled_eval_subset")
    if not isinstance(eval_rows, list) or not eval_rows:
        raise MLHybridScorerOfflineExperimentError("scoring labeled_eval_subset[] must exist")
    _require_unique_canonical_ids(eval_rows, field_name="labeled_eval_subset")
    if not str(metadata.get("eval_work_set_sha256") or "").strip():
        raise MLHybridScorerOfflineExperimentError("scoring metadata.eval_work_set_sha256 must be present")
    return metadata


def _require_unique_canonical_ids(rows: Sequence[Any], *, field_name: str) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    missing: list[int] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            raise MLHybridScorerOfflineExperimentError(f"{field_name}[{idx}] is not an object")
        work_id = str(row.get("canonical_openalex_work_id") or "").strip()
        if not work_id:
            missing.append(idx)
            continue
        if work_id in seen:
            duplicates.append(work_id)
        seen.add(work_id)
    if missing:
        raise MLHybridScorerOfflineExperimentError(f"{field_name} rows missing canonical_openalex_work_id: {missing[:10]}")
    if duplicates:
        raise MLHybridScorerOfflineExperimentError(
            f"{field_name} contains duplicate canonical_openalex_work_id values: {duplicates[:10]}"
        )


def _validate_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates")
    if metadata.get("artifact_type") != "ml_offline_production_candidate_metric_gates":
        raise MLHybridScorerOfflineExperimentError(
            "expected gates metadata.artifact_type='ml_offline_production_candidate_metric_gates'"
        )
    if metadata.get("gates_version") != METRIC_GATES_VERSION:
        raise MLHybridScorerOfflineExperimentError(
            f"expected gates metadata.gates_version={METRIC_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("independent_learned_validation_passed") is not True:
        raise MLHybridScorerOfflineExperimentError("gates independent_learned_validation_passed must be true")
    if payload.get("material_lift_passed") is not False:
        raise MLHybridScorerOfflineExperimentError("gates material_lift_passed must be false")
    if payload.get("recommended_next_stage") != RECOMMENDED_NEXT_STAGE:
        raise MLHybridScorerOfflineExperimentError(
            f"gates recommended_next_stage must be {RECOMMENDED_NEXT_STAGE!r}"
        )
    return metadata


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="experiment-spec")
    if metadata.get("artifact_type") != SPEC_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentError(
            f"expected spec metadata.artifact_type={SPEC_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("spec_version") != SPEC_VERSION:
        raise MLHybridScorerOfflineExperimentError(
            f"expected spec metadata.spec_version={SPEC_VERSION!r}, got {metadata.get('spec_version')!r}"
        )
    if _get(payload, "future_gate_contract.best_arm_on_seen_eval_is_exploratory_only") is not True:
        raise MLHybridScorerOfflineExperimentError(
            "spec future_gate_contract.best_arm_on_seen_eval_is_exploratory_only must be true"
        )
    arms = payload.get("pre_registered_hybrid_arms")
    if not isinstance(arms, list) or len(arms) != len(EXPECTED_ARMS):
        raise MLHybridScorerOfflineExperimentError("spec pre_registered_hybrid_arms must contain exactly 5 arms")
    observed = [(str(item.get("arm_id")), str(item.get("score_formula"))) for item in arms if isinstance(item, Mapping)]
    expected = list(EXPECTED_ARMS)
    if observed != expected:
        raise MLHybridScorerOfflineExperimentError(
            "spec pre_registered_hybrid_arms must match expected arm ids and formulas exactly"
        )
    return metadata


def _validate_assignment(payload: Mapping[str, Any], *, eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("assignment_version") != HOLDOUT_ASSIGNMENT_VERSION:
        raise MLHybridScorerOfflineExperimentError(
            f"expected assignment metadata.assignment_version={HOLDOUT_ASSIGNMENT_VERSION!r}, got {metadata.get('assignment_version')!r}"
        )
    if _get(payload, "leakage_report.global_zero_assertion") is not True:
        raise MLHybridScorerOfflineExperimentError("assignment leakage_report.global_zero_assertion must be true")
    if metadata.get("eval_work_set_sha256") != eval_sha:
        raise MLHybridScorerOfflineExperimentError("assignment metadata.eval_work_set_sha256 must match scoring/spec")
    assignments = payload.get("assignments")
    if not isinstance(assignments, list) or not assignments:
        raise MLHybridScorerOfflineExperimentError("assignment assignments[] must exist")
    return metadata


def _validate_holdout_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _metadata(payload, name="holdout-policy")


def _eval_work_ids(assignment_payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    assignments = assignment_payload.get("assignments")
    if not isinstance(assignments, list):
        return out
    for item in assignments:
        if not isinstance(item, Mapping):
            continue
        if item.get("assignment") != "eval":
            continue
        work_id = str(item.get("canonical_openalex_work_id") or "").strip()
        if work_id:
            out.add(work_id)
    return out


def _rank_percentiles(rows: Sequence[Mapping[str, Any]], score_field: str) -> dict[str, float]:
    n = len(rows)
    if n == 0:
        return {}
    values: list[tuple[str, float]] = []
    for row in rows:
        work_id = str(row["canonical_openalex_work_id"])
        value = _float_or_none(row.get(score_field))
        if value is None:
            raise MLHybridScorerOfflineExperimentError(f"candidate row {work_id} missing numeric {score_field}")
        values.append((work_id, value))
    if n == 1:
        return {values[0][0]: 1.0}
    ordered = sorted(values, key=lambda item: (-item[1], item[0]))
    out: dict[str, float] = {}
    index = 0
    while index < n:
        end = index + 1
        value = ordered[index][1]
        while end < n and ordered[end][1] == value:
            end += 1
        first_rank = index + 1.0
        last_rank = float(end)
        average_rank = (first_rank + last_rank) / 2.0
        rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0))
        for pos in range(index, end):
            out[ordered[pos][0]] = rank_pct
        index = end
    return out


def _candidate_work_table(
    *,
    scoring_payload: Mapping[str, Any],
    assignment_payload: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    labeled_rows = scoring_payload.get("labeled_eval_subset")
    if not isinstance(labeled_rows, list):
        raise MLHybridScorerOfflineExperimentError("scoring labeled_eval_subset[] must exist")
    labeled_by_work = {
        str(row["canonical_openalex_work_id"]): row
        for row in labeled_rows
        if isinstance(row, Mapping) and str(row.get("canonical_openalex_work_id") or "").strip()
    }
    candidate_rows_raw = scoring_payload.get("candidate_pool_rows")
    if not isinstance(candidate_rows_raw, list):
        raise MLHybridScorerOfflineExperimentError("scoring candidate_pool_rows[] must exist")

    missing_learned: list[str] = []
    candidates: list[dict[str, Any]] = []
    for raw in candidate_rows_raw:
        if not isinstance(raw, Mapping):
            continue
        work_id = str(raw["canonical_openalex_work_id"])
        if raw.get("heuristic_rank") is None:
            raise MLHybridScorerOfflineExperimentError(f"candidate row {work_id} missing heuristic_rank")
        joined = labeled_by_work.get(work_id)
        learned = _float_or_none(joined.get("audit_embedding_probability_work")) if isinstance(joined, Mapping) else None
        if learned is None:
            missing_learned.append(work_id)
        candidates.append(
            {
                "canonical_openalex_work_id": work_id,
                "title": raw.get("title"),
                "year": raw.get("year"),
                "final_score": _float_or_none(raw.get("final_score")),
                "heuristic_rank": raw.get("heuristic_rank"),
                "audit_embedding_probability_work": learned,
                "label_any_positive": joined.get("label_any_positive") if isinstance(joined, Mapping) else None,
                "observation_count": joined.get("observation_count") if isinstance(joined, Mapping) else None,
                "positive_observation_count": joined.get("positive_observation_count") if isinstance(joined, Mapping) else None,
                "negative_observation_count": joined.get("negative_observation_count") if isinstance(joined, Mapping) else None,
                "conflicting_target_observations": joined.get("conflicting_target_observations") if isinstance(joined, Mapping) else None,
                "row_ids": list(joined.get("row_ids") or []) if isinstance(joined, Mapping) else [],
            }
        )
    if missing_learned:
        raise MLHybridScorerOfflineExperimentError(
            "audit_embedding_probability_work is missing for candidate pool works; "
            f"count={len(missing_learned)}, preview={missing_learned[:10]}"
        )

    final_rank_pct = _rank_percentiles(candidates, "final_score")
    learned_rank_pct = _rank_percentiles(candidates, "audit_embedding_probability_work")
    eval_ids = _eval_work_ids(assignment_payload)
    for row in candidates:
        work_id = row["canonical_openalex_work_id"]
        row["final_score_rank_pct"] = final_rank_pct[work_id]
        row["audit_embedding_probability_rank_pct"] = learned_rank_pct[work_id]
        row["arm_scores"] = _arm_scores(row)

    metric_rows = [
        row
        for row in candidates
        if row["canonical_openalex_work_id"] in eval_ids and isinstance(row.get("label_any_positive"), bool)
    ]
    return candidates, metric_rows, eval_ids


def _arm_scores(row: Mapping[str, Any]) -> dict[str, float]:
    final_score = float(row["final_score"])
    learned = float(row["audit_embedding_probability_work"])
    final_rank = float(row["final_score_rank_pct"])
    learned_rank = float(row["audit_embedding_probability_rank_pct"])
    return {
        "heuristic_final_score_baseline": final_score,
        "holdout_embedding_probability_baseline": learned,
        "hybrid_rank_mean_50_50": 0.5 * final_rank + 0.5 * learned_rank,
        "hybrid_rank_mean_75_25_heuristic": 0.75 * final_rank + 0.25 * learned_rank,
        "hybrid_rank_mean_25_75_heuristic": 0.25 * final_rank + 0.75 * learned_rank,
    }


def _roc_auc_mann_whitney(scores_labels: Sequence[tuple[float, bool]]) -> float | None:
    n = len(scores_labels)
    pos_n = sum(1 for _score, label in scores_labels if label)
    neg_n = n - pos_n
    if pos_n == 0 or neg_n == 0:
        return None
    order = sorted(range(n), key=lambda i: (scores_labels[i][0], scores_labels[i][1], i))
    ranks = [0.0] * n
    rank_start = 1
    index = 0
    while index < n:
        end = index
        value = scores_labels[order[index]][0]
        while end < n and scores_labels[order[end]][0] == value:
            end += 1
        mid = (rank_start + rank_start + (end - index) - 1) / 2.0
        for pos in range(index, end):
            ranks[order[pos]] = mid
        rank_start += end - index
        index = end
    rank_sum = sum(ranks[i] for i in range(n) if scores_labels[i][1])
    return (rank_sum - pos_n * (pos_n + 1) / 2.0) / (pos_n * neg_n)


def _average_precision(scores_labels_desc: Sequence[tuple[float, bool]]) -> float | None:
    positives = sum(1 for _score, label in scores_labels_desc if label)
    if positives == 0:
        return None
    running_pos = 0
    precision_sum = 0.0
    for idx, (_score, label) in enumerate(scores_labels_desc, start=1):
        if label:
            running_pos += 1
            precision_sum += running_pos / idx
    return precision_sum / positives


def _precision_recall_at_k(scores_labels_desc: Sequence[tuple[float, bool]], k: int) -> dict[str, Any]:
    total = len(scores_labels_desc)
    positives = sum(1 for _score, label in scores_labels_desc if label)
    if total < k:
        return {
            "precision": None,
            "recall": None,
            "reason": f"requires at least {k} labeled candidate works",
            "labeled_work_count": total,
            "positive_count": positives,
            "negative_count": total - positives,
        }
    top = list(scores_labels_desc[:k])
    top_pos = sum(1 for _score, label in top if label)
    return {
        "precision": top_pos / k,
        "recall": (top_pos / positives) if positives else None,
        "reason": None if positives else "recall requires at least one positive labeled candidate work",
        "labeled_work_count": total,
        "positive_count": positives,
        "negative_count": total - positives,
        "top_k_labeled_positive_count": top_pos,
        "top_k_labeled_negative_count": k - top_pos,
    }


def _percentile(sorted_values: Sequence[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _score_distribution(candidates: Sequence[Mapping[str, Any]], arm_id: str) -> dict[str, Any]:
    values = sorted(float(row["arm_scores"][arm_id]) for row in candidates)
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "min": values[0],
        "p25": _percentile(values, 0.25),
        "median": _percentile(values, 0.50),
        "p75": _percentile(values, 0.75),
        "max": values[-1],
        "mean": sum(values) / len(values),
    }


def _arm_metric(
    *,
    arm_id: str,
    candidates: Sequence[Mapping[str, Any]],
    metric_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    scores_labels = [(float(row["arm_scores"][arm_id]), bool(row["label_any_positive"])) for row in metric_rows]
    scores_labels_desc = sorted(scores_labels, key=lambda item: (-item[0], item[1]))
    positives = sum(1 for _score, label in scores_labels if label)
    negatives = len(scores_labels) - positives
    auc = _roc_auc_mann_whitney(scores_labels)
    ap = _average_precision(scores_labels_desc)
    sorted_rows = sorted(metric_rows, key=lambda row: (-float(row["arm_scores"][arm_id]), bool(row["label_any_positive"])))
    return {
        "arm_id": arm_id,
        "metric_level": "canonical_work_labeled_eval_subset",
        "labeled_eval_subset_work_count": len(metric_rows),
        "scored_labeled_work_count": len(scores_labels),
        "positive_work_count": positives,
        "negative_work_count": negatives,
        "roc_auc_mann_whitney": auc,
        "roc_auc_reason": None if positives and negatives else "ROC-AUC requires at least one positive and one negative labeled candidate work",
        "average_precision": ap,
        "average_precision_reason": None if ap is not None else "average precision requires at least one positive labeled candidate work",
        "precision_recall_at_k": {str(k): _precision_recall_at_k(scores_labels_desc, k) for k in K_VALUES},
        "score_distribution": _score_distribution(candidates, arm_id),
        "top_k_previews": {str(k): _top_k_preview(sorted_rows, arm_id, k) for k in K_VALUES},
    }


def _top_k_preview(rows: Sequence[Mapping[str, Any]], arm_id: str, k: int) -> list[dict[str, Any]]:
    return [
        {
            "canonical_openalex_work_id": row["canonical_openalex_work_id"],
            "title": row.get("title"),
            "score": row["arm_scores"][arm_id],
            "label_any_positive": row.get("label_any_positive"),
        }
        for row in rows[:k]
    ]


def _pr_value(metrics: Mapping[str, Any], k: int) -> Any:
    entry = _get(metrics, f"precision_recall_at_k.{k}.precision")
    return entry


def _metric_delta(value: Any, baseline: Any) -> float | None:
    left = _float_or_none(value)
    right = _float_or_none(baseline)
    if left is None or right is None:
        return None
    return left - right


def _comparison(metrics: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict[str, Any]:
    delta_roc = _metric_delta(metrics.get("roc_auc_mann_whitney"), baseline.get("roc_auc_mann_whitney"))
    delta_ap = _metric_delta(metrics.get("average_precision"), baseline.get("average_precision"))
    delta_p5 = _metric_delta(_pr_value(metrics, 5), _pr_value(baseline, 5))
    delta_p10 = _metric_delta(_pr_value(metrics, 10), _pr_value(baseline, 10))
    delta_p20 = _metric_delta(_pr_value(metrics, 20), _pr_value(baseline, 20))
    material = (
        (_is_number(delta_roc) and float(delta_roc) >= MATERIAL_LIFT_ROC_AUC)
        or (_is_number(delta_ap) and float(delta_ap) >= MATERIAL_LIFT_AVERAGE_PRECISION)
    )
    return {
        "delta_roc_auc": delta_roc,
        "delta_average_precision": delta_ap,
        "delta_precision_at_5": delta_p5,
        "delta_precision_at_10": delta_p10,
        "delta_precision_at_20": delta_p20,
        "material_lift_passed_against_heuristic": material,
        "precision_at_10_non_regression_advisory": "regression" if _is_number(delta_p10) and delta_p10 < 0 else "ok_or_not_applicable",
    }


def _assert_learned_baseline_matches_scoring(metrics: Mapping[str, Any], scoring_payload: Mapping[str, Any]) -> None:
    expected = _get(scoring_payload, "learned_or_embedding_metrics.metrics")
    if not isinstance(expected, Mapping):
        raise MLHybridScorerOfflineExperimentError("scoring learned metrics missing for sanity check")
    checks = [
        ("roc_auc_mann_whitney", metrics.get("roc_auc_mann_whitney"), expected.get("roc_auc_mann_whitney")),
        ("average_precision", metrics.get("average_precision"), expected.get("average_precision")),
    ]
    for k in K_VALUES:
        checks.append((f"precision_at_{k}", _pr_value(metrics, k), _pr_value(expected, k)))
    for name, actual, exp in checks:
        if actual is None and exp is None:
            continue
        if not (_is_number(actual) and _is_number(exp) and abs(float(actual) - float(exp)) <= 1e-9):
            raise MLHybridScorerOfflineExperimentError(
                f"holdout embedding baseline metric {name} does not match scoring v3 learned metrics"
            )


def _candidate_eval_coverage(
    candidates: Sequence[Mapping[str, Any]],
    metric_rows: Sequence[Mapping[str, Any]],
    eval_ids: set[str],
    scoring_payload: Mapping[str, Any],
) -> dict[str, Any]:
    positives = sum(1 for row in metric_rows if row.get("label_any_positive") is True)
    negatives = sum(1 for row in metric_rows if row.get("label_any_positive") is False)
    return {
        "candidate_pool_work_count": len(candidates),
        "assignment_eval_work_count": len(eval_ids),
        "labeled_eval_metric_work_count": len(metric_rows),
        "labeled_eval_positive_work_count": positives,
        "labeled_eval_negative_work_count": negatives,
        "positive_work_prevalence": (positives / len(metric_rows)) if metric_rows else None,
        "scoring_candidate_unique_canonical_work_count": _get(
            scoring_payload, "candidate_pool_summary.candidate_unique_canonical_work_count"
        ),
    }


def _candidate_work_scores(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "canonical_openalex_work_id": row["canonical_openalex_work_id"],
            "title": row.get("title"),
            "year": row.get("year"),
            "heuristic_rank": row.get("heuristic_rank"),
            "final_score": row.get("final_score"),
            "audit_embedding_probability_work": row.get("audit_embedding_probability_work"),
            "final_score_rank_pct": row.get("final_score_rank_pct"),
            "audit_embedding_probability_rank_pct": row.get("audit_embedding_probability_rank_pct"),
            "label_any_positive": row.get("label_any_positive"),
            "arm_scores": dict(row["arm_scores"]),
        }
        for row in candidates
    ]


def build_ml_hybrid_scorer_offline_experiment_payload(
    *,
    production_candidate_scoring_path: Path,
    production_candidate_metric_gates_path: Path,
    experiment_spec_path: Path,
    holdout_assignment_path: Path,
    holdout_policy_path: Path | None = None,
    experiment_version: str = EXPERIMENT_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    scoring_path = Path(production_candidate_scoring_path).resolve()
    gates_path = Path(production_candidate_metric_gates_path).resolve()
    spec_path = Path(experiment_spec_path).resolve()
    assignment_path = Path(holdout_assignment_path).resolve()
    policy_path = Path(holdout_policy_path).resolve() if holdout_policy_path else None

    scoring_payload = _load_json_object(scoring_path)
    gates_payload = _load_json_object(gates_path)
    spec_payload = _load_json_object(spec_path)
    assignment_payload = _load_json_object(assignment_path)

    scoring_metadata = _validate_scoring(scoring_payload)
    gates_metadata = _validate_gates(gates_payload)
    spec_metadata = _validate_spec(spec_payload)
    eval_sha = str(scoring_metadata.get("eval_work_set_sha256"))
    if spec_metadata.get("eval_work_set_sha256") != eval_sha:
        raise MLHybridScorerOfflineExperimentError("spec metadata.eval_work_set_sha256 must match scoring")
    assignment_metadata = _validate_assignment(assignment_payload, eval_sha=eval_sha)

    inputs = [
        _input_record("production_candidate_scoring", scoring_path, repo_root=root),
        _input_record("production_candidate_metric_gates", gates_path, repo_root=root),
        _input_record("experiment_spec", spec_path, repo_root=root),
        _input_record("holdout_assignment", assignment_path, repo_root=root),
    ]
    holdout_policy_version = None
    if policy_path is not None:
        policy_payload = _load_json_object(policy_path)
        holdout_policy_metadata = _validate_holdout_policy(policy_payload)
        holdout_policy_version = holdout_policy_metadata.get("policy_version")
        inputs.append(_input_record("holdout_policy", policy_path, repo_root=root))

    candidates, metric_rows, eval_ids = _candidate_work_table(
        scoring_payload=scoring_payload,
        assignment_payload=assignment_payload,
    )
    arm_metrics = {
        arm_id: _arm_metric(arm_id=arm_id, candidates=candidates, metric_rows=metric_rows)
        for arm_id, _formula in EXPECTED_ARMS
    }
    _assert_learned_baseline_matches_scoring(
        arm_metrics["holdout_embedding_probability_baseline"],
        scoring_payload,
    )
    heuristic_baseline = arm_metrics["heuristic_final_score_baseline"]
    comparisons = {
        arm_id: _comparison(metrics, heuristic_baseline)
        for arm_id, metrics in arm_metrics.items()
        if arm_id != "heuristic_final_score_baseline"
    }
    best_by_roc = max(
        arm_metrics.values(),
        key=lambda metrics: (
            float(metrics["roc_auc_mann_whitney"]) if _is_number(metrics.get("roc_auc_mann_whitney")) else float("-inf"),
            str(metrics["arm_id"]),
        ),
    )
    best_by_ap = max(
        arm_metrics.values(),
        key=lambda metrics: (
            float(metrics["average_precision"]) if _is_number(metrics.get("average_precision")) else float("-inf"),
            str(metrics["arm_id"]),
        ),
    )
    hybrid_material_lift = any(
        comparison["material_lift_passed_against_heuristic"]
        for arm_id, comparison in comparisons.items()
        if arm_id in HYBRID_ARM_IDS
    )
    recommended = "create_hybrid_scorer_metric_gates_v1" if hybrid_material_lift else "collect_labels_or_features_or_new_eval_surface"
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "experiment_version": experiment_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_scoring_version": scoring_metadata.get("experiment_version"),
        "source_metric_gates_version": gates_metadata.get("gates_version"),
        "experiment_spec_version": spec_metadata.get("spec_version"),
        "holdout_assignment_version": assignment_metadata.get("assignment_version"),
        "holdout_policy_version": holdout_policy_version,
        "eval_work_set_sha256": eval_sha,
        "ranking_run_id": scoring_metadata.get("ranking_run_id"),
        "family": scoring_metadata.get("family"),
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "candidate_eval_coverage": _candidate_eval_coverage(candidates, metric_rows, eval_ids, scoring_payload),
        "rank_percentile_policy": {
            "scope": "full candidate pool",
            "higher_raw_score_is_better": True,
            "tie_policy": "descending average rank",
            "if_n_equals_1_rank_pct": 1.0,
        },
        "pre_registered_arms_executed": [
            {"arm_id": arm_id, "score_formula": formula}
            for arm_id, formula in EXPECTED_ARMS
        ],
        "candidate_work_scores": _candidate_work_scores(candidates),
        "arm_metrics": arm_metrics,
        "comparisons_vs_heuristic": comparisons,
        "summary": {
            "heuristic_baseline_metrics": heuristic_baseline,
            "holdout_embedding_baseline_metrics": arm_metrics["holdout_embedding_probability_baseline"],
            "best_arm_by_roc_auc": {
                "arm_id": best_by_roc["arm_id"],
                "roc_auc_mann_whitney": best_by_roc["roc_auc_mann_whitney"],
            },
            "best_arm_by_average_precision": {
                "arm_id": best_by_ap["arm_id"],
                "average_precision": best_by_ap["average_precision"],
            },
            "best_arm_selection_is_exploratory_only": True,
            "hybrid_material_lift_passed": hybrid_material_lift,
            "recommended_next_stage": recommended,
            "interpretation_note": "rank fusion on already-observed eval surface; exploratory best-arm selection",
        },
        "leakage_report": {
            "train_rows_used_in_metrics": 0,
            "train_works_used_in_metrics": 0,
            "eval_work_set_sha256_matches_inputs": True,
            "supervised_fit_used": False,
            "eval_label_weight_tuning_used": False,
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "missing_hybrid_metric_gates": True,
            "missing_ml_shadow_scorer_v1": True,
            "no_production_model_artifact": True,
        },
        "interpretation": {
            "summary": "Pre-registered hybrid arms were executed without fitting or eval-label tuning.",
            "not_claimed": [
                "live recommender validation",
                "shadow readiness",
                "production readiness",
                "production model artifact",
            ],
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_hybrid_scorer_offline_experiment(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    coverage = payload["candidate_eval_coverage"]
    summary = payload["summary"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# Hybrid Scorer Offline Experiment ({metadata['experiment_version']})",
        "",
        "## Executive Summary",
        "",
        "This executes the pre-registered hybrid scorer offline experiment on scoring v3 JSON only. It uses fixed label-blind arms, no fitting, no DB, and no ranking writes.",
        "",
        f"- **Hybrid material lift passed:** {summary['hybrid_material_lift_passed']}",
        f"- **Recommended next stage:** `{summary['recommended_next_stage']}`",
        f"- **Best ROC-AUC arm:** `{summary['best_arm_by_roc_auc']['arm_id']}`",
        f"- **Best AP arm:** `{summary['best_arm_by_average_precision']['arm_id']}`",
        f"- **Shadow scoring allowed:** {blockers['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {blockers['production_default_allowed']}",
        "",
        "## Candidate/Eval Coverage",
        "",
        "| Measure | Value |",
        "| --- | ---: |",
        f"| Candidate pool works | {coverage['candidate_pool_work_count']} |",
        f"| Assignment eval works | {coverage['assignment_eval_work_count']} |",
        f"| Labeled eval metric works | {coverage['labeled_eval_metric_work_count']} |",
        f"| Positive eval works | {coverage['labeled_eval_positive_work_count']} |",
        f"| Negative eval works | {coverage['labeled_eval_negative_work_count']} |",
        f"| Positive work prevalence | {_fmt(coverage['positive_work_prevalence'])} |",
        "",
        "## Arm Metrics",
        "",
        "| Arm | ROC-AUC | AP | P@5 | P@10 | P@20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for arm_id, _formula in EXPECTED_ARMS:
        metrics = payload["arm_metrics"][arm_id]
        lines.append(
            f"| `{arm_id}` | {_fmt(metrics['roc_auc_mann_whitney'])} | {_fmt(metrics['average_precision'])} | "
            f"{_fmt(_pr_value(metrics, 5))} | {_fmt(_pr_value(metrics, 10))} | {_fmt(_pr_value(metrics, 20))} |"
        )
    lines.extend(["", "## Deltas Vs Heuristic", "", "| Arm | delta ROC-AUC | delta AP | delta P@5 | delta P@10 | delta P@20 | Material lift |", "| --- | ---: | ---: | ---: | ---: | ---: | --- |"])
    for arm_id, comparison in payload["comparisons_vs_heuristic"].items():
        lines.append(
            f"| `{arm_id}` | {_fmt(comparison['delta_roc_auc'])} | {_fmt(comparison['delta_average_precision'])} | "
            f"{_fmt(comparison['delta_precision_at_5'])} | {_fmt(comparison['delta_precision_at_10'])} | "
            f"{_fmt(comparison['delta_precision_at_20'])} | {comparison['material_lift_passed_against_heuristic']} |"
        )
    lines.extend(
        [
            "",
            "## Best-Arm Exploratory Warning",
            "",
            f"Best-arm selection is exploratory only: {summary['best_arm_selection_is_exploratory_only']}. This experiment evaluates an already-seen v3 eval surface.",
            "",
            "## Material Lift Assessment",
            "",
            f"- Hybrid material lift passed: {summary['hybrid_material_lift_passed']}",
            "- Material lift requires a hybrid arm to beat heuristic by ROC-AUC >= 0.03 or AP >= 0.02.",
            "",
            "## Prevalence/P@k Caveat",
            "",
            "The eval set is positive-heavy, so P@k is advisory when arms are saturated.",
            "",
            "## Leakage Checks",
            "",
            f"- Train rows used in metrics: {payload['leakage_report']['train_rows_used_in_metrics']}",
            f"- Train works used in metrics: {payload['leakage_report']['train_works_used_in_metrics']}",
            f"- Supervised fit used: {payload['leakage_report']['supervised_fit_used']}",
            f"- Eval-label weight tuning used: {payload['leakage_report']['eval_label_weight_tuning_used']}",
            "",
            "## Recommended Next Stage",
            "",
            f"`{summary['recommended_next_stage']}`",
            "",
            "## Not Shadow / Not Production",
            "",
            f"- Shadow scoring allowed: {blockers['shadow_scoring_allowed']}",
            f"- Production default allowed: {blockers['production_default_allowed']}",
            f"- Missing hybrid metric gates: {blockers['missing_hybrid_metric_gates']}",
            f"- Missing `ml-shadow-scorer-v1`: {blockers['missing_ml_shadow_scorer_v1']}",
            f"- No production model artifact: {blockers['no_production_model_artifact']}",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_hybrid_scorer_offline_experiment(
    *,
    production_candidate_scoring_path: Path,
    production_candidate_metric_gates_path: Path,
    experiment_spec_path: Path,
    holdout_assignment_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    holdout_policy_path: Path | None = None,
    experiment_version: str = EXPERIMENT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_hybrid_scorer_offline_experiment_payload(
        production_candidate_scoring_path=production_candidate_scoring_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        experiment_spec_path=experiment_spec_path,
        holdout_assignment_path=holdout_assignment_path,
        holdout_policy_path=holdout_policy_path,
        experiment_version=experiment_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_hybrid_scorer_offline_experiment(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "EXPERIMENT_VERSION",
    "MLHybridScorerOfflineExperimentError",
    "build_ml_hybrid_scorer_offline_experiment_payload",
    "markdown_from_ml_hybrid_scorer_offline_experiment",
    "write_ml_hybrid_scorer_offline_experiment",
]
