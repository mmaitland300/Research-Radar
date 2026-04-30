"""Offline-only tiny learned baseline on persisted paper_scores features (emerging family, no writes)."""

from __future__ import annotations

import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg

from pipeline.ml_offline_baseline_eval import (
    _build_score_lookups,
    _parse_config_json,
    fetch_paper_scores_with_openalex,
    fetch_ranking_run_row,
    filter_audit_rows_for_run,
    join_label_row_to_score,
    load_label_dataset,
    pairwise_accuracy,
    precision_at_k,
    roc_auc_mann_whitney,
    sha256_file,
)
from pipeline.recommendation_review_worksheet import cluster_version_from_config

EMERGING_FAMILY = "emerging"
ALLOWED_TARGETS = frozenset({"good_or_acceptable", "surprising_or_useful"})
# Persisted emerging features only (no bridge_score).
FEATURE_NAMES = (
    "final_score",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "diversity_penalty",
)

MIN_POS_FOR_TINY = 10
MIN_NEG_FOR_TINY = 10
MAX_FOLDS = 5

TINY_BASELINE_CAVEATS = (
    "This is an offline tiny baseline experiment, not validation.",
    "Labels are single-reviewer audit labels with ranking-selection bias.",
    "Results must not change production ranking defaults.",
    "No train/dev/test split is created by this artifact.",
)


class MLTinyBaselineError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sigmoid(z: float) -> float:
    if z > 35.0:
        return 1.0
    if z < -35.0:
        return 0.0
    return 1.0 / (1.0 + math.exp(-z))


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    return float(statistics.median(vals))


def _fold_count_for_classes(pos_n: int, neg_n: int) -> int:
    """Deterministic stratified k: at most 5, at least 2 when both classes >= 10."""
    if pos_n < MIN_POS_FOR_TINY or neg_n < MIN_NEG_FOR_TINY:
        return 0
    k = min(MAX_FOLDS, pos_n, neg_n)
    return max(2, k)


def stratified_round_robin_fold_test_indices(
    row_ids: Sequence[str],
    y: Sequence[int],
    n_folds: int,
) -> list[list[int]]:
    """Round-robin assignment within each class; indices sorted by row_id for determinism."""
    if n_folds < 2:
        raise MLTinyBaselineError(f"n_folds must be >= 2, got {n_folds}")
    pos = [i for i, yi in enumerate(y) if yi == 1]
    neg = [i for i, yi in enumerate(y) if yi == 0]
    pos.sort(key=lambda i: row_ids[i])
    neg.sort(key=lambda i: row_ids[i])
    fold_tests: list[list[int]] = [[] for _ in range(n_folds)]
    for j, i in enumerate(pos):
        fold_tests[j % n_folds].append(i)
    for j, i in enumerate(neg):
        fold_tests[j % n_folds].append(i)
    return fold_tests


def _column_train_stats(
    rows: list[dict[str, Any]],
    train_idx: list[int],
    name: str,
) -> tuple[float, float, float]:
    """Median (impute), mean, std on train after imputation."""
    raw: list[float] = []
    for i in train_idx:
        v = _float_or_none(rows[i].get(name))
        raw.append(v if v is not None else math.nan)
    finite = [v for v in raw if not math.isnan(v)]
    med = _median(finite) if finite else 0.0
    filled: list[float] = []
    for v in raw:
        filled.append(med if math.isnan(v) else v)
    mu = statistics.fmean(filled) if filled else 0.0
    if len(filled) < 2:
        sig = 0.0
    else:
        sig = statistics.pstdev(filled)
    return med, mu, sig if sig > 1e-12 else 0.0


def _row_feature_vector(
    row: dict[str, Any],
    feature_names: Sequence[str],
    medians: list[float],
    means: list[float],
    stds: list[float],
) -> list[float]:
    out: list[float] = []
    for j, name in enumerate(feature_names):
        v = _float_or_none(row.get(name))
        if v is None:
            v = medians[j]
        sig = stds[j]
        out.append((v - means[j]) / sig if sig > 1e-12 else 0.0)
    out.append(1.0)
    return out


def prepare_stratified_cv_fold_tests(
    rows: list[dict[str, Any]],
    *,
    target: str,
) -> tuple[list[int], list[list[int]], int, list[str]]:
    """Validate class counts and return y, fold test index lists, n_folds, row_ids (deterministic folds)."""
    if target not in ALLOWED_TARGETS:
        raise MLTinyBaselineError(
            f"target must be one of {sorted(ALLOWED_TARGETS)}, not {target!r}",
        )
    y = [1 if bool(r[target]) else 0 for r in rows]
    pos_n = sum(y)
    neg_n = len(y) - pos_n
    if pos_n < MIN_POS_FOR_TINY or neg_n < MIN_NEG_FOR_TINY:
        raise MLTinyBaselineError(
            f"insufficient class balance for tiny baseline: need at least {MIN_POS_FOR_TINY} positive and "
            f"{MIN_NEG_FOR_TINY} negative joined emerging rows for {target}; got {pos_n} positive, {neg_n} negative.",
        )
    row_ids = [str(r.get("row_id", f"idx{i}")) for i, r in enumerate(rows)]
    n_folds = _fold_count_for_classes(pos_n, neg_n)
    fold_tests = stratified_round_robin_fold_test_indices(row_ids, y, n_folds)
    return y, fold_tests, n_folds, row_ids


def compute_oof_learned_logits_learned_full(
    rows: list[dict[str, Any]],
    *,
    target: str,
) -> list[float]:
    """Out-of-fold linear logits (w·x) from the same stratified folds and learned_full feature set as ml-tiny-baseline."""
    y, fold_tests, _, _ = prepare_stratified_cv_fold_tests(rows, target=target)
    n = len(rows)
    oof: list[float] = [float("nan")] * n
    for fold_id, test_idx in enumerate(fold_tests):
        train_idx = [i for i in range(n) if i not in set(test_idx)]
        if not train_idx or not test_idx:
            raise MLTinyBaselineError(f"fold {fold_id} has empty train or test")
        y_tr = [y[i] for i in train_idx]
        if sum(y_tr) == 0 or sum(y_tr) == len(y_tr):
            raise MLTinyBaselineError(
                f"fold {fold_id} training set is single-class after stratified assignment; reduce folds or check data.",
            )
        medians: list[float] = []
        means: list[float] = []
        stds: list[float] = []
        for name in FEATURE_NAMES:
            med, mu, sig = _column_train_stats(rows, train_idx, name)
            medians.append(med)
            means.append(mu)
            stds.append(sig)
        X_tr = [_row_feature_vector(rows[i], FEATURE_NAMES, medians, means, stds) for i in train_idx]
        y_tr_bin = [y[i] for i in train_idx]
        w = _logistic_fit_gd(X_tr, y_tr_bin)
        for i in test_idx:
            xi = _row_feature_vector(rows[i], FEATURE_NAMES, medians, means, stds)
            oof[i] = _dot(w, xi)
    if any(math.isnan(v) for v in oof):
        raise MLTinyBaselineError("OOF learned logits did not cover all rows")
    return oof


def _logistic_fit_gd(
    X: list[list[float]],
    y: list[int],
    *,
    epochs: int = 6000,
    lr: float = 0.15,
    l2: float = 1e-3,
) -> list[float]:
    """Tiny L2 logistic regression; bias column is last entry (not L2-penalized)."""
    n = len(X)
    if n == 0:
        raise MLTinyBaselineError("empty training set for logistic fit")
    d = len(X[0])
    w = [0.0] * d
    for _ in range(epochs):
        grad = [0.0] * d
        for i in range(n):
            z = sum(w[j] * X[i][j] for j in range(d))
            err = _sigmoid(z) - y[i]
            for j in range(d):
                grad[j] += err * X[i][j]
        for j in range(d):
            grad[j] /= n
            if j < d - 1:
                grad[j] += l2 * w[j]
        for j in range(d):
            w[j] -= lr * grad[j]
    return w


def _dot(w: Sequence[float], x: Sequence[float]) -> float:
    return sum(w[i] * x[i] for i in range(len(w)))


def _metrics_for_scores(scores_labels: list[tuple[float, bool]]) -> dict[str, Any]:
    if not scores_labels:
        return {
            "roc_auc_mann_whitney": None,
            "pairwise_accuracy": None,
            "precision_at_5": None,
            "precision_at_10": None,
            "precision_at_20": None,
        }
    sl_desc = sorted(scores_labels, key=lambda t: (-t[0], t[1]))
    return {
        "roc_auc_mann_whitney": roc_auc_mann_whitney(scores_labels),
        "pairwise_accuracy": pairwise_accuracy(scores_labels),
        "precision_at_5": precision_at_k(sl_desc, 5),
        "precision_at_10": precision_at_k(sl_desc, 10),
        "precision_at_20": precision_at_k(sl_desc, 20),
    }


def collect_joined_emerging_rows(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    target: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLTinyBaselineError(f"label dataset not found: {path}")
    raw = load_label_dataset(path)
    rows, dup_skip = filter_audit_rows_for_run(raw, ranking_run_id=ranking_run_id)
    run_row = fetch_ranking_run_row(conn, ranking_run_id=ranking_run_id)
    score_rows = fetch_paper_scores_with_openalex(conn, ranking_run_id=ranking_run_id)
    by_work, by_wtoken = _build_score_lookups(score_rows)

    joined: list[dict[str, Any]] = []
    missing = 0
    for lab in rows:
        if str(lab.get("family", "")) != EMERGING_FAMILY:
            continue
        tv = lab.get(target)
        if not isinstance(tv, bool):
            continue
        sc = join_label_row_to_score(lab, by_work, by_wtoken)
        if sc is None:
            missing += 1
            continue
        merged = dict(lab)
        merged["_joined_score"] = True
        merged["recommendation_family"] = sc["recommendation_family"]
        merged["work_id"] = sc["work_id"]
        for f in FEATURE_NAMES:
            merged[f] = sc.get(f)
        merged["final_score"] = sc.get("final_score")
        merged["_rank"] = sc.get("_rank")
        joined.append(merged)

    meta = {
        "label_rows_run_match": len(rows),
        "duplicate_row_id_skipped": dup_skip,
        "emerging_target_bool_rows_joined": len(joined),
        "missing_score_for_emerging_labeled_rows": missing,
    }
    return joined, meta


def collect_joined_emerging_rows_dual_bool_targets(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Join emerging rows where both good_or_acceptable and surprising_or_useful are bool (same slice for rollup)."""
    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLTinyBaselineError(f"label dataset not found: {path}")
    raw = load_label_dataset(path)
    rows, dup_skip = filter_audit_rows_for_run(raw, ranking_run_id=ranking_run_id)
    _ = fetch_ranking_run_row(conn, ranking_run_id=ranking_run_id)
    score_rows = fetch_paper_scores_with_openalex(conn, ranking_run_id=ranking_run_id)
    by_work, by_wtoken = _build_score_lookups(score_rows)

    joined: list[dict[str, Any]] = []
    missing = 0
    for lab in rows:
        if str(lab.get("family", "")) != EMERGING_FAMILY:
            continue
        if not isinstance(lab.get("good_or_acceptable"), bool) or not isinstance(
            lab.get("surprising_or_useful"), bool
        ):
            continue
        sc = join_label_row_to_score(lab, by_work, by_wtoken)
        if sc is None:
            missing += 1
            continue
        merged = dict(lab)
        merged["_joined_score"] = True
        merged["recommendation_family"] = sc["recommendation_family"]
        merged["work_id"] = sc["work_id"]
        for f in FEATURE_NAMES:
            merged[f] = sc.get(f)
        merged["final_score"] = sc.get("final_score")
        merged["_rank"] = sc.get("_rank")
        joined.append(merged)

    meta = {
        "label_rows_run_match": len(rows),
        "duplicate_row_id_skipped": dup_skip,
        "emerging_dual_bool_target_rows_joined": len(joined),
        "missing_score_for_emerging_labeled_rows": missing,
    }
    return joined, meta


def run_stratified_cv_tiny_baseline(
    rows: list[dict[str, Any]],
    *,
    target: str,
) -> dict[str, Any]:
    y, fold_tests, n_folds, _row_ids = prepare_stratified_cv_fold_tests(rows, target=target)
    pos_n = sum(y)
    neg_n = len(y) - pos_n

    oof_learned: list[tuple[float, bool]] = []
    oof_heuristic: list[tuple[float, bool]] = []
    per_fold: list[dict[str, Any]] = []
    coef_accum = [0.0] * (len(FEATURE_NAMES) + 1)

    for fold_id, test_idx in enumerate(fold_tests):
        train_idx = [i for i in range(len(rows)) if i not in set(test_idx)]
        if not train_idx or not test_idx:
            raise MLTinyBaselineError(f"fold {fold_id} has empty train or test")
        y_tr = [y[i] for i in train_idx]
        if sum(y_tr) == 0 or sum(y_tr) == len(y_tr):
            raise MLTinyBaselineError(
                f"fold {fold_id} training set is single-class after stratified assignment; reduce folds or check data.",
            )

        medians: list[float] = []
        means: list[float] = []
        stds: list[float] = []
        for name in FEATURE_NAMES:
            med, mu, sig = _column_train_stats(rows, train_idx, name)
            medians.append(med)
            means.append(mu)
            stds.append(sig)

        X_tr = [_row_feature_vector(rows[i], FEATURE_NAMES, medians, means, stds) for i in train_idx]
        y_tr_bin = [y[i] for i in train_idx]
        w = _logistic_fit_gd(X_tr, y_tr_bin)

        for j in range(len(w)):
            coef_accum[j] += w[j]

        learned_test: list[tuple[float, bool]] = []
        heuristic_test: list[tuple[float, bool]] = []
        for i in test_idx:
            xi = _row_feature_vector(rows[i], FEATURE_NAMES, medians, means, stds)
            ls = _dot(w, xi)
            hf = _float_or_none(rows[i].get("final_score"))
            if hf is None:
                hf = 0.0
            lab = bool(rows[i][target])
            learned_test.append((ls, lab))
            heuristic_test.append((hf, lab))
            oof_learned.append((ls, lab))
            oof_heuristic.append((hf, lab))

        per_fold.append(
            {
                "fold_id": fold_id,
                "n_train": len(train_idx),
                "n_test": len(test_idx),
                "test_positive_count": sum(1 for i in test_idx if y[i] == 1),
                "test_negative_count": sum(1 for i in test_idx if y[i] == 0),
                "learned": _metrics_for_scores(learned_test),
                "heuristic_final_score": _metrics_for_scores(heuristic_test),
                "coefficients_standardized_space": {
                    "intercept": w[-1],
                    "weights": {FEATURE_NAMES[j]: w[j] for j in range(len(FEATURE_NAMES))},
                },
            }
        )

    n_f = float(n_folds)
    mean_coef = {
        "intercept": coef_accum[-1] / n_f,
        "weights": {FEATURE_NAMES[j]: coef_accum[j] / n_f for j in range(len(FEATURE_NAMES))},
    }

    return {
        "cv_policy": {
            "type": "stratified_round_robin_by_row_id",
            "n_folds": n_folds,
            "min_positive_required": MIN_POS_FOR_TINY,
            "min_negative_required": MIN_NEG_FOR_TINY,
            "feature_names": list(FEATURE_NAMES),
            "model": "l2_logistic_regression_gradient_descent_pure_python",
        },
        "class_counts": {"positive": pos_n, "negative": neg_n, "total": len(rows)},
        "aggregate_out_of_fold": {
            "learned": _metrics_for_scores(oof_learned),
            "heuristic_final_score": _metrics_for_scores(oof_heuristic),
        },
        "mean_coefficients_across_folds_standardized_space": mean_coef,
        "per_fold": per_fold,
    }


def build_ml_tiny_baseline_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    target: str,
) -> dict[str, Any]:
    rid = ranking_run_id.strip()
    if not rid:
        raise MLTinyBaselineError("ranking_run_id must be non-empty")
    fam = family.strip().lower()
    if fam != EMERGING_FAMILY:
        raise MLTinyBaselineError(
            f"ml-tiny-baseline currently supports only family={EMERGING_FAMILY!r}, not {family!r}",
        )
    tgt = target.strip()
    if tgt not in ALLOWED_TARGETS:
        raise MLTinyBaselineError(
            f"target must be one of {sorted(ALLOWED_TARGETS)} for this command; got {target!r}. "
            "Refusing bridge_like_yes_or_partial and other targets.",
        )

    path = label_dataset_path.resolve()
    label_sha = sha256_file(path)
    raw = load_label_dataset(path)
    label_version = str(raw.get("dataset_version", ""))

    run_row = fetch_ranking_run_row(conn, ranking_run_id=rid)
    cfg = _parse_config_json(run_row.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""

    rows, join_meta = collect_joined_emerging_rows(conn, label_dataset_path=path, ranking_run_id=rid, target=tgt)
    cv_block = run_stratified_cv_tiny_baseline(rows, target=tgt)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_tiny_baseline",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": rid,
            "ranking_version": str(run_row.get("ranking_version", "")),
            "corpus_snapshot_version": str(run_row.get("corpus_snapshot_version", "")),
            "embedding_version": str(run_row.get("embedding_version", "")),
            "cluster_version": cluster_version,
            "label_dataset_path": path.as_posix(),
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
            "family": EMERGING_FAMILY,
            "target": tgt,
        },
        "caveats": list(TINY_BASELINE_CAVEATS),
        "join_summary": join_meta,
        "cv_results": cv_block,
        "interpretation_note": (
            "This artifact compares a tiny transparent logistic model on five persisted score features "
            "against the ranking's final_score on the same cross-validation folds. "
            "Stratified CV estimates offline behavior on this fixed slice only; it does not allocate a production held-out split. "
            "It does not claim ML superiority over production ranking and is not validation."
        ),
    }


def markdown_from_ml_tiny_baseline(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    cv = payload["cv_results"]
    agg = cv["aggregate_out_of_fold"]
    lines = [
        "# Offline tiny baseline (emerging only)",
        "",
        "Deterministic stratified cross-validation on a **fixed** manual-label slice joined to `paper_scores`. "
        "No database writes; no production, API, or web behavior change.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **target:** `{prov.get('target')}`",
        f"- **family:** `{prov.get('family')}`",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        "## Class counts",
        "",
        f"- **positive:** `{cv['class_counts']['positive']}`",
        f"- **negative:** `{cv['class_counts']['negative']}`",
        f"- **total joined:** `{cv['class_counts']['total']}`",
        "",
        "## CV policy",
        "",
        f"- **folds:** `{cv['cv_policy']['n_folds']}`",
        f"- **model:** `{cv['cv_policy']['model']}`",
        f"- **features:** {', '.join(cv['cv_policy']['feature_names'])}",
        "",
        "## Out-of-fold aggregate metrics",
        "",
        "### Learned (logistic on standardized features)",
        "",
        f"- **roc_auc_mann_whitney:** `{agg['learned'].get('roc_auc_mann_whitney')}`",
        f"- **pairwise_accuracy:** `{agg['learned'].get('pairwise_accuracy')}`",
        f"- **precision_at_5/10/20:** `{agg['learned'].get('precision_at_5')}` / "
        f"`{agg['learned'].get('precision_at_10')}` / `{agg['learned'].get('precision_at_20')}`",
        "",
        "### Heuristic (final_score only, same rows)",
        "",
        f"- **roc_auc_mann_whitney:** `{agg['heuristic_final_score'].get('roc_auc_mann_whitney')}`",
        f"- **pairwise_accuracy:** `{agg['heuristic_final_score'].get('pairwise_accuracy')}`",
        f"- **precision_at_5/10/20:** `{agg['heuristic_final_score'].get('precision_at_5')}` / "
        f"`{agg['heuristic_final_score'].get('precision_at_10')}` / `{agg['heuristic_final_score'].get('precision_at_20')}`",
        "",
        "## Mean coefficients (standardized feature space, averaged across folds)",
        "",
        f"- **intercept:** `{cv['mean_coefficients_across_folds_standardized_space']['intercept']}`",
        "",
        *(
            f"- **`{name}`:** `{cv['mean_coefficients_across_folds_standardized_space']['weights'][name]}`"
            for name in FEATURE_NAMES
        ),
        "",
        "## Interpretation",
        "",
        payload.get("interpretation_note", ""),
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_ml_tiny_baseline(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    target: str,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_tiny_baseline_payload(
        conn,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        family=family,
        target=target,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_tiny_baseline(payload), encoding="utf-8")
    return payload


def run_ml_tiny_baseline_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    target: str,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_tiny_baseline(
            conn,
            label_dataset_path=label_dataset_path,
            ranking_run_id=ranking_run_id,
            family=family,
            target=target,
            json_path=output_json,
            markdown_path=markdown_output,
        )
