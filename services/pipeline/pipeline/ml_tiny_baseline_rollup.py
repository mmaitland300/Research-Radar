"""Offline rollup: fold-wise robustness + fixed ablations vs heuristic (emerging only, read-only DB)."""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg

from pipeline.ml_offline_baseline_eval import _parse_config_json, load_label_dataset, sha256_file
from pipeline.ml_tiny_baseline import (
    EMERGING_FAMILY,
    MLTinyBaselineError,
    _column_train_stats,
    _dot,
    _float_or_none,
    _logistic_fit_gd,
    _metrics_for_scores,
    _row_feature_vector,
    collect_joined_emerging_rows_dual_bool_targets,
    fetch_ranking_run_row,
    prepare_stratified_cv_fold_tests,
)
from pipeline.recommendation_review_worksheet import cluster_version_from_config

ROLLUP_CAVEATS = (
    "This is an offline robustness diagnostic, not validation.",
    "Labels are single-reviewer audit labels with ranking-selection bias.",
    "Results must not change production ranking defaults.",
    "No train/dev/test split is created by this artifact.",
    "Flat or unchanged P@k means the learned model has not shown improved recommendation-head quality.",
)

FLOAT_TIE_EPS = 1e-12

# (spec_id, mode, feature_names or None for heuristic)
ABLATION_SPECS: tuple[tuple[str, str, tuple[str, ...] | None], ...] = (
    ("heuristic_final_score", "heuristic", None),
    ("learned_final_score_only", "learned", ("final_score",)),
    ("learned_semantic_only", "learned", ("semantic_score",)),
    (
        "learned_topic_citation_only",
        "learned",
        ("citation_velocity_score", "topic_growth_score", "diversity_penalty"),
    ),
    (
        "learned_without_final_score",
        "learned",
        ("semantic_score", "citation_velocity_score", "topic_growth_score", "diversity_penalty"),
    ),
    (
        "learned_full",
        "learned",
        ("final_score", "semantic_score", "citation_velocity_score", "topic_growth_score", "diversity_penalty"),
    ),
)

TARGET_ORDER = ("good_or_acceptable", "surprising_or_useful")


class MLTinyBaselineRollupError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def fold_tests_fingerprint(row_ids: Sequence[str], y: Sequence[int], fold_tests: list[list[int]]) -> str:
    payload = {"row_ids": list(row_ids), "y": list(y), "fold_tests": fold_tests}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _quartiles(values: list[float]) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    s = sorted(values)
    n = len(s)

    def _q(p: float) -> float:
        if n == 1:
            return float(s[0])
        idx = p * (n - 1)
        lo = int(math.floor(idx))
        hi = int(math.ceil(idx))
        if lo == hi:
            return float(s[lo])
        return float(s[lo] + (s[hi] - s[lo]) * (idx - lo))

    return _q(0.25), _q(0.5), _q(0.75)


def _fold_summary_metrics(per_fold_rows: list[dict[str, Any]]) -> dict[str, Any]:
    aucs = [float(f["roc_auc_mann_whitney"]) for f in per_fold_rows if f.get("roc_auc_mann_whitney") is not None]
    q1, med, q3 = _quartiles(aucs)
    iqr = (q3 - q1) if q1 is not None and q3 is not None else None
    return {
        "per_fold_roc_auc_mann_whitney": aucs,
        "mean_per_fold_auc": statistics.fmean(aucs) if aucs else None,
        "median_per_fold_auc": med,
        "iqr_per_fold_auc": iqr,
        "min_per_fold_auc": min(aucs) if aucs else None,
        "max_per_fold_auc": max(aucs) if aucs else None,
    }


def evaluate_spec_on_folds(
    rows: list[dict[str, Any]],
    y: list[int],
    fold_tests: list[list[int]],
    *,
    target: str,
    spec_id: str,
    mode: str,
    feature_names: tuple[str, ...] | None,
) -> dict[str, Any]:
    """One spec: OOF scores + per-fold metrics (heuristic uses final_score on test rows)."""
    oof: list[tuple[float, bool]] = []
    per_fold: list[dict[str, Any]] = []

    for fold_id, test_idx in enumerate(fold_tests):
        train_idx = [i for i in range(len(rows)) if i not in set(test_idx)]
        if not train_idx or not test_idx:
            raise MLTinyBaselineRollupError(f"fold {fold_id} has empty train or test")
        y_tr = [y[i] for i in train_idx]
        if sum(y_tr) == 0 or sum(y_tr) == len(y_tr):
            raise MLTinyBaselineRollupError(
                f"fold {fold_id} training set is single-class after stratified assignment.",
            )
        n_test = len(test_idx)

        if mode == "heuristic":
            fold_scores: list[tuple[float, bool]] = []
            for i in test_idx:
                hf = _float_or_none(rows[i].get("final_score"))
                if hf is None:
                    hf = 0.0
                fold_scores.append((hf, bool(rows[i][target])))
                oof.append((hf, bool(rows[i][target])))
            m = _metrics_for_scores(fold_scores)
            per_fold.append(
                {
                    "fold_id": fold_id,
                    "n_train": len(train_idx),
                    "n_test": n_test,
                    "test_positive_count": sum(1 for i in test_idx if y[i] == 1),
                    "test_negative_count": sum(1 for i in test_idx if y[i] == 0),
                    "roc_auc_mann_whitney": m["roc_auc_mann_whitney"],
                    "pairwise_accuracy": m["pairwise_accuracy"],
                    "precision_at_5": m["precision_at_5"],
                    "precision_at_10": m["precision_at_10"],
                    "precision_at_20": m["precision_at_20"],
                }
            )
            continue

        assert feature_names is not None and mode == "learned"
        medians: list[float] = []
        means: list[float] = []
        stds: list[float] = []
        for name in feature_names:
            med, mu, sig = _column_train_stats(rows, train_idx, name)
            medians.append(med)
            means.append(mu)
            stds.append(sig)

        X_tr = [_row_feature_vector(rows[i], feature_names, medians, means, stds) for i in train_idx]
        y_tr_bin = [y[i] for i in train_idx]
        w = _logistic_fit_gd(X_tr, y_tr_bin)

        fold_scores = []
        for i in test_idx:
            xi = _row_feature_vector(rows[i], feature_names, medians, means, stds)
            ls = _dot(w, xi)
            lab = bool(rows[i][target])
            fold_scores.append((ls, lab))
            oof.append((ls, lab))
        m = _metrics_for_scores(fold_scores)
        per_fold.append(
            {
                "fold_id": fold_id,
                "n_train": len(train_idx),
                "n_test": n_test,
                "test_positive_count": sum(1 for i in test_idx if y[i] == 1),
                "test_negative_count": sum(1 for i in test_idx if y[i] == 0),
                "roc_auc_mann_whitney": m["roc_auc_mann_whitney"],
                "pairwise_accuracy": m["pairwise_accuracy"],
                "precision_at_5": m["precision_at_5"],
                "precision_at_10": m["precision_at_10"],
                "precision_at_20": m["precision_at_20"],
            }
        )

    agg = _metrics_for_scores(oof)
    fold_stats = _fold_summary_metrics(per_fold)
    out: dict[str, Any] = {
        "spec_id": spec_id,
        "mode": mode,
        "feature_names": list(feature_names) if feature_names else [],
        "aggregate_out_of_fold": agg,
        "per_fold": per_fold,
        **fold_stats,
    }
    return out


def _compare_learned_to_heuristic(
    learned: dict[str, Any],
    heuristic: dict[str, Any],
) -> dict[str, Any]:
    h_agg = heuristic["aggregate_out_of_fold"]
    l_agg = learned["aggregate_out_of_fold"]
    h_auc = h_agg.get("roc_auc_mann_whitney")
    l_auc = l_agg.get("roc_auc_mann_whitney")
    h_p = h_agg.get("pairwise_accuracy")
    l_p = l_agg.get("pairwise_accuracy")
    auc_delta = (l_auc - h_auc) if isinstance(l_auc, (int, float)) and isinstance(h_auc, (int, float)) else None
    p_delta = (l_p - h_p) if isinstance(l_p, (int, float)) and isinstance(h_p, (int, float)) else None

    beat = tie = lost = 0
    gaps: list[float] = []
    for fl, fh in zip(learned["per_fold"], heuristic["per_fold"], strict=True):
        la = fl.get("roc_auc_mann_whitney")
        ha = fh.get("roc_auc_mann_whitney")
        if la is None or ha is None:
            continue
        g = float(la) - float(ha)
        gaps.append(g)
        if g > FLOAT_TIE_EPS:
            beat += 1
        elif g < -FLOAT_TIE_EPS:
            lost += 1
        else:
            tie += 1

    worst_gap = min(gaps) if gaps else None

    p_improved = False
    p_worsened = False
    for k in (5, 10, 20):
        key = f"precision_at_{k}"
        for fl, fh in zip(learned["per_fold"], heuristic["per_fold"], strict=True):
            lpv = fl.get(key)
            hpv = fh.get(key)
            if lpv is None or hpv is None:
                continue
            if float(lpv) > float(hpv) + FLOAT_TIE_EPS:
                p_improved = True
            if float(lpv) < float(hpv) - FLOAT_TIE_EPS:
                p_worsened = True

    return {
        "aggregate_auc_delta": auc_delta,
        "aggregate_pairwise_delta": p_delta,
        "learned_beat_heuristic_fold_count": beat,
        "learned_tied_heuristic_fold_count": tie,
        "learned_lost_to_heuristic_fold_count": lost,
        "worst_fold_auc_gap": worst_gap,
        "p_at_k_improved_any_fold": p_improved,
        "p_at_k_worsened_any_fold": p_worsened,
    }


def _conservative_fields_and_interpretation(
    *,
    target_block: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    specs = target_block["specs"]
    heur = specs["heuristic_final_score"]
    full = specs["learned_full"]
    no_fs = specs["learned_without_final_score"]
    cmp_full = target_block["comparisons_vs_heuristic"]["learned_full"]

    h_oof_p5 = heur["aggregate_out_of_fold"].get("precision_at_5")
    h_oof_p10 = heur["aggregate_out_of_fold"].get("precision_at_10")
    h_oof_p20 = heur["aggregate_out_of_fold"].get("precision_at_20")
    f_oof_p5 = full["aggregate_out_of_fold"].get("precision_at_5")
    f_oof_p10 = full["aggregate_out_of_fold"].get("precision_at_10")
    f_oof_p20 = full["aggregate_out_of_fold"].get("precision_at_20")

    pk_flat = True
    for hf, ff in ((h_oof_p5, f_oof_p5), (h_oof_p10, f_oof_p10), (h_oof_p20, f_oof_p20)):
        if hf is not None and ff is not None and abs(float(ff) - float(hf)) > FLOAT_TIE_EPS:
            pk_flat = False
            break

    auc_delta = cmp_full.get("aggregate_auc_delta")
    unstable = False
    if cmp_full.get("worst_fold_auc_gap") is not None and float(cmp_full["worst_fold_auc_gap"]) < -FLOAT_TIE_EPS:
        unstable = True
    if cmp_full.get("learned_lost_to_heuristic_fold_count", 0) > cmp_full.get("learned_beat_heuristic_fold_count", 0):
        unstable = True

    full_auc = full["aggregate_out_of_fold"].get("roc_auc_mann_whitney")
    no_fs_auc = no_fs["aggregate_out_of_fold"].get("roc_auc_mann_whitney")
    heur_auc = heur["aggregate_out_of_fold"].get("roc_auc_mann_whitney")
    signal_beyond = False
    if (
        isinstance(full_auc, (int, float))
        and isinstance(no_fs_auc, (int, float))
        and isinstance(heur_auc, (int, float))
        and float(no_fs_auc) >= float(heur_auc) - 0.03
        and float(no_fs_auc) >= float(full_auc) - 0.05
    ):
        signal_beyond = True

    supports_more = bool(
        isinstance(auc_delta, (int, float))
        and float(auc_delta) > FLOAT_TIE_EPS
        and not unstable
    ) or (
        cmp_full.get("learned_beat_heuristic_fold_count", 0)
        >= cmp_full.get("learned_lost_to_heuristic_fold_count", 0)
        and isinstance(auc_delta, (int, float))
        and float(auc_delta) > -0.02
    )

    lines: list[str] = []
    if isinstance(auc_delta, (int, float)) and float(auc_delta) > FLOAT_TIE_EPS and pk_flat:
        lines.append(
            "Learned_full improves OOF AUC / pairwise modestly vs heuristic_final_score, but P@k is flat or unchanged; "
            "this supports offline feature-learning investigation, not product ranking change."
        )
    if unstable:
        lines.append("Fold-wise AUC gaps are mixed or worst_fold_auc_gap is negative; treat the OOF gain as unstable.")
    if signal_beyond:
        lines.append(
            "learned_without_final_score is close to learned_full relative to heuristic; there may be signal beyond the existing composite."
        )
    else:
        lines.append(
            "learned_without_final_score is well below learned_full or heuristic; the slice mostly tracks the existing composite/heuristic."
        )
    lines.append("This is not validation and does not justify production superiority.")

    suggested = (
        "Continue offline diagnostics (ablations, fold stability, more labels) before any production experiment."
        if supports_more
        else "Treat offline gains as exploratory only; gather more balanced labels and re-check P@k before further modeling."
    )

    fields = {
        "supports_more_ml_experiments": supports_more,
        "supports_product_ranking_change": False,
        "supports_validation_claim": False,
        "suggested_next_step": suggested,
        "oof_p_at_k_unchanged_vs_heuristic": pk_flat,
    }
    return fields, " ".join(lines)


def build_ml_tiny_baseline_rollup_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
) -> dict[str, Any]:
    rid = ranking_run_id.strip()
    if not rid:
        raise MLTinyBaselineRollupError("ranking_run_id must be non-empty")
    if family.strip().lower() != EMERGING_FAMILY:
        raise MLTinyBaselineRollupError(
            f"ml-tiny-baseline-rollup supports only family={EMERGING_FAMILY!r}, not {family!r}",
        )

    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLTinyBaselineRollupError(f"label dataset not found: {path}")
    label_sha = sha256_file(path)
    raw = load_label_dataset(path)
    label_version = str(raw.get("dataset_version", ""))

    run_row = fetch_ranking_run_row(conn, ranking_run_id=rid)
    cfg = _parse_config_json(run_row.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""

    try:
        joined, join_meta = collect_joined_emerging_rows_dual_bool_targets(
            conn, label_dataset_path=path, ranking_run_id=rid
        )
    except MLTinyBaselineError as e:
        raise MLTinyBaselineRollupError(str(e), code=e.code) from e
    if not joined:
        raise MLTinyBaselineRollupError("no joined emerging rows with both targets boolean for this run")

    targets_out: dict[str, Any] = {}

    for tgt in TARGET_ORDER:
        try:
            y, fold_tests, n_folds, row_ids = prepare_stratified_cv_fold_tests(joined, target=tgt)
        except MLTinyBaselineError as e:
            raise MLTinyBaselineRollupError(str(e), code=e.code) from e
        fp = fold_tests_fingerprint(row_ids, y, fold_tests)

        specs: dict[str, Any] = {}
        for spec_id, mode, fnames in ABLATION_SPECS:
            specs[spec_id] = evaluate_spec_on_folds(
                joined, y, fold_tests, target=tgt, spec_id=spec_id, mode=mode, feature_names=fnames
            )

        comparisons: dict[str, Any] = {}
        heur = specs["heuristic_final_score"]
        for spec_id, mode, _fn in ABLATION_SPECS:
            if mode != "learned":
                continue
            comparisons[spec_id] = _compare_learned_to_heuristic(specs[spec_id], heur)

        pos_n = sum(y)
        neg_n = len(y) - pos_n
        block = {
            "class_counts": {"positive": pos_n, "negative": neg_n, "total": len(joined)},
            "fold_count": n_folds,
            "fold_tests_fingerprint_sha256": fp,
            "specs": specs,
            "comparisons_vs_heuristic": comparisons,
        }
        fields, interp = _conservative_fields_and_interpretation(target_block=block)
        block["conservative_decision_fields"] = fields
        block["interpretation_summary"] = interp
        targets_out[tgt] = block

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_tiny_baseline_rollup",
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
            "targets": list(TARGET_ORDER),
        },
        "caveats": list(ROLLUP_CAVEATS),
        "join_summary": join_meta,
        "cv_policy_note": (
            "Same joined emerging slice (dual boolean targets) and stratified_round_robin_by_row_id fold assignment "
            "as ml-tiny-baseline per target; fingerprint is per-target (includes y)."
        ),
        "targets": targets_out,
    }


def markdown_from_ml_tiny_baseline_rollup(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    lines = [
        "# Tiny baseline robustness rollup (emerging only)",
        "",
        "Offline-only diagnostics: ablations and fold-wise summaries vs `heuristic_final_score`. "
        "No ranking, API, web, or default behavior changes.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **targets:** {', '.join(prov.get('targets') or [])}",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        "## Join summary",
        "",
        f"- **dual-bool emerging rows joined:** `{payload['join_summary'].get('emerging_dual_bool_target_rows_joined')}`",
        "",
    ]
    for tgt, block in payload.get("targets", {}).items():
        lines.extend(
            [
                f"## Target: `{tgt}`",
                "",
                f"- **fold fingerprint:** `{block.get('fold_tests_fingerprint_sha256')}`",
                f"- **class counts:** positive {block['class_counts']['positive']} / negative {block['class_counts']['negative']} (n={block['class_counts']['total']})",
                "",
                "### Conservative decision fields",
                "",
            ]
        )
        cdf = block.get("conservative_decision_fields", {})
        for k in sorted(cdf.keys()):
            lines.append(f"- **`{k}`:** `{cdf[k]}`")
        lines.extend(["", f"- **interpretation_summary:** {block.get('interpretation_summary', '')}", ""])
        lines.append("### OOF ROC AUC by spec")
        lines.append("")
        lines.append("| spec | OOF AUC | mean fold AUC |")
        lines.append("|------|---------|---------------|")
        for sid, spec in block.get("specs", {}).items():
            agg = spec.get("aggregate_out_of_fold", {})
            auc = agg.get("roc_auc_mann_whitney")
            mauc = spec.get("mean_per_fold_auc")
            lines.append(f"| `{sid}` | `{auc}` | `{mauc}` |")
        lines.append("")
        lines.append("### learned_full vs heuristic (comparison)")
        lines.append("")
        cmpb = block.get("comparisons_vs_heuristic", {}).get("learned_full", {})
        for k in sorted(cmpb.keys()):
            lines.append(f"- **`{k}`:** `{cmpb[k]}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_ml_tiny_baseline_rollup(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_tiny_baseline_rollup_payload(
        conn,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        family=family,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_tiny_baseline_rollup(payload), encoding="utf-8")
    return payload


def run_ml_tiny_baseline_rollup_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_tiny_baseline_rollup(
            conn,
            label_dataset_path=label_dataset_path,
            ranking_run_id=ranking_run_id,
            family=family,
            json_path=output_json,
            markdown_path=markdown_output,
        )
