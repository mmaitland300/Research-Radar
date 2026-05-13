"""Source-split tiny baseline: train on rank-shaped emerging labels, test on blind rows.

Read-only DB access. No ranking run, no conflict resolution, no production model
artifact. Blind label rows keep family=None; only the paper_scores lookup uses the
requested score family.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg

from pipeline.ml_offline_baseline_eval import (
    _build_score_lookups,
    _parse_config_json,
    fetch_paper_scores_with_openalex,
    fetch_ranking_run_row,
    join_label_row_to_score,
    join_label_row_to_score_family,
    load_label_dataset,
    precision_at_k,
    roc_auc_mann_whitney,
    sha256_file,
)
from pipeline.ml_tiny_baseline import (
    ALLOWED_TARGETS,
    EMERGING_FAMILY,
    FEATURE_NAMES,
    _column_train_stats,
    _dot,
    _float_or_none,
    _logistic_fit_gd,
    _row_feature_vector,
)
from pipeline.recommendation_review_worksheet import cluster_version_from_config
from pipeline.repo_paths import portable_repo_path

TARGET_ORDER = ("good_or_acceptable", "surprising_or_useful")
TRAIN_REVIEW_POOL_VARIANTS = (
    "full_family_top_k",
    "ml_contrastive_offline_audit",
    "ml_emerging_target_gap_audit:good_or_acceptable",
)
BLIND_TEST_REVIEW_POOL_VARIANT = "ml_blind_snapshot_audit"

SOURCE_SPLIT_CAVEATS = (
    "This is not validation.",
    "Blind labels reduce but do not eliminate selection bias.",
    "This is a source-split offline diagnostic, not a production train/test policy.",
    "Results must not change production ranking defaults.",
    "No production model artifact is produced.",
)

CONFLICT_POLICY_SUMMARY = {
    "observation_level_rows": True,
    "no_silent_merge": True,
    "no_automatic_conflict_resolution": True,
    "blind_rows_test_only": True,
}

P_AT_K_VALUES = (5, 10, 20)


class MLSourceSplitTinyBaselineError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _audit_rows_for_run_observation_level(payload: dict[str, Any], *, ranking_run_id: str) -> list[dict[str, Any]]:
    rows_in = payload.get("rows")
    if not isinstance(rows_in, list):
        raise MLSourceSplitTinyBaselineError("label dataset missing 'rows' array")
    rows: list[dict[str, Any]] = []
    for r in rows_in:
        if not isinstance(r, dict):
            continue
        if str(r.get("split", "")) != "audit_only":
            continue
        if str(r.get("ranking_run_id", "")) != ranking_run_id:
            continue
        rows.append(r)
    return rows


def _variant_counts(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for r in rows:
        key = str(r.get("review_pool_variant") or "(null)")
        out[key] += 1
    return dict(sorted(out.items()))


def _target_counts(rows: Sequence[dict[str, Any]], target: str) -> dict[str, int]:
    pos = neg = null_n = 0
    for r in rows:
        v = r.get(target)
        if v is True:
            pos += 1
        elif v is False:
            neg += 1
        else:
            null_n += 1
    return {"positive": pos, "negative": neg, "null": null_n, "total": len(rows)}


def _row_key(row: dict[str, Any]) -> str:
    rid = str(row.get("row_id") or "").strip()
    if rid:
        return f"row_id:{rid}"
    wid = str(row.get("work_id") or "").strip()
    if wid:
        return f"work_id:{wid}"
    return f"paper_id:{row.get('paper_id') or ''}"


def _missing_feature_row(label: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_id": label.get("row_id"),
        "paper_id": label.get("paper_id"),
        "work_id": label.get("work_id"),
        "family": label.get("family"),
        "review_pool_variant": label.get("review_pool_variant"),
    }


def _merge_label_with_score(label: dict[str, Any], score: dict[str, Any]) -> dict[str, Any]:
    merged = dict(label)
    merged["_joined_score"] = True
    merged["score_family"] = score.get("recommendation_family")
    merged["score_work_id"] = score.get("work_id")
    merged["recommendation_family"] = score.get("recommendation_family")
    merged["_rank"] = score.get("_rank")
    for feature in FEATURE_NAMES:
        merged[feature] = score.get(feature)
    return merged


def _feature_non_null_counts(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    n = len(rows)
    for feature in FEATURE_NAMES:
        present = sum(1 for r in rows if _float_or_none(r.get(feature)) is not None)
        out[feature] = {"non_null": present, "null": n - present}
    return out


def _coverage_block(
    *,
    selected_rows: Sequence[dict[str, Any]],
    joined_rows: Sequence[dict[str, Any]],
    missing_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    selected = len(selected_rows)
    joined = len(joined_rows)
    missing = len(missing_rows)
    return {
        "selected_row_count": selected,
        "joined_feature_row_count": joined,
        "missing_feature_row_count": missing,
        "coverage_rate": (joined / selected) if selected else None,
        "feature_non_null_counts": _feature_non_null_counts(joined_rows),
    }


def _metric_block(scores_labels: list[tuple[float, bool]]) -> dict[str, Any]:
    pos = sum(1 for _score, y in scores_labels if y)
    neg = len(scores_labels) - pos
    auc = roc_auc_mann_whitney(scores_labels)
    if not scores_labels:
        auc_reason = "no labeled scored blind rows"
    elif pos == 0 or neg == 0:
        auc_reason = "ROC AUC requires at least one positive and one negative blind row"
    else:
        auc_reason = None
    desc = sorted(scores_labels, key=lambda t: (-t[0], t[1]))
    p_at_k: dict[str, dict[str, Any]] = {}
    for k in P_AT_K_VALUES:
        val = precision_at_k(desc, k)
        reason = None if val is not None else f"requires at least {k} labeled scored blind rows"
        p_at_k[str(k)] = {"value": val, "reason": reason}
    return {
        "scored_labeled_row_count": len(scores_labels),
        "positive_count": pos,
        "negative_count": neg,
        "roc_auc_mann_whitney": auc,
        "roc_auc_reason": auc_reason,
        "precision_at_k": p_at_k,
    }


def _null_metric_block(reason: str) -> dict[str, Any]:
    return {
        "scored_labeled_row_count": 0,
        "positive_count": 0,
        "negative_count": 0,
        "roc_auc_mann_whitney": None,
        "roc_auc_reason": reason,
        "precision_at_k": {str(k): {"value": None, "reason": reason} for k in P_AT_K_VALUES},
    }


def _fit_train_preprocessor(rows: list[dict[str, Any]], feature_names: Sequence[str]) -> dict[str, Any]:
    idx = list(range(len(rows)))
    medians: list[float] = []
    means: list[float] = []
    stds: list[float] = []
    by_feature: dict[str, dict[str, float]] = {}
    for feature in feature_names:
        med, mu, sig = _column_train_stats(rows, idx, feature)
        medians.append(med)
        means.append(mu)
        stds.append(sig)
        by_feature[feature] = {
            "impute_median": med,
            "mean_after_imputation": mu,
            "std_after_imputation": sig,
        }
    return {
        "fit_on": "train rows only with boolean target and joined paper_scores features",
        "feature_names": list(feature_names),
        "medians": medians,
        "means": means,
        "stds": stds,
        "by_feature": by_feature,
    }


def _vector_from_preprocessor(
    row: dict[str, Any],
    *,
    feature_names: Sequence[str],
    preprocessing: dict[str, Any],
) -> list[float]:
    return _row_feature_vector(
        row,
        feature_names,
        list(preprocessing["medians"]),
        list(preprocessing["means"]),
        list(preprocessing["stds"]),
    )


def _target_result(
    *,
    target: str,
    train_selected: list[dict[str, Any]],
    train_joined: list[dict[str, Any]],
    test_selected: list[dict[str, Any]],
    test_joined: list[dict[str, Any]],
) -> dict[str, Any]:
    train_bool = [r for r in train_joined if isinstance(r.get(target), bool)]
    test_bool = [r for r in test_joined if isinstance(r.get(target), bool)]
    train_bool_without_features = [
        r for r in train_selected if isinstance(r.get(target), bool) and not r.get("_joined_score")
    ]
    test_bool_without_features = [
        r for r in test_selected if isinstance(r.get(target), bool) and not r.get("_joined_score")
    ]
    train_missing_target = [r for r in train_selected if not isinstance(r.get(target), bool)]
    test_missing_target = [r for r in test_selected if not isinstance(r.get(target), bool)]

    heuristic_scores = [
        (_float_or_none(r.get("final_score")), bool(r[target]))
        for r in test_bool
    ]
    heuristic_scores_clean = [(float(s), y) for s, y in heuristic_scores if s is not None]
    missing_heuristic_final_score = sum(1 for s, _y in heuristic_scores if s is None)

    y_train = [1 if r[target] is True else 0 for r in train_bool]
    train_pos = sum(y_train)
    train_neg = len(y_train) - train_pos

    learned_block: dict[str, Any]
    model_block: dict[str, Any]
    preprocessing_block: dict[str, Any] | None = None
    if not train_bool:
        reason = "no train rows with boolean target and joined features"
        learned_block = _null_metric_block(reason)
        model_block = {"trained": False, "not_trained_reason": reason}
    elif train_pos == 0 or train_neg == 0:
        reason = "training split must contain both positive and negative rows"
        learned_block = _null_metric_block(reason)
        model_block = {
            "trained": False,
            "not_trained_reason": reason,
            "training_positive_count": train_pos,
            "training_negative_count": train_neg,
        }
    else:
        preprocessing_block = _fit_train_preprocessor(train_bool, FEATURE_NAMES)
        X_train = [
            _vector_from_preprocessor(r, feature_names=FEATURE_NAMES, preprocessing=preprocessing_block)
            for r in train_bool
        ]
        weights = _logistic_fit_gd(X_train, y_train)
        learned_scores = [
            (
                _dot(
                    weights,
                    _vector_from_preprocessor(r, feature_names=FEATURE_NAMES, preprocessing=preprocessing_block),
                ),
                bool(r[target]),
            )
            for r in test_bool
        ]
        learned_block = _metric_block(learned_scores)
        model_block = {
            "trained": True,
            "model": "l2_logistic_regression_gradient_descent_pure_python",
            "feature_names": list(FEATURE_NAMES),
            "training_positive_count": train_pos,
            "training_negative_count": train_neg,
            "coefficients_standardized_space": {
                "intercept": weights[-1],
                "weights": {FEATURE_NAMES[i]: weights[i] for i in range(len(FEATURE_NAMES))},
            },
        }

    return {
        "target": target,
        "selected_row_counts": {
            "train": _target_counts(train_selected, target),
            "test": _target_counts(test_selected, target),
        },
        "rows_used_for_training_or_metrics": {
            "train_boolean_joined_feature_rows": len(train_bool),
            "test_boolean_joined_feature_rows": len(test_bool),
        },
        "excluded_rows": {
            "train_missing_target_count": len(train_missing_target),
            "test_missing_target_count": len(test_missing_target),
            "train_missing_feature_count": len(train_bool_without_features),
            "test_missing_feature_count": len(test_bool_without_features),
            "test_missing_final_score_count_for_heuristic": missing_heuristic_final_score,
            "reasons": {
                "missing_target": "target is not a JSON boolean",
                "missing_feature": "no paper_scores row joined for the required score family and ranking_run_id",
                "missing_final_score": "paper_scores row joined but final_score was null or non-numeric",
            },
        },
        "preprocessing": preprocessing_block,
        "learned_model": model_block,
        "blind_test_metrics": {
            "heuristic_final_score": _metric_block(heuristic_scores_clean),
            "learned_model": learned_block,
        },
    }


def _join_source_split_rows(
    rows: list[dict[str, Any]],
    *,
    by_work: dict[tuple[str, int], dict],
    by_wtoken: dict[tuple[str, str], dict],
    family: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    train_selected = [
        dict(r)
        for r in rows
        if r.get("family") == family and str(r.get("review_pool_variant") or "") in TRAIN_REVIEW_POOL_VARIANTS
    ]
    test_selected = [
        dict(r)
        for r in rows
        if r.get("family") is None and str(r.get("review_pool_variant") or "") == BLIND_TEST_REVIEW_POOL_VARIANT
    ]

    train_joined: list[dict[str, Any]] = []
    train_missing: list[dict[str, Any]] = []
    for lab in train_selected:
        sc = join_label_row_to_score(lab, by_work, by_wtoken)
        if sc is None:
            lab["_joined_score"] = False
            train_missing.append(lab)
        else:
            train_joined.append(_merge_label_with_score(lab, sc))

    test_joined: list[dict[str, Any]] = []
    test_missing: list[dict[str, Any]] = []
    for lab in test_selected:
        sc = join_label_row_to_score_family(lab, by_work, by_wtoken, score_family=family)
        if sc is None:
            lab["_joined_score"] = False
            test_missing.append(lab)
        else:
            joined = _merge_label_with_score(lab, sc)
            joined["family"] = lab.get("family")
            test_joined.append(joined)

    train_all = train_joined + train_missing
    test_all = test_joined + test_missing
    return train_all, train_joined, train_missing, test_all, test_joined, test_missing


def build_ml_source_split_tiny_baseline_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    ranking_run_id: str,
    family: str,
) -> dict[str, Any]:
    rid = ranking_run_id.strip()
    if not rid:
        raise MLSourceSplitTinyBaselineError("ranking_run_id must be non-empty")
    fam = family.strip().lower()
    if fam != EMERGING_FAMILY:
        raise MLSourceSplitTinyBaselineError(
            f"ml-source-split-tiny-baseline supports only family={EMERGING_FAMILY!r}, not {family!r}",
        )
    for target in TARGET_ORDER:
        if target not in ALLOWED_TARGETS:
            raise MLSourceSplitTinyBaselineError(f"target {target!r} is not supported by tiny baseline machinery")

    label_path = label_dataset_path.resolve()
    policy_path = conflict_policy_path.resolve()
    if not label_path.is_file():
        raise MLSourceSplitTinyBaselineError(f"label dataset not found: {label_path}")
    if not policy_path.is_file():
        raise MLSourceSplitTinyBaselineError(f"conflict policy not found: {policy_path}")

    label_sha = sha256_file(label_path)
    policy_sha = sha256_file(policy_path)
    raw = load_label_dataset(label_path)
    label_version = str(raw.get("dataset_version", ""))
    rows = _audit_rows_for_run_observation_level(raw, ranking_run_id=rid)

    run_row = fetch_ranking_run_row(conn, ranking_run_id=rid)
    cfg = _parse_config_json(run_row.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""
    score_rows = fetch_paper_scores_with_openalex(conn, ranking_run_id=rid)
    by_work, by_wtoken = _build_score_lookups(score_rows)

    train_all, train_joined, train_missing, test_all, test_joined, test_missing = _join_source_split_rows(
        rows,
        by_work=by_work,
        by_wtoken=by_wtoken,
        family=fam,
    )

    blind_train_overlap = [
        _row_key(r)
        for r in train_all
        if str(r.get("review_pool_variant") or "") == BLIND_TEST_REVIEW_POOL_VARIANT
    ]
    train_keys = {_row_key(r) for r in train_all}
    test_keys = {_row_key(r) for r in test_all}

    targets = {
        target: _target_result(
            target=target,
            train_selected=train_all,
            train_joined=train_joined,
            test_selected=test_all,
            test_joined=test_joined,
        )
        for target in TARGET_ORDER
    }

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_source_split_tiny_baseline",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": rid,
            "ranking_version": str(run_row.get("ranking_version", "")),
            "corpus_snapshot_version": str(run_row.get("corpus_snapshot_version", "")),
            "embedding_version": str(run_row.get("embedding_version", "")),
            "cluster_version": cluster_version,
            "label_dataset_path": portable_repo_path(label_path),
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
            "conflict_policy_path": portable_repo_path(policy_path),
            "conflict_policy_sha256": policy_sha,
            "family_context": fam,
            "score_family_for_blind_rows": fam,
            "targets": list(TARGET_ORDER),
        },
        "conflict_policy_summary": dict(CONFLICT_POLICY_SUMMARY),
        "caveats": list(SOURCE_SPLIT_CAVEATS),
        "source_split_policy": {
            "observation_level_rows": True,
            "duplicate_merging": "none",
            "conflict_resolution": "none",
            "train_slice": {
                "family": fam,
                "ranking_run_id": rid,
                "review_pool_variants": list(TRAIN_REVIEW_POOL_VARIANTS),
            },
            "test_slice": {
                "family": None,
                "ranking_run_id": rid,
                "review_pool_variant": BLIND_TEST_REVIEW_POOL_VARIANT,
                "paper_scores_lookup_family": fam,
            },
        },
        "row_counts": {
            "audit_rows_for_ranking_run": len(rows),
            "train": {
                "total": len(train_all),
                "by_review_pool_variant": _variant_counts(train_all),
            },
            "test": {
                "total": len(test_all),
                "by_review_pool_variant": _variant_counts(test_all),
            },
        },
        "feature_coverage": {
            "train": _coverage_block(selected_rows=train_all, joined_rows=train_joined, missing_rows=train_missing),
            "test": _coverage_block(selected_rows=test_all, joined_rows=test_joined, missing_rows=test_missing),
            "missing_feature_rows": {
                "train": [_missing_feature_row(r) for r in train_missing],
                "test": [_missing_feature_row(r) for r in test_missing],
            },
        },
        "leakage_checks": {
            "blind_rows_selected_for_train_count": len(blind_train_overlap),
            "blind_rows_selected_for_train_keys": blind_train_overlap,
            "train_test_observation_key_overlap_count": len(train_keys & test_keys),
            "train_test_observation_key_overlap": sorted(train_keys & test_keys),
            "blind_test_rows_family_null_count": sum(1 for r in test_all if r.get("family") is None),
            "blind_test_rows_family_non_null_count": sum(1 for r in test_all if r.get("family") is not None),
        },
        "targets": targets,
        "interpretation_note": (
            "This fixed source split asks whether a tiny logistic model trained only on rank-shaped emerging labels "
            "transfers to blind-source labels. It is an offline diagnostic only and must not be described as validation "
            "or used to alter production ranking defaults."
        ),
    }


def _fmt_metric(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if math.isnan(value):
            return "n/a"
        return f"{value:.4f}"
    return str(value)


def _format_p_at_k(block: dict[str, Any]) -> str:
    p = block.get("precision_at_k") or {}
    vals = []
    for k in P_AT_K_VALUES:
        entry = p.get(str(k)) or {}
        vals.append(_fmt_metric(entry.get("value")))
    return " / ".join(vals)


def markdown_from_ml_source_split_tiny_baseline(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    rows = payload["row_counts"]
    cov = payload["feature_coverage"]
    lines = [
        "# Source-split tiny baseline diagnostic (emerging)",
        "",
        "Offline-only diagnostic: train a tiny logistic baseline on rank-shaped emerging audit labels and test it on blind-source rows. No ranking, API, web, or production model behavior changes.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **label_dataset_path:** `{prov.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        f"- **conflict_policy_path:** `{prov.get('conflict_policy_path')}`",
        f"- **conflict_policy_sha256:** `{prov.get('conflict_policy_sha256')}`",
        f"- **family_context / score family:** `{prov.get('family_context')}`",
        "",
        "## Conflict Policy Summary",
        "",
        "- observation-level rows",
        "- no silent merge",
        "- no automatic conflict resolution",
        "- blind rows test-only",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        "## Split Counts",
        "",
        f"- **train rows:** `{rows['train']['total']}`",
        f"- **test rows:** `{rows['test']['total']}`",
        f"- **train variants:** `{rows['train']['by_review_pool_variant']}`",
        f"- **test variants:** `{rows['test']['by_review_pool_variant']}`",
        "",
        "## Feature Coverage",
        "",
        f"- **train joined / selected:** `{cov['train']['joined_feature_row_count']}` / `{cov['train']['selected_row_count']}`",
        f"- **test joined / selected:** `{cov['test']['joined_feature_row_count']}` / `{cov['test']['selected_row_count']}`",
        f"- **train missing features:** `{cov['train']['missing_feature_row_count']}`",
        f"- **test missing features:** `{cov['test']['missing_feature_row_count']}`",
        "",
        "## Blind Test Metrics",
        "",
        "| target | train pos/neg/null | test pos/neg/null | heuristic AUC | learned AUC | heuristic P@5/10/20 | learned P@5/10/20 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for target, block in payload.get("targets", {}).items():
        train_counts = block["selected_row_counts"]["train"]
        test_counts = block["selected_row_counts"]["test"]
        h = block["blind_test_metrics"]["heuristic_final_score"]
        l = block["blind_test_metrics"]["learned_model"]
        lines.append(
            "| "
            f"`{target}` | "
            f"{train_counts['positive']}/{train_counts['negative']}/{train_counts['null']} | "
            f"{test_counts['positive']}/{test_counts['negative']}/{test_counts['null']} | "
            f"{_fmt_metric(h.get('roc_auc_mann_whitney'))} | "
            f"{_fmt_metric(l.get('roc_auc_mann_whitney'))} | "
            f"{_format_p_at_k(h)} | "
            f"{_format_p_at_k(l)} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            payload.get("interpretation_note", ""),
            "",
            "P@k values are `n/a` when fewer than k labeled, scored blind rows are available for that target/channel. AUC is `n/a` unless the scored blind rows contain both classes.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_ml_source_split_tiny_baseline(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    ranking_run_id: str,
    family: str,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_source_split_tiny_baseline_payload(
        conn,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        ranking_run_id=ranking_run_id,
        family=family,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_source_split_tiny_baseline(payload), encoding="utf-8")
    return payload


def run_ml_source_split_tiny_baseline_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    ranking_run_id: str,
    family: str,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_source_split_tiny_baseline(
            conn,
            label_dataset_path=label_dataset_path,
            conflict_policy_path=conflict_policy_path,
            ranking_run_id=ranking_run_id,
            family=family,
            json_path=output_json,
            markdown_path=markdown_output,
        )
