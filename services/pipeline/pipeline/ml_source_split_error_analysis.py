"""Blind-row error analysis for source-split tiny baseline outputs.

This module reuses the frozen preprocessing and coefficients from a committed
source-split artifact. It rejoins blind rows to persisted paper_scores for
features, but does not refit anything and does not write to the database.
"""

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
    fetch_paper_scores_with_openalex,
    fetch_ranking_run_row,
    join_label_row_to_score_family,
    load_label_dataset,
    normalize_w_token,
    roc_auc_mann_whitney,
    sha256_file,
)
from pipeline.ml_source_split_tiny_baseline import (
    BLIND_TEST_REVIEW_POOL_VARIANT,
    P_AT_K_VALUES,
    TARGET_ORDER,
    _audit_rows_for_run_observation_level,
    _merge_label_with_score,
)
from pipeline.ml_tiny_baseline import FEATURE_NAMES, _dot, _float_or_none, _row_feature_vector
from pipeline.repo_paths import portable_repo_path

ERROR_ANALYSIS_CAVEATS = (
    "Not validation.",
    "Blind-source offline diagnostic only.",
    "Learned model underperformed heuristic on source-split blind metrics for at least one target; must not drive production ranking.",
    "Buckets are for feature/label inspection, not product-quality claims.",
)

BUCKET_ORDER = (
    "promoted_positive",
    "promoted_negative",
    "demoted_positive",
    "demoted_negative",
)

DEFAULT_TOP_N = 10
FLOAT_TOL = 1e-9


class MLSourceSplitErrorAnalysisError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def load_source_split_artifact(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise MLSourceSplitErrorAnalysisError(f"failed to load source-split artifact {path}: {e}") from e
    if payload.get("artifact_type") != "ml_source_split_tiny_baseline":
        raise MLSourceSplitErrorAnalysisError(
            f"source-split artifact has unexpected artifact_type={payload.get('artifact_type')!r}"
        )
    return payload


def validate_source_split_provenance(
    source_artifact: dict[str, Any],
    *,
    ranking_run_id: str,
    family: str,
) -> dict[str, Any]:
    prov = source_artifact.get("provenance")
    if not isinstance(prov, dict):
        raise MLSourceSplitErrorAnalysisError("source-split artifact missing provenance")
    artifact_rid = str(prov.get("ranking_run_id") or "")
    if artifact_rid != ranking_run_id:
        raise MLSourceSplitErrorAnalysisError(
            f"--ranking-run-id {ranking_run_id!r} does not match source artifact ranking_run_id {artifact_rid!r}"
        )
    score_family = str(prov.get("score_family_for_blind_rows") or "")
    family_context = str(prov.get("family_context") or "")
    if family != score_family:
        raise MLSourceSplitErrorAnalysisError(
            f"--family {family!r} does not match source artifact score_family_for_blind_rows {score_family!r}"
        )
    if family_context and family_context != score_family:
        raise MLSourceSplitErrorAnalysisError(
            "source artifact provenance has inconsistent family_context "
            f"{family_context!r} and score_family_for_blind_rows {score_family!r}"
        )
    return prov


def _assert_label_sha_matches(source_prov: dict[str, Any], *, label_dataset_path: Path) -> str:
    actual = sha256_file(label_dataset_path)
    expected = str(source_prov.get("label_dataset_sha256") or "")
    if expected and actual != expected:
        raise MLSourceSplitErrorAnalysisError(
            f"label dataset SHA mismatch: source artifact has {expected}, disk has {actual}"
        )
    return actual


def _blind_rows_for_artifact(
    label_payload: dict[str, Any],
    *,
    ranking_run_id: str,
) -> list[dict[str, Any]]:
    rows = _audit_rows_for_run_observation_level(label_payload, ranking_run_id=ranking_run_id)
    return [
        dict(r)
        for r in rows
        if r.get("family") is None and str(r.get("review_pool_variant") or "") == BLIND_TEST_REVIEW_POOL_VARIANT
    ]


def _join_blind_rows_to_scores(
    blind_rows: list[dict[str, Any]],
    *,
    score_rows: Sequence[dict[str, Any]],
    score_family: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_work, by_wtoken = _build_score_lookups(score_rows)
    joined: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    for lab in blind_rows:
        sc = join_label_row_to_score_family(lab, by_work, by_wtoken, score_family=score_family)
        if sc is None:
            miss = dict(lab)
            miss["_joined_score"] = False
            missing.append(miss)
            continue
        merged = _merge_label_with_score(lab, sc)
        merged["family"] = lab.get("family")
        joined.append(merged)
    return joined, missing


def _preprocessing_vector(row: dict[str, Any], preprocessing: dict[str, Any]) -> list[float]:
    return _row_feature_vector(
        row,
        FEATURE_NAMES,
        list(preprocessing["medians"]),
        list(preprocessing["means"]),
        list(preprocessing["stds"]),
    )


def _frozen_learned_logit(row: dict[str, Any], target_artifact: dict[str, Any]) -> float:
    preprocessing = target_artifact.get("preprocessing")
    model = target_artifact.get("learned_model")
    if not isinstance(preprocessing, dict) or not isinstance(model, dict) or not model.get("trained"):
        raise MLSourceSplitErrorAnalysisError("source artifact target is missing trained preprocessing/model")
    coef = model.get("coefficients_standardized_space")
    if not isinstance(coef, dict):
        raise MLSourceSplitErrorAnalysisError("source artifact target missing coefficients_standardized_space")
    weights_raw = coef.get("weights")
    if not isinstance(weights_raw, dict):
        raise MLSourceSplitErrorAnalysisError("source artifact target missing feature weights")
    weights = [float(weights_raw[name]) for name in FEATURE_NAMES]
    intercept = float(coef.get("intercept") or 0.0)
    return _dot(weights + [intercept], _preprocessing_vector(row, preprocessing))


def ordinal_rank_descending(rows: Sequence[dict[str, Any]], score_field: str) -> dict[str, int]:
    order = sorted(
        range(len(rows)),
        key=lambda i: (
            -float(rows[i].get(score_field) or 0.0),
            str(rows[i].get("row_id") or ""),
            str(rows[i].get("paper_id") or ""),
        ),
    )
    return {str(rows[idx].get("_analysis_key")): pos for pos, idx in enumerate(order, start=1)}


def _analysis_key(row: dict[str, Any], idx: int) -> str:
    row_id = str(row.get("row_id") or "").strip()
    if row_id:
        return f"row_id:{row_id}"
    paper_id = str(row.get("paper_id") or "").strip()
    if paper_id:
        return f"paper_id:{paper_id}"
    return f"idx:{idx}"


def _openalex_work_id_from_label(row: dict[str, Any]) -> str | None:
    for key in ("openalex_work_id", "paper_id", "work_id"):
        wt = normalize_w_token(str(row.get(key) or ""))
        if wt:
            return wt
    return None


def _detail_row(row: dict[str, Any], *, target: str) -> dict[str, Any]:
    detail = {
        "row_id": row.get("row_id"),
        "paper_id": row.get("paper_id"),
        "openalex_work_id": _openalex_work_id_from_label(row),
        "work_id": row.get("work_id"),
        "title": row.get("title"),
        "target": target,
        "relevance_label": row.get("relevance_label"),
        "novelty_label": row.get("novelty_label"),
        "bridge_like_label": row.get("bridge_like_label"),
        "good_or_acceptable": row.get("good_or_acceptable"),
        "surprising_or_useful": row.get("surprising_or_useful"),
        "reviewer_notes": row.get("reviewer_notes"),
        "source_worksheet_path": row.get("source_worksheet_path"),
        "review_pool_variant": row.get("review_pool_variant"),
        "sample_reason": row.get("sample_reason"),
        "cluster_id": row.get("cluster_id"),
        "topics": row.get("topics"),
        "final_score": row.get("final_score"),
        "learned_logit": row.get("learned_logit"),
        "heuristic_rank": row.get("heuristic_rank"),
        "learned_rank": row.get("learned_rank"),
        "rank_delta": row.get("rank_delta"),
        "semantic_score": row.get("semantic_score"),
        "citation_velocity_score": row.get("citation_velocity_score"),
        "topic_growth_score": row.get("topic_growth_score"),
        "diversity_penalty": row.get("diversity_penalty"),
    }
    if row.get("internal_work_id") is not None:
        detail["internal_work_id"] = row.get("internal_work_id")
    if row.get("ranking_context_family_scores_json") is not None:
        detail["ranking_context_family_scores_json"] = row.get("ranking_context_family_scores_json")
    if row.get("ranking_context_family_ranks_json") is not None:
        detail["ranking_context_family_ranks_json"] = row.get("ranking_context_family_ranks_json")
    return detail


def _movement_bucket(row: dict[str, Any], *, target: str) -> str:
    positive = bool(row.get(target))
    delta = int(row.get("rank_delta") or 0)
    if delta > 0:
        return "promoted_positive" if positive else "promoted_negative"
    if delta < 0:
        return "demoted_positive" if positive else "demoted_negative"
    return "stable_positive" if positive else "stable_negative"


def _top_k_overlap(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in P_AT_K_VALUES:
        h_top = sorted(rows, key=lambda r: (int(r["heuristic_rank"]), str(r.get("paper_id") or "")))[:k]
        l_top = sorted(rows, key=lambda r: (int(r["learned_rank"]), str(r.get("paper_id") or "")))[:k]
        hs = {str(r.get("paper_id") or "") for r in h_top if r.get("paper_id")}
        ls = {str(r.get("paper_id") or "") for r in l_top if r.get("paper_id")}
        inter = hs & ls
        union = hs | ls
        out[str(k)] = {
            "k": k,
            "heuristic_top_k_size": len(hs),
            "learned_top_k_size": len(ls),
            "intersection_size": len(inter),
            "union_size": len(union),
            "jaccard": (len(inter) / len(union)) if union else None,
            "overlap_paper_ids": sorted(inter),
        }
    return out


def _mean_median(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "mean": None, "median": None}
    return {"count": len(values), "mean": statistics.fmean(values), "median": statistics.median(values)}


def _feature_summaries_by_bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for bucket in BUCKET_ORDER:
        bucket_rows = [r for r in rows if r.get("movement_bucket") == bucket]
        out[bucket] = {
            feature: _mean_median(
                [
                    float(v)
                    for v in (_float_or_none(r.get(feature)) for r in bucket_rows)
                    if v is not None
                ]
            )
            for feature in FEATURE_NAMES
        }
    return out


def _metrics_for_scores(scores_labels: list[tuple[float, bool]]) -> dict[str, Any]:
    pos = sum(1 for _s, y in scores_labels if y)
    neg = len(scores_labels) - pos
    return {
        "row_count": len(scores_labels),
        "positive_count": pos,
        "negative_count": neg,
        "roc_auc_mann_whitney": roc_auc_mann_whitney(scores_labels),
    }


def _assert_auc_matches_source(
    source_target: dict[str, Any],
    *,
    learned_scores: list[tuple[float, bool]],
    heuristic_scores: list[tuple[float, bool]],
) -> dict[str, Any]:
    source_metrics = source_target.get("blind_test_metrics") if isinstance(source_target, dict) else None
    if not isinstance(source_metrics, dict):
        raise MLSourceSplitErrorAnalysisError("source artifact target missing blind_test_metrics")
    checks: dict[str, Any] = {}
    for key, scores in (
        ("learned_model", learned_scores),
        ("heuristic_final_score", heuristic_scores),
    ):
        expected = (((source_metrics.get(key) or {}).get("roc_auc_mann_whitney")))
        actual = roc_auc_mann_whitney(scores)
        ok = expected is None and actual is None
        if isinstance(expected, (float, int)) and isinstance(actual, (float, int)):
            ok = abs(float(expected) - float(actual)) <= FLOAT_TOL
        if not ok:
            raise MLSourceSplitErrorAnalysisError(
                f"recomputed {key} blind ROC AUC {actual!r} does not match source artifact {expected!r}"
            )
        checks[key] = {"expected_roc_auc": expected, "recomputed_roc_auc": actual, "matches": True}
    return checks


def _target_analysis(
    *,
    target: str,
    blind_joined_rows: list[dict[str, Any]],
    source_target: dict[str, Any],
    top_n: int,
) -> dict[str, Any]:
    eligible: list[dict[str, Any]] = []
    for idx, row in enumerate(blind_joined_rows):
        if not isinstance(row.get(target), bool):
            continue
        final_score = _float_or_none(row.get("final_score"))
        if final_score is None:
            continue
        scored = dict(row)
        scored["_analysis_key"] = _analysis_key(scored, idx)
        scored["final_score"] = final_score
        scored["learned_logit"] = _frozen_learned_logit(scored, source_target)
        eligible.append(scored)

    h_ranks = ordinal_rank_descending(eligible, "final_score")
    l_ranks = ordinal_rank_descending(eligible, "learned_logit")
    for row in eligible:
        key = str(row["_analysis_key"])
        row["heuristic_rank"] = h_ranks[key]
        row["learned_rank"] = l_ranks[key]
        row["rank_delta"] = int(row["heuristic_rank"]) - int(row["learned_rank"])
        row["movement_bucket"] = _movement_bucket(row, target=target)

    heuristic_scores = [(float(r["final_score"]), bool(r[target])) for r in eligible]
    learned_scores = [(float(r["learned_logit"]), bool(r[target])) for r in eligible]
    drift_checks = _assert_auc_matches_source(
        source_target,
        learned_scores=learned_scores,
        heuristic_scores=heuristic_scores,
    )

    promotions = sorted(eligible, key=lambda r: (-int(r["rank_delta"]), str(r.get("row_id") or ""), str(r.get("paper_id") or "")))
    demotions = sorted(eligible, key=lambda r: (int(r["rank_delta"]), str(r.get("row_id") or ""), str(r.get("paper_id") or "")))
    promoted_negatives = [r for r in promotions if r.get(target) is False and int(r.get("rank_delta") or 0) > 0]
    demoted_positives = [r for r in demotions if r.get(target) is True and int(r.get("rank_delta") or 0) < 0]
    bucket_counts: dict[str, int] = {}
    for row in eligible:
        b = str(row.get("movement_bucket"))
        bucket_counts[b] = bucket_counts.get(b, 0) + 1

    return {
        "target": target,
        "blind_boolean_row_count": len(eligible),
        "class_counts": {
            "positive": sum(1 for r in eligible if r.get(target) is True),
            "negative": sum(1 for r in eligible if r.get(target) is False),
            "null_or_excluded": len(blind_joined_rows) - len(eligible),
        },
        "drift_checks": drift_checks,
        "metrics": {
            "heuristic_final_score": _metrics_for_scores(heuristic_scores),
            "learned_model": _metrics_for_scores(learned_scores),
        },
        "top_k_overlap": _top_k_overlap(eligible),
        "bucket_counts": dict(sorted(bucket_counts.items())),
        "largest_learned_promotions": [_detail_row(r, target=target) for r in promotions if int(r.get("rank_delta") or 0) > 0][:top_n],
        "largest_learned_demotions": [_detail_row(r, target=target) for r in demotions if int(r.get("rank_delta") or 0) < 0][:top_n],
        "promoted_negatives": [_detail_row(r, target=target) for r in promoted_negatives[:top_n]],
        "demoted_positives": [_detail_row(r, target=target) for r in demoted_positives[:top_n]],
        "feature_summaries_by_bucket": _feature_summaries_by_bucket(eligible),
        "interpretation": (
            "These rows describe correlations between frozen learned-logit reorderings, labels, and persisted features "
            "inside the blind-source slice. They do not establish causation or product-quality impact; labels are "
            "single-reviewer judgments and the blind sample still has sampling design constraints."
        ),
        "all_detail_rows": [_detail_row(r, target=target) | {"movement_bucket": r.get("movement_bucket")} for r in eligible],
    }


def build_ml_source_split_error_analysis_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    source_split_artifact_path: Path,
    ranking_run_id: str,
    family: str,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    rid = ranking_run_id.strip()
    fam = family.strip().lower()
    if not rid:
        raise MLSourceSplitErrorAnalysisError("ranking_run_id must be non-empty")
    if not fam:
        raise MLSourceSplitErrorAnalysisError("family must be non-empty")
    if top_n < 1 or top_n > 100:
        raise MLSourceSplitErrorAnalysisError("top_n must be between 1 and 100")

    label_path = label_dataset_path.resolve()
    source_path = source_split_artifact_path.resolve()
    if not label_path.is_file():
        raise MLSourceSplitErrorAnalysisError(f"label dataset not found: {label_path}")
    if not source_path.is_file():
        raise MLSourceSplitErrorAnalysisError(f"source-split artifact not found: {source_path}")

    source_artifact = load_source_split_artifact(source_path)
    source_prov = validate_source_split_provenance(source_artifact, ranking_run_id=rid, family=fam)
    label_sha = _assert_label_sha_matches(source_prov, label_dataset_path=label_path)
    source_sha = sha256_file(source_path)

    label_payload = load_label_dataset(label_path)
    blind_rows = _blind_rows_for_artifact(label_payload, ranking_run_id=rid)

    run_row = fetch_ranking_run_row(conn, ranking_run_id=rid)
    score_rows = fetch_paper_scores_with_openalex(conn, ranking_run_id=rid)
    blind_joined, blind_missing = _join_blind_rows_to_scores(blind_rows, score_rows=score_rows, score_family=fam)

    targets_artifact = source_artifact.get("targets")
    if not isinstance(targets_artifact, dict):
        raise MLSourceSplitErrorAnalysisError("source artifact missing targets")
    targets_out: dict[str, Any] = {}
    for target in TARGET_ORDER:
        source_target = targets_artifact.get(target)
        if not isinstance(source_target, dict):
            raise MLSourceSplitErrorAnalysisError(f"source artifact missing target {target!r}")
        targets_out[target] = _target_analysis(
            target=target,
            blind_joined_rows=blind_joined,
            source_target=source_target,
            top_n=top_n,
        )

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_source_split_error_analysis",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": rid,
            "family": fam,
            "score_family_for_blind_rows": fam,
            "ranking_version": str(run_row.get("ranking_version", "")),
            "corpus_snapshot_version": str(run_row.get("corpus_snapshot_version", "")),
            "embedding_version": str(run_row.get("embedding_version", "")),
            "label_dataset_path": portable_repo_path(label_path),
            "label_dataset_sha256": label_sha,
            "source_split_artifact_path": portable_repo_path(source_path),
            "source_split_artifact_sha256": source_sha,
            "source_split_conflict_policy_path": source_prov.get("conflict_policy_path"),
            "source_split_conflict_policy_sha256": source_prov.get("conflict_policy_sha256"),
            "top_n": top_n,
        },
        "caveats": list(ERROR_ANALYSIS_CAVEATS),
        "analysis_scope": {
            "blind_test_rows_only": True,
            "review_pool_variant": BLIND_TEST_REVIEW_POOL_VARIANT,
            "label_family": None,
            "ranking_run_id": rid,
            "score_family": fam,
            "model_source": "frozen preprocessing and coefficients from source_split_artifact_path",
        },
        "feature_join_summary": {
            "blind_selected_row_count": len(blind_rows),
            "blind_joined_feature_row_count": len(blind_joined),
            "blind_missing_feature_row_count": len(blind_missing),
            "missing_feature_rows": [
                {
                    "row_id": r.get("row_id"),
                    "paper_id": r.get("paper_id"),
                    "work_id": r.get("work_id"),
                    "internal_work_id": r.get("internal_work_id"),
                }
                for r in blind_missing
            ],
        },
        "targets": targets_out,
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if math.isnan(value):
            return "n/a"
        return f"{value:.4f}"
    return str(value)


def _short_text(value: Any, max_len: int = 90) -> str:
    text = "" if value is None else " ".join(str(value).split())
    text = text.replace("|", "\\|")
    if len(text) > max_len:
        return text[: max_len - 3].rstrip() + "..."
    return text


def _row_line(row: dict[str, Any]) -> str:
    return (
        f"| `{_short_text(row.get('paper_id'), 52)}` | {row.get('rank_delta')} | "
        f"{_fmt(row.get('final_score'))} | {_fmt(row.get('learned_logit'))} | "
        f"{_short_text(row.get('reviewer_notes'))} |"
    )


def markdown_from_ml_source_split_error_analysis(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    lines = [
        "# Source-split blind error analysis",
        "",
        "Reviewer-facing offline audit of how frozen source-split learned logits reorder blind-source rows versus `final_score`.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **family / score family:** `{prov.get('family')}`",
        f"- **label_dataset_path:** `{prov.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        f"- **source_split_artifact_path:** `{prov.get('source_split_artifact_path')}`",
        f"- **source_split_artifact_sha256:** `{prov.get('source_split_artifact_sha256')}`",
        f"- **conflict_policy_path:** `{prov.get('source_split_conflict_policy_path')}`",
        f"- **conflict_policy_sha256:** `{prov.get('source_split_conflict_policy_sha256')}`",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        "## Feature Join",
        "",
        f"- **blind selected rows:** `{payload['feature_join_summary']['blind_selected_row_count']}`",
        f"- **joined feature rows:** `{payload['feature_join_summary']['blind_joined_feature_row_count']}`",
        f"- **missing feature rows:** `{payload['feature_join_summary']['blind_missing_feature_row_count']}`",
        "",
    ]
    for target, block in payload.get("targets", {}).items():
        lines.extend(
            [
                f"## Target `{target}`",
                "",
                f"- **blind boolean rows:** `{block.get('blind_boolean_row_count')}`",
                f"- **class counts:** positive `{block['class_counts']['positive']}`, negative `{block['class_counts']['negative']}`",
                f"- **heuristic AUC:** `{_fmt(block['metrics']['heuristic_final_score'].get('roc_auc_mann_whitney'))}`",
                f"- **learned AUC:** `{_fmt(block['metrics']['learned_model'].get('roc_auc_mann_whitney'))}`",
                "",
                "### Top-k Overlap",
                "",
                "| k | heuristic size | learned size | intersection | union | Jaccard |",
                "|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for k in P_AT_K_VALUES:
            o = block["top_k_overlap"][str(k)]
            lines.append(
                f"| {k} | {o['heuristic_top_k_size']} | {o['learned_top_k_size']} | "
                f"{o['intersection_size']} | {o['union_size']} | {_fmt(o['jaccard'])} |"
            )
        lines.extend(
            [
                "",
                "### Promoted negatives",
                "",
                "| paper_id | rank_delta | final_score | learned_logit | reviewer notes |",
                "|---|---:|---:|---:|---|",
            ]
        )
        for row in block.get("promoted_negatives", [])[:5]:
            lines.append(_row_line(row))
        if not block.get("promoted_negatives"):
            lines.append("| n/a | n/a | n/a | n/a | none |")
        lines.extend(
            [
                "",
                "### Demoted positives",
                "",
                "| paper_id | rank_delta | final_score | learned_logit | reviewer notes |",
                "|---|---:|---:|---:|---|",
            ]
        )
        for row in block.get("demoted_positives", [])[:5]:
            lines.append(_row_line(row))
        if not block.get("demoted_positives"):
            lines.append("| n/a | n/a | n/a | n/a | none |")
        lines.extend(
            [
                "",
                "### Largest learned promotions",
                "",
                "| paper_id | rank_delta | final_score | learned_logit | reviewer notes |",
                "|---|---:|---:|---:|---|",
            ]
        )
        for row in block.get("largest_learned_promotions", [])[:5]:
            lines.append(_row_line(row))
        lines.extend(
            [
                "",
                "### Largest learned demotions",
                "",
                "| paper_id | rank_delta | final_score | learned_logit | reviewer notes |",
                "|---|---:|---:|---:|---|",
            ]
        )
        for row in block.get("largest_learned_demotions", [])[:5]:
            lines.append(_row_line(row))
        lines.extend(
            [
                "",
                "### Feature summaries by bucket",
                "",
                "| bucket | feature | count | mean | median |",
                "|---|---|---:|---:|---:|",
            ]
        )
        summaries = block.get("feature_summaries_by_bucket") or {}
        for bucket in BUCKET_ORDER:
            feature_block = summaries.get(bucket) or {}
            for feature in FEATURE_NAMES:
                s = feature_block.get(feature) or {}
                lines.append(
                    f"| `{bucket}` | `{feature}` | {s.get('count', 0)} | "
                    f"{_fmt(s.get('mean'))} | {_fmt(s.get('median'))} |"
                )
        lines.extend(["", "### Interpretation", "", block.get("interpretation", ""), ""])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_source_split_error_analysis(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    source_split_artifact_path: Path,
    ranking_run_id: str,
    family: str,
    json_path: Path,
    markdown_path: Path | None,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    payload = build_ml_source_split_error_analysis_payload(
        conn,
        label_dataset_path=label_dataset_path,
        source_split_artifact_path=source_split_artifact_path,
        ranking_run_id=ranking_run_id,
        family=family,
        top_n=top_n,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_source_split_error_analysis(payload), encoding="utf-8")
    return payload


def run_ml_source_split_error_analysis_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    source_split_artifact_path: Path,
    ranking_run_id: str,
    family: str,
    output_json: Path,
    markdown_output: Path | None,
    top_n: int = DEFAULT_TOP_N,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_source_split_error_analysis(
            conn,
            label_dataset_path=label_dataset_path,
            source_split_artifact_path=source_split_artifact_path,
            ranking_run_id=ranking_run_id,
            family=family,
            json_path=output_json,
            markdown_path=markdown_output,
            top_n=top_n,
        )
