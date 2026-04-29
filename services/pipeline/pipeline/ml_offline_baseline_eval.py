"""Read-only offline baseline evaluation: join ml-label-dataset rows to paper_scores (no training, no writes)."""

from __future__ import annotations

import hashlib
import json
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.recommendation_review_worksheet import cluster_version_from_config

VALID_FAMILIES = frozenset({"bridge", "emerging", "undercited"})
TARGET_FIELDS = ("good_or_acceptable", "surprising_or_useful", "bridge_like_yes_or_partial")
FEATURE_FIELDS = (
    "final_score",
    "bridge_score",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "diversity_penalty",
)

_W_TOKEN = re.compile(r"(W\d+)", re.IGNORECASE)


class MLOfflineBaselineEvalError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_w_token(value: str | None) -> str | None:
    if not value:
        return None
    m = _W_TOKEN.search(str(value).strip())
    if not m:
        return None
    return m.group(1).upper()


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            p = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(p) if isinstance(p, dict) else {}
    return {}


def load_label_dataset(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise MLOfflineBaselineEvalError(f"Failed to load label dataset {path}: {e}") from e


def filter_audit_rows_for_run(
    payload: dict[str, Any],
    *,
    ranking_run_id: str,
) -> tuple[list[dict[str, Any]], int]:
    """Rows with split=audit_only and ranking_run_id match; dedupe exact duplicate row_id."""
    rows_in = payload.get("rows")
    if not isinstance(rows_in, list):
        raise MLOfflineBaselineEvalError("label dataset missing 'rows' array")
    out: list[dict[str, Any]] = []
    seen_rid: set[str] = set()
    skipped_dup = 0
    for r in rows_in:
        if not isinstance(r, dict):
            continue
        if str(r.get("split", "")) != "audit_only":
            continue
        if str(r.get("ranking_run_id", "")) != ranking_run_id:
            continue
        rid = str(r.get("row_id", ""))
        if rid and rid in seen_rid:
            skipped_dup += 1
            continue
        if rid:
            seen_rid.add(rid)
        out.append(r)
    return out, skipped_dup


def _rank_scores_per_family(rows: list[dict[str, Any]]) -> None:
    """Mutate rows in-place with integer rank (1 = highest final_score) per recommendation_family."""
    by_fam: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_fam[str(r["recommendation_family"])].append(r)
    for fam, lst in by_fam.items():
        lst.sort(key=lambda x: (-float(x["final_score"]), int(x["work_id"])))
        for i, r in enumerate(lst, start=1):
            r["_rank"] = i


def fetch_ranking_run_row(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version,
                   config_json, status
            FROM ranking_runs
            WHERE ranking_run_id = %s
            """,
            (ranking_run_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise MLOfflineBaselineEvalError(f"ranking_run_id not found: {ranking_run_id!r}")
    d = dict(row)
    if str(d.get("status")) != "succeeded":
        raise MLOfflineBaselineEvalError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={d.get('status')!r}).",
        )
    return d


def fetch_paper_scores_with_openalex(conn: psycopg.Connection, *, ranking_run_id: str) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ps.work_id,
                ps.recommendation_family,
                ps.semantic_score,
                ps.citation_velocity_score,
                ps.topic_growth_score,
                ps.bridge_score,
                ps.diversity_penalty,
                ps.final_score,
                w.openalex_id
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            WHERE ps.ranking_run_id = %s
            ORDER BY ps.recommendation_family ASC, ps.final_score DESC, ps.work_id ASC
            """,
            (ranking_run_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    _rank_scores_per_family(rows)
    return rows


def _build_score_lookups(score_rows: Sequence[dict[str, Any]]) -> tuple[dict[tuple[str, int], dict], dict[tuple[str, str], dict]]:
    by_work: dict[tuple[str, int], dict] = {}
    by_wtoken: dict[tuple[str, str], dict] = {}
    for r in score_rows:
        fam = str(r["recommendation_family"])
        wid = int(r["work_id"])
        by_work[(fam, wid)] = r
        wt = normalize_w_token(str(r.get("openalex_id") or ""))
        if wt:
            by_wtoken[(fam, wt)] = r
    return by_work, by_wtoken


def join_label_row_to_score(
    label: dict[str, Any],
    by_work: dict[tuple[str, int], dict],
    by_wtoken: dict[tuple[str, str], dict],
) -> dict[str, Any] | None:
    fam = label.get("family")
    if not isinstance(fam, str) or fam not in VALID_FAMILIES:
        return None
    work_id_raw = label.get("work_id")
    paper_id = label.get("paper_id")
    score: dict[str, Any] | None = None
    if work_id_raw is not None:
        s = str(work_id_raw).strip()
        if s.isdigit():
            score = by_work.get((fam, int(s)))
    if score is None:
        wt = normalize_w_token(str(paper_id or "")) or (
            normalize_w_token(str(work_id_raw or "")) if work_id_raw else None
        )
        if wt:
            score = by_wtoken.get((fam, wt))
    return score


def pairwise_accuracy(scores_labels: list[tuple[float, bool]]) -> float | None:
    """Fraction of (pos, neg) pairs where score_pos > score_neg (+ 0.5 ties)."""
    pos_scores = [s for s, y in scores_labels if y]
    neg_scores = [s for s, y in scores_labels if not y]
    if not pos_scores or not neg_scores:
        return None
    tot = 0
    wins = 0.0
    for sp in pos_scores:
        for sn in neg_scores:
            tot += 1
            if sp > sn:
                wins += 1.0
            elif sp == sn:
                wins += 0.5
    return wins / tot if tot else None


def roc_auc_mann_whitney(scores_labels: list[tuple[float, bool]]) -> float | None:
    """Rank/Mann–Whitney U AUC (higher score ⇒ higher rank); equivalent to pairwise accuracy with tie half-weight."""
    n = len(scores_labels)
    pos_n = sum(1 for _, y in scores_labels if y)
    neg_n = n - pos_n
    if pos_n == 0 or neg_n == 0:
        return None
    order = sorted(range(n), key=lambda i: (scores_labels[i][0], scores_labels[i][1], i))
    ranks = [0.0] * n
    r_start = 1
    a = 0
    while a < n:
        b = a
        v0 = scores_labels[order[a]][0]
        while b < n and scores_labels[order[b]][0] == v0:
            b += 1
        mid = (r_start + r_start + (b - a) - 1) / 2.0
        for k in range(a, b):
            ranks[order[k]] = mid
        r_start += b - a
        a = b
    sr = sum(ranks[i] for i in range(n) if scores_labels[i][1])
    return (sr - pos_n * (pos_n + 1) / 2.0) / (pos_n * neg_n)


def precision_at_k(scores_labels_desc: list[tuple[float, bool]], k: int) -> float | None:
    """Precision@k: positives in top-k by score descending / k; None if fewer than k labeled rows."""
    if len(scores_labels_desc) < k:
        return None
    top = scores_labels_desc[:k]
    return sum(1 for _, y in top if y) / k


def summarize_features(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in FEATURE_FIELDS:
        vals: list[float] = []
        for r in rows:
            v = r.get(field)
            if v is None:
                continue
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                continue
        if not vals:
            out[field] = {"count": 0, "mean": None, "median": None}
        else:
            out[field] = {
                "count": len(vals),
                "mean": statistics.fmean(vals),
                "median": statistics.median(vals),
            }
    return out


def compute_family_target_metrics(
    joined_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    joined_rows: label fields + optional paper_scores columns; `_joined_score` True when matched.
    Stratify by label `family` (must match recommendation family for matched rows).
    """
    out: dict[str, Any] = {}
    for fam in sorted(VALID_FAMILIES):
        out[fam] = {}
        fam_rows = [r for r in joined_rows if str(r.get("family") or "") == fam]
        matched_fam = [r for r in fam_rows if r.get("_joined_score") is True]
        out[fam]["baseline_feature_summary"] = summarize_features(matched_fam)
        for target in TARGET_FIELDS:
            all_target = [r for r in fam_rows if isinstance(r.get(target), bool)]
            matched = [r for r in all_target if r.get("_joined_score") is True]
            n_miss = len(all_target) - len(matched)
            n_pos = sum(1 for r in matched if r[target] is True)
            n_neg = sum(1 for r in matched if r[target] is False)

            pos_ranks = [int(r["_rank"]) for r in matched if r[target] is True and r.get("_rank") is not None]
            neg_ranks = [int(r["_rank"]) for r in matched if r[target] is False and r.get("_rank") is not None]
            pos_scores = [float(r["final_score"]) for r in matched if r[target] is True and r.get("final_score") is not None]
            neg_scores = [float(r["final_score"]) for r in matched if r[target] is False and r.get("final_score") is not None]

            sl_desc = sorted(
                [(float(r["final_score"]), bool(r[target])) for r in matched if r.get("final_score") is not None],
                key=lambda x: (-x[0], x[1]),
            )
            p5 = precision_at_k(sl_desc, 5)
            p10 = precision_at_k(sl_desc, 10)
            p20 = precision_at_k(sl_desc, 20)

            auc = roc_auc_mann_whitney(
                [(float(r["final_score"]), bool(r[target])) for r in matched if r.get("final_score") is not None]
            )
            pacc = pairwise_accuracy(
                [(float(r["final_score"]), bool(r[target])) for r in matched if r.get("final_score") is not None]
            )

            out[fam][target] = {
                "labeled_row_count": len(all_target),
                "matched_to_ranking_count": len(matched),
                "missing_from_ranking_count": n_miss,
                "positive_count": n_pos,
                "negative_count": n_neg,
                "mean_final_score_positive": statistics.fmean(pos_scores) if pos_scores else None,
                "mean_final_score_negative": statistics.fmean(neg_scores) if neg_scores else None,
                "median_rank_positive": float(statistics.median(pos_ranks)) if pos_ranks else None,
                "median_rank_negative": float(statistics.median(neg_ranks)) if neg_ranks else None,
                "precision_at_5": p5,
                "precision_at_10": p10,
                "precision_at_20": p20,
                "roc_auc_mann_whitney": auc,
                "pairwise_accuracy": pacc,
            }
    return out


CAVEATS = (
    "Offline audit baseline only; not validation of production ranking quality.",
    "Labels are single-reviewer manual audit material unless a source states otherwise.",
    "Rows are biased by ranking outputs and worksheet selection (ranking-selection bias).",
    "This evaluation does not create or imply train/dev/test splits.",
)


def markdown_from_ml_offline_baseline_eval(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    meta = payload["metrics"]["by_family"]
    lines = [
        "# Offline label baseline evaluation",
        "",
        "Read-only join of **ml-label-dataset** rows to persisted **`paper_scores`** for one explicit `ranking_run_id`. "
        "Metrics are **label-aware** (manual `good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial` only) "
        "and stratified by **recommendation family**. No model training and no database writes.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **ranking_version:** `{prov.get('ranking_version')}`",
        f"- **corpus_snapshot_version:** `{prov.get('corpus_snapshot_version')}`",
        f"- **embedding_version:** `{prov.get('embedding_version')}`",
        f"- **cluster_version:** `{prov.get('cluster_version')}`",
        f"- **label_dataset_path:** `{prov.get('label_dataset_path')}`",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        f"- **generated_at:** `{prov.get('generated_at')}`",
        "",
        "## Join summary",
        "",
        f"- **Label rows (audit_only, run match, after row_id dedupe):** {payload['join_summary']['label_rows_included']}",
        f"- **Duplicate row_id rows skipped:** {payload['join_summary']['duplicate_row_id_skipped']}",
        f"- **Joined to paper_scores:** {payload['join_summary']['joined_count']}",
        f"- **Missing from ranking (no score row for family/work):** {payload['join_summary']['missing_score_join_count']}",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in CAVEATS],
        "",
        "- **Duplicate labeled observations** for the same `paper_id` remain separate rows when `row_id` differs; metrics treat each joined row independently.",
        "",
        "## Metrics (by family and target)",
        "",
        "See JSON `metrics.by_family` for `good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial`, "
        "including precision@k (k=5,10,20 when at least k matched labeled rows exist), ROC AUC (Mann–Whitney rank), and pairwise accuracy.",
        "",
        "### Families present",
        "",
        ", ".join(f"`{k}`" for k in sorted(meta.keys())),
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def build_ml_offline_baseline_eval_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
) -> dict[str, Any]:
    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLOfflineBaselineEvalError(f"label dataset not found: {path}")
    label_sha = sha256_file(path)
    raw_payload = load_label_dataset(path)
    label_version = str(raw_payload.get("dataset_version", ""))

    rows, dup_skip = filter_audit_rows_for_run(raw_payload, ranking_run_id=ranking_run_id)
    run_row = fetch_ranking_run_row(conn, ranking_run_id=ranking_run_id)
    cfg = _parse_config_json(run_row.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""

    score_rows = fetch_paper_scores_with_openalex(conn, ranking_run_id=ranking_run_id)
    by_work, by_wtoken = _build_score_lookups(score_rows)

    joined: list[dict[str, Any]] = []
    missing = 0
    for lab in rows:
        sc = join_label_row_to_score(lab, by_work, by_wtoken)
        merged = {**lab}
        merged["_joined_score"] = bool(sc)
        if sc is None:
            missing += 1
            merged["recommendation_family"] = lab.get("family")
            merged["final_score"] = None
            merged["_rank"] = None
            for f in ("semantic_score", "citation_velocity_score", "topic_growth_score", "bridge_score", "diversity_penalty"):
                merged[f] = None
        else:
            merged.update(
                {
                    "recommendation_family": sc["recommendation_family"],
                    "work_id": sc["work_id"],
                    "semantic_score": sc.get("semantic_score"),
                    "citation_velocity_score": sc.get("citation_velocity_score"),
                    "topic_growth_score": sc.get("topic_growth_score"),
                    "bridge_score": sc.get("bridge_score"),
                    "diversity_penalty": sc.get("diversity_penalty"),
                    "final_score": sc.get("final_score"),
                    "_rank": sc.get("_rank"),
                }
            )
        joined.append(merged)

    metrics = compute_family_target_metrics(joined)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_offline_baseline_eval",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": ranking_run_id,
            "ranking_version": str(run_row.get("ranking_version", "")),
            "corpus_snapshot_version": str(run_row.get("corpus_snapshot_version", "")),
            "embedding_version": str(run_row.get("embedding_version", "")),
            "cluster_version": cluster_version,
            "label_dataset_path": path.as_posix(),
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
        },
        "caveats": list(CAVEATS),
        "join_summary": {
            "label_rows_included": len(rows),
            "duplicate_row_id_skipped": dup_skip,
            "joined_count": sum(1 for r in joined if r.get("_joined_score")),
            "missing_score_join_count": missing,
        },
        "metrics": {
            "by_family": metrics,
        },
        "joined_rows": joined,
    }


def write_ml_offline_baseline_eval(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_offline_baseline_eval_payload(
        conn,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_offline_baseline_eval(payload), encoding="utf-8")
    return payload


def run_ml_offline_baseline_eval_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    ranking_run_id: str,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_offline_baseline_eval(
            conn,
            label_dataset_path=label_dataset_path,
            ranking_run_id=ranking_run_id,
            json_path=output_json,
            markdown_path=markdown_output,
        )
