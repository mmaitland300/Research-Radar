"""Deterministic bridge signal diagnostics for one explicit ranking_run_id (read-only)."""

from __future__ import annotations

import json
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.bridge_experiment_readiness import fetch_top_k_work_ids, overlap_count_and_jaccard
from pipeline.clustering_persistence import load_cluster_assignments
from pipeline.recommendation_review_worksheet import cluster_version_from_config


class BridgeSignalDiagnosticsError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _load_run(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise BridgeSignalDiagnosticsError(f"ranking_run_id not found: {ranking_run_id!r}", code=2)
    if str(row["status"]) != "succeeded":
        raise BridgeSignalDiagnosticsError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={row['status']!r}).",
            code=2,
        )
    return dict(row)


def _bridge_coverage(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*)::bigint AS bridge_family_row_count,
            COUNT(*) FILTER (WHERE ps.bridge_score IS NOT NULL)::bigint AS bridge_score_nonnull_count,
            COUNT(*) FILTER (WHERE ps.bridge_score IS NULL)::bigint AS bridge_score_null_count,
            COUNT(*) FILTER (WHERE ps.bridge_eligible IS TRUE)::bigint AS bridge_eligible_true_count,
            COUNT(*) FILTER (WHERE ps.bridge_eligible IS FALSE)::bigint AS bridge_eligible_false_count,
            COUNT(*) FILTER (WHERE ps.bridge_eligible IS NULL)::bigint AS bridge_eligible_null_count,
            COUNT(*) FILTER (WHERE ps.bridge_signal_json IS NOT NULL)::bigint
                AS bridge_signal_json_present_count,
            COUNT(*) FILTER (WHERE ps.bridge_signal_json IS NULL)::bigint
                AS bridge_signal_json_missing_count
        FROM paper_scores ps
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'bridge'
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise BridgeSignalDiagnosticsError("bridge coverage query returned no row", code=2)
    return {k: int(row[k]) for k in row.keys()}


def _bridge_scores_all(conn: psycopg.Connection, *, ranking_run_id: str) -> list[float | None]:
    rows = conn.execute(
        """
        SELECT ps.bridge_score
        FROM paper_scores ps
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'bridge'
        ORDER BY ps.work_id ASC
        """,
        (ranking_run_id,),
    ).fetchall()
    out: list[float | None] = []
    for r in rows:
        v = r["bridge_score"]
        if v is None:
            out.append(None)
        else:
            out.append(float(v))
    return out


def _score_distribution_stats(scores: list[float | None]) -> dict[str, Any]:
    non_null = [s for s in scores if s is not None]
    null_count = len(scores) - len(non_null)
    if not non_null:
        return {
            "min_bridge_score": None,
            "max_bridge_score": None,
            "mean_bridge_score": None,
            "median_bridge_score": None,
            "unique_bridge_score_count": 0,
            "null_bridge_score_count": null_count,
        }
    uniq = len(set(non_null))
    return {
        "min_bridge_score": min(non_null),
        "max_bridge_score": max(non_null),
        "mean_bridge_score": round(statistics.mean(non_null), 9),
        "median_bridge_score": float(statistics.median(non_null)),
        "unique_bridge_score_count": uniq,
        "null_bridge_score_count": null_count,
    }


def _coerce_signal(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return dict(parsed) if isinstance(parsed, dict) else None
    return None


def summarize_bridge_signal_json(sig: Any) -> dict[str, Any] | None:
    """Stable subset for JSON rows; never the full raw blob for Markdown."""
    d = _coerce_signal(sig)
    if d is None:
        return None
    sv = d.get("signal_version")
    base: dict[str, Any] = {"signal_version": sv if isinstance(sv, str) else str(sv)}
    if sv != "neighbor_mix_v1":
        keys = [k for k in d.keys() if isinstance(k, str)]
        base["top_level_keys_sample"] = sorted(keys)[:25]
        return base
    base["eligible"] = d.get("eligible")
    base["k"] = d.get("k")
    if "mix_score" in d:
        base["mix_score"] = d.get("mix_score")
    if "foreign_neighbor_count" in d:
        base["foreign_neighbor_count"] = d.get("foreign_neighbor_count")
    if "anchor_cluster_id" in d:
        base["anchor_cluster_id"] = d.get("anchor_cluster_id")
    nids = d.get("neighbor_work_ids")
    if isinstance(nids, list):
        base["neighbor_work_id_count"] = len(nids)
    return base


def _top_k_bridge_rows(
    conn: psycopg.Connection, *, ranking_run_id: str, k: int
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            ps.work_id,
            w.openalex_id AS paper_id,
            w.title,
            ps.final_score,
            ps.semantic_score,
            ps.citation_velocity_score,
            ps.topic_growth_score,
            ps.bridge_score,
            ps.diversity_penalty,
            ps.bridge_eligible,
            ps.bridge_signal_json
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'bridge'
        ORDER BY ps.final_score DESC, ps.work_id ASC
        LIMIT %s
        """,
        (ranking_run_id, k),
    ).fetchall()
    return [dict(r) for r in rows]


def _pick_suggested_next_step(
    *,
    flags: dict[str, bool],
    cluster_warnings: list[str],
) -> str:
    if flags["bridge_signal_details_missing_or_sparse"]:
        return "repair_bridge_signal_generation"
    if flags["eligibility_filter_not_selective_at_head"]:
        return "tighten_bridge_eligibility_thresholds"
    if cluster_warnings:
        return "inspect_cluster_assignments"
    if flags["bridge_head_emerging_overlap_high"]:
        return "increase_bridge_score_weight_only_after_new_distinctness_run"
    if flags["bridge_score_has_low_variance"]:
        return "inspect_cluster_assignments"
    return "expand_corpus_before_bridge_tuning"


def build_bridge_signal_diagnostics_payload(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    k: int,
) -> dict[str, Any]:
    rid = str(ranking_run_id).strip()
    if not rid:
        raise BridgeSignalDiagnosticsError("--ranking-run-id is required and must not be blank", code=2)
    if k < 1 or k > 200:
        raise BridgeSignalDiagnosticsError("--k must be between 1 and 200", code=2)

    run = _load_run(conn, ranking_run_id=rid)
    cfg = _parse_config_json(run.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""

    coverage = _bridge_coverage(conn, ranking_run_id=rid)
    all_scores = _bridge_scores_all(conn, ranking_run_id=rid)

    full_top_ids = fetch_top_k_work_ids(conn, ranking_run_id=rid, family="bridge", k=k, bridge_eligible_true_only=False)
    eligible_top_ids = fetch_top_k_work_ids(
        conn, ranking_run_id=rid, family="bridge", k=k, bridge_eligible_true_only=True
    )
    emerging_top_ids = fetch_top_k_work_ids(conn, ranking_run_id=rid, family="emerging", k=k, bridge_eligible_true_only=False)
    under_top_ids = fetch_top_k_work_ids(conn, ranking_run_id=rid, family="undercited", k=k, bridge_eligible_true_only=False)

    raw_rows = _top_k_bridge_rows(conn, ranking_run_id=rid, k=k)

    elig_true = sum(1 for r in raw_rows if r.get("bridge_eligible") is True)
    elig_false = sum(1 for r in raw_rows if r.get("bridge_eligible") is False)
    elig_null = sum(1 for r in raw_rows if r.get("bridge_eligible") is None)

    top_k_scores = [float(r["bridge_score"]) if r.get("bridge_score") is not None else None for r in raw_rows]

    set_full = set(full_top_ids)
    set_eligible = set(eligible_top_ids)
    set_em = set(emerging_top_ids)
    set_under = set(under_top_ids)

    inter_be = set_full & set_em
    bridge_only = sorted(set_full - set_em)
    emerging_only = sorted(set_em - set_full)
    overlap_count = len(inter_be)
    _o, _u, jaccard_be = overlap_count_and_jaccard(full_top_ids, emerging_top_ids)
    eligible_inter_be = set_eligible & set_em
    eligible_overlap_count = len(eligible_inter_be)
    _eo, _eu, eligible_jaccard_be = overlap_count_and_jaccard(eligible_top_ids, emerging_top_ids)
    full_vs_eligible_overlap = len(set_full & set_eligible)
    _fo, _fu, full_vs_eligible_jaccard = overlap_count_and_jaccard(full_top_ids, eligible_top_ids)
    overlap_delta = round(float(jaccard_be) - float(eligible_jaccard_be), 6)

    warnings: list[str] = []
    if coverage["bridge_signal_json_missing_count"] > 0:
        warnings.append(
            f"{coverage['bridge_signal_json_missing_count']} bridge-family rows have NULL bridge_signal_json "
            "(signal not persisted for those works)."
        )
    if cluster_version == "":
        warnings.append("cluster_version is empty on ranking run config; cluster-pair diagnostics from DB are skipped.")

    cluster_diag_warnings: list[str] = []
    cluster_by_work: dict[int, str] = {}
    if cluster_version:
        cluster_by_work = load_cluster_assignments(conn, cluster_version=cluster_version)
        if not cluster_by_work:
            cluster_diag_warnings.append(
                f"No rows in `clusters` for cluster_version={cluster_version!r}; "
                "neighbor-cluster resolution for diagnostics is unavailable."
            )

    pair_counter: Counter[tuple[str, str]] = Counter()
    anchor_clusters_top_k: set[str] = set()
    neighbor_clusters_top_k: set[str] = set()
    rows_missing_detail = 0

    for r in raw_rows:
        wid = int(r["work_id"])
        sig = _coerce_signal(r.get("bridge_signal_json"))
        if sig is None:
            rows_missing_detail += 1
            continue
        if sig.get("signal_version") != "neighbor_mix_v1":
            rows_missing_detail += 1
            continue
        anchor = sig.get("anchor_cluster_id")
        if isinstance(anchor, str) and anchor.strip():
            anchor_clusters_top_k.add(anchor)
        elif wid in cluster_by_work:
            anchor_clusters_top_k.add(cluster_by_work[wid])
        nids = sig.get("neighbor_work_ids")
        if not isinstance(nids, list) or not nids:
            if sig.get("eligible") is True:
                rows_missing_detail += 1
            continue
        aclus = anchor if isinstance(anchor, str) and anchor.strip() else cluster_by_work.get(wid)
        if aclus is None:
            rows_missing_detail += 1
            continue
        for nid in nids:
            if not isinstance(nid, int):
                continue
            nc = cluster_by_work.get(int(nid))
            if nc is None:
                rows_missing_detail += 1
                continue
            neighbor_clusters_top_k.add(nc)
            pair_counter[(str(aclus), str(nc))] += 1

    top_pairs = [
        {"source_cluster": a, "target_cluster": b, "count": c}
        for (a, b), c in pair_counter.most_common(15)
    ]

    top_k_missing_json = sum(1 for r in raw_rows if _coerce_signal(r.get("bridge_signal_json")) is None)

    bridge_signal_sparse = (
        coverage["bridge_signal_json_missing_count"] > 0
        or top_k_missing_json > 0
        or rows_missing_detail > max(1, k // 2)
    )

    score_stats_all = _score_distribution_stats(all_scores)
    score_stats_topk = _score_distribution_stats(top_k_scores)

    non_null_topk = [s for s in top_k_scores if s is not None]
    score_range = 0.0
    if len(non_null_topk) >= 2:
        score_range = max(non_null_topk) - min(non_null_topk)
    low_variance = score_stats_topk["unique_bridge_score_count"] <= 3 or (
        len(non_null_topk) >= 2 and score_range <= 1e-9
    )

    full_equals_eligible = full_top_ids == eligible_top_ids
    eligibility_not_selective = bool(full_equals_eligible)
    overlap_high = jaccard_be >= 0.50
    eligible_less_emerging_like = eligible_jaccard_be < jaccard_be
    eligible_distinctness_improves = overlap_delta >= 0.10

    diagnosis_flags = {
        "eligibility_filter_not_selective_at_head": eligibility_not_selective,
        "bridge_score_has_low_variance": low_variance,
        "bridge_head_emerging_overlap_high": overlap_high,
        "bridge_signal_details_missing_or_sparse": bridge_signal_sparse,
        "eligible_head_differs_from_full": not full_equals_eligible,
        "eligible_head_less_emerging_like_than_full": eligible_less_emerging_like,
        "eligible_distinctness_improves_by_threshold": eligible_distinctness_improves,
    }

    if cluster_diag_warnings:
        warnings.extend(cluster_diag_warnings)

    suggested = _pick_suggested_next_step(
        flags=diagnosis_flags,
        cluster_warnings=cluster_diag_warnings,
    )

    compact_rows: list[dict[str, Any]] = []
    for i, r in enumerate(raw_rows, start=1):
        wid = int(r["work_id"])
        compact_rows.append(
            {
                "rank": i,
                "work_id": wid,
                "paper_id": str(r["paper_id"]) if r.get("paper_id") is not None else None,
                "title": str(r["title"]) if r.get("title") is not None else None,
                "final_score": float(r["final_score"]) if r.get("final_score") is not None else None,
                "semantic_score": float(r["semantic_score"]) if r.get("semantic_score") is not None else None,
                "citation_velocity_score": float(r["citation_velocity_score"])
                if r.get("citation_velocity_score") is not None
                else None,
                "topic_growth_score": float(r["topic_growth_score"]) if r.get("topic_growth_score") is not None else None,
                "bridge_score": float(r["bridge_score"]) if r.get("bridge_score") is not None else None,
                "diversity_penalty": float(r["diversity_penalty"]) if r.get("diversity_penalty") is not None else None,
                "bridge_eligible": r.get("bridge_eligible"),
                "in_emerging_top_k": wid in set_em,
                "in_undercited_top_k": wid in set_under,
                "bridge_signal_summary": summarize_bridge_signal_json(r.get("bridge_signal_json")),
            }
        )

    provenance = {
        "ranking_run_id": str(run["ranking_run_id"]),
        "ranking_version": str(run["ranking_version"]),
        "corpus_snapshot_version": str(run["corpus_snapshot_version"]),
        "embedding_version": str(run["embedding_version"]),
        "cluster_version": cluster_version,
        "k": k,
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": provenance,
        "bridge_row_coverage": coverage,
        "head_eligibility": {
            "full_bridge_top_k_eligible_true_count": elig_true,
            "full_bridge_top_k_eligible_false_count": elig_false,
            "full_bridge_top_k_eligible_null_count": elig_null,
            "eligible_only_bridge_top_k_count": len(eligible_top_ids),
            "full_bridge_equals_eligible_only_bridge_top_k": full_equals_eligible,
        },
        "score_distribution": {
            "all_bridge_rows": score_stats_all,
            "bridge_top_k": score_stats_topk,
        },
        "bridge_top_k_rows": compact_rows,
        "overlap_detail": {
            "bridge_vs_emerging_jaccard": jaccard_be,
            "bridge_top_k_overlap_with_emerging_count": overlap_count,
            "bridge_top_k_only_count": len(bridge_only),
            "emerging_top_k_only_count": len(emerging_only),
            "bridge_only_work_ids": bridge_only,
            "emerging_only_work_ids": emerging_only,
            "shared_bridge_emerging_work_ids": sorted(inter_be),
            "eligible_bridge_top_k_ids": eligible_top_ids,
            "eligible_bridge_vs_emerging_overlap_count": eligible_overlap_count,
            "eligible_bridge_vs_emerging_jaccard": eligible_jaccard_be,
            "full_bridge_vs_eligible_bridge_overlap_count": full_vs_eligible_overlap,
            "full_bridge_vs_eligible_bridge_jaccard": full_vs_eligible_jaccard,
            "emerging_overlap_delta_from_full_to_eligible": overlap_delta,
        },
        "cluster_signal_diagnostics": {
            "distinct_anchor_cluster_count_in_bridge_top_k": len(anchor_clusters_top_k),
            "distinct_neighbor_cluster_count_in_bridge_top_k": len(neighbor_clusters_top_k),
            "top_cluster_pairs": top_pairs,
            "rows_missing_cluster_or_signal_details": rows_missing_detail,
            "warnings": list(cluster_diag_warnings),
        },
        "diagnosis": diagnosis_flags,
        "suggested_next_step": suggested,
        "warnings": warnings,
    }


def markdown_from_diagnostics(payload: dict[str, Any]) -> str:
    prov = payload.get("provenance", {})
    cov = payload.get("bridge_row_coverage", {})
    head = payload.get("head_eligibility", {})
    dist = payload.get("score_distribution", {})
    ov = payload.get("overlap_detail", {})
    diag = payload.get("diagnosis", {})
    csd = payload.get("cluster_signal_diagnostics", {})
    lines: list[str] = [
        "# Bridge signal diagnostics",
        "",
        "Diagnostic only: this report does **not** validate bridge ranking and does **not** prove ML ranking is better.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id', '')}`",
        f"- **ranking_version:** `{prov.get('ranking_version', '')}`",
        f"- **corpus_snapshot_version:** `{prov.get('corpus_snapshot_version', '')}`",
        f"- **embedding_version:** `{prov.get('embedding_version', '')}`",
        f"- **cluster_version:** `{prov.get('cluster_version', '')}`",
        f"- **k:** `{prov.get('k', '')}`",
        "",
        "## Key blocker summary",
        "",
        f"- **Full bridge top-k equals eligible-only top-k:** `{head.get('full_bridge_equals_eligible_only_bridge_top_k')}` "
        f"(eligibility filter not selective at head when true).",
        f"- **Full bridge vs emerging Jaccard (top-k work_id sets):** `{ov.get('bridge_vs_emerging_jaccard')}`",
        f"- **Eligible-only bridge vs emerging Jaccard (top-k work_id sets):** "
        f"`{ov.get('eligible_bridge_vs_emerging_jaccard')}`",
        f"- **Emerging overlap delta (full - eligible):** `{ov.get('emerging_overlap_delta_from_full_to_eligible')}`",
        f"- **High emerging overlap (Jaccard ≥ 0.50):** `{diag.get('bridge_head_emerging_overlap_high')}`",
        f"- **Low bridge_score variance in top-k:** `{diag.get('bridge_score_has_low_variance')}`",
        f"- **Signal details missing or sparse:** `{diag.get('bridge_signal_details_missing_or_sparse')}`",
        "",
        "## Bridge eligibility coverage (all bridge-family rows)",
        "",
        f"- **bridge_family_row_count:** `{cov.get('bridge_family_row_count')}`",
        f"- **bridge_eligible_true_count:** `{cov.get('bridge_eligible_true_count')}`",
        f"- **bridge_eligible_false_count:** `{cov.get('bridge_eligible_false_count')}`",
        f"- **bridge_eligible_null_count:** `{cov.get('bridge_eligible_null_count')}`",
        f"- **bridge_score_nonnull_count:** `{cov.get('bridge_score_nonnull_count')}`",
        f"- **bridge_score_null_count:** `{cov.get('bridge_score_null_count')}`",
        f"- **bridge_signal_json_present_count:** `{cov.get('bridge_signal_json_present_count')}`",
        f"- **bridge_signal_json_missing_count:** `{cov.get('bridge_signal_json_missing_count')}`",
        "",
        "## Score distribution",
        "",
        "### All bridge rows",
        "",
    ]
    ab = dist.get("all_bridge_rows") or {}
    lines.extend(
        [
            f"- **min / max / mean / median:** `{ab.get('min_bridge_score')}` / `{ab.get('max_bridge_score')}` / "
            f"`{ab.get('mean_bridge_score')}` / `{ab.get('median_bridge_score')}`",
            f"- **unique_bridge_score_count:** `{ab.get('unique_bridge_score_count')}`",
            f"- **null_bridge_score_count:** `{ab.get('null_bridge_score_count')}`",
            "",
            "### Bridge top-k",
            "",
        ]
    )
    bk = dist.get("bridge_top_k") or {}
    lines.extend(
        [
            f"- **min / max / mean / median:** `{bk.get('min_bridge_score')}` / `{bk.get('max_bridge_score')}` / "
            f"`{bk.get('mean_bridge_score')}` / `{bk.get('median_bridge_score')}`",
            f"- **unique_bridge_score_count:** `{bk.get('unique_bridge_score_count')}`",
            f"- **null_bridge_score_count:** `{bk.get('null_bridge_score_count')}`",
            "",
            "## Overlap detail (top-k)",
            "",
            f"- **Full bridge overlap count (bridge ∩ emerging):** `{ov.get('bridge_top_k_overlap_with_emerging_count')}`",
            f"- **Full bridge vs emerging Jaccard:** `{ov.get('bridge_vs_emerging_jaccard')}`",
            f"- **Eligible-only bridge overlap count (bridge_eligible=true ∩ emerging):** "
            f"`{ov.get('eligible_bridge_vs_emerging_overlap_count')}`",
            f"- **Eligible-only bridge vs emerging Jaccard:** `{ov.get('eligible_bridge_vs_emerging_jaccard')}`",
            f"- **Full vs eligible bridge overlap count:** `{ov.get('full_bridge_vs_eligible_bridge_overlap_count')}`",
            f"- **Full vs eligible bridge Jaccard:** `{ov.get('full_bridge_vs_eligible_bridge_jaccard')}`",
            f"- **Emerging overlap delta (full - eligible):** `{ov.get('emerging_overlap_delta_from_full_to_eligible')}`",
            f"- **Bridge-only count:** `{ov.get('bridge_top_k_only_count')}`",
            f"- **Emerging-only count:** `{ov.get('emerging_top_k_only_count')}`",
            "",
            "## Compact bridge top-k (scores + overlap flags)",
            "",
            "| rank | work_id | in_em | in_uc | eligible | signal (summary) | final | sem | cite | topic | bridge | div |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in payload.get("bridge_top_k_rows") or []:
        summ = row.get("bridge_signal_summary")
        summ_s = ""
        if isinstance(summ, dict):
            parts = [str(summ.get("signal_version", ""))]
            if "eligible" in summ:
                parts.append(f"elig={summ.get('eligible')}")
            if summ.get("mix_score") is not None:
                parts.append(f"mix={summ.get('mix_score')}")
            if summ.get("neighbor_work_id_count") is not None:
                parts.append(f"n={summ.get('neighbor_work_id_count')}")
            summ_s = "; ".join(parts)
        lines.append(
            f"| {row.get('rank')} | {row.get('work_id')} | {row.get('in_emerging_top_k')} | "
            f"{row.get('in_undercited_top_k')} | {row.get('bridge_eligible')} | `{summ_s}` | "
            f"{row.get('final_score')} | {row.get('semantic_score')} | {row.get('citation_velocity_score')} | "
            f"{row.get('topic_growth_score')} | {row.get('bridge_score')} | {row.get('diversity_penalty')} |"
        )
    lines.extend(
        [
            "",
            "Titles omitted from the table width; see JSON `bridge_top_k_rows` for `paper_id` and full `title`. "
            "Raw `bridge_signal_json` is not inlined in Markdown.",
            "",
            "## Cluster / signal diagnostics (bridge top-k)",
            "",
            f"- **Distinct anchor clusters:** `{csd.get('distinct_anchor_cluster_count_in_bridge_top_k')}`",
            f"- **Distinct neighbor clusters (from neighbor lists × clusters table):** "
            f"`{csd.get('distinct_neighbor_cluster_count_in_bridge_top_k')}`",
            f"- **Rows missing cluster/signal resolution:** `{csd.get('rows_missing_cluster_or_signal_details')}`",
            "",
            "### Top cluster pairs (anchor → neighbor)",
            "",
        ]
    )
    for p in csd.get("top_cluster_pairs") or []:
        lines.append(f"- `{p.get('source_cluster')}` → `{p.get('target_cluster')}`: **{p.get('count')}**")
    if not (csd.get("top_cluster_pairs") or []):
        lines.append("- *(none resolved)*")
    lines.extend(
        [
            "",
            "## Diagnosis flags (hints only)",
            "",
            f"- **eligibility_filter_not_selective_at_head:** `{diag.get('eligibility_filter_not_selective_at_head')}`",
            f"- **bridge_score_has_low_variance:** `{diag.get('bridge_score_has_low_variance')}`",
            f"- **bridge_head_emerging_overlap_high:** `{diag.get('bridge_head_emerging_overlap_high')}`",
            f"- **bridge_signal_details_missing_or_sparse:** `{diag.get('bridge_signal_details_missing_or_sparse')}`",
            f"- **eligible_head_differs_from_full:** `{diag.get('eligible_head_differs_from_full')}`",
            f"- **eligible_head_less_emerging_like_than_full:** "
            f"`{diag.get('eligible_head_less_emerging_like_than_full')}`",
            f"- **eligible_distinctness_improves_by_threshold (delta >= 0.10):** "
            f"`{diag.get('eligible_distinctness_improves_by_threshold')}`",
            "",
            "## Suggested next step",
            "",
            f"- `{payload.get('suggested_next_step', '')}`",
            "",
            "## Limitations",
            "",
            "- Operator-facing diagnostic only; not statistical validation.",
            "- Does not change ranking weights or persisted signals.",
            "",
        ]
    )
    for w in payload.get("warnings") or []:
        lines.append(f"- Warning: {w}")
    if payload.get("warnings"):
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_signal_diagnostics(
    *,
    ranking_run_id: str,
    k: int,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_bridge_signal_diagnostics_payload(conn, ranking_run_id=ranking_run_id, k=k)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_diagnostics(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeSignalDiagnosticsError",
    "build_bridge_signal_diagnostics_payload",
    "markdown_from_diagnostics",
    "run_bridge_signal_diagnostics",
    "summarize_bridge_signal_json",
]
