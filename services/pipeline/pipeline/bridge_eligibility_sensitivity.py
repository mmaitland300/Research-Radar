"""Read-only threshold sweep for bridge eligibility sensitivity on one ranking run."""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.bridge_experiment_readiness import fetch_top_k_work_ids, overlap_count_and_jaccard
from pipeline.clustering_persistence import load_cluster_assignments
from pipeline.recommendation_review_worksheet import cluster_version_from_config


class BridgeEligibilitySensitivityError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _BridgeRow:
    work_id: int
    final_score: float
    bridge_score: float | None
    bridge_eligible: bool | None
    signal_raw: Any


@dataclass(frozen=True)
class _SignalMetrics:
    signal_version: str | None
    eligible: bool | None
    mix_score: float | None
    cross_cluster_neighbor_share: float | None
    same_cluster_neighbor_share: float | None
    distinct_neighbor_cluster_count: int | None


def _parse_json(raw: Any) -> dict[str, Any]:
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


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    return None


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
        raise BridgeEligibilitySensitivityError(f"ranking_run_id not found: {ranking_run_id!r}", code=2)
    if str(row["status"]) != "succeeded":
        raise BridgeEligibilitySensitivityError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={row['status']!r}).",
            code=2,
        )
    return dict(row)


def _load_bridge_rows(conn: psycopg.Connection, *, ranking_run_id: str) -> list[_BridgeRow]:
    rows = conn.execute(
        """
        SELECT work_id, final_score, bridge_score, bridge_eligible, bridge_signal_json
        FROM paper_scores
        WHERE ranking_run_id = %s
          AND recommendation_family = 'bridge'
        ORDER BY final_score DESC, work_id ASC
        """,
        (ranking_run_id,),
    ).fetchall()
    out: list[_BridgeRow] = []
    for r in rows:
        out.append(
            _BridgeRow(
                work_id=int(r["work_id"]),
                final_score=float(r["final_score"]),
                bridge_score=float(r["bridge_score"]) if r["bridge_score"] is not None else None,
                bridge_eligible=_as_bool(r["bridge_eligible"]),
                signal_raw=r["bridge_signal_json"],
            )
        )
    return out


def _signal_metrics_for_row(
    *,
    signal: dict[str, Any],
    work_id: int,
    cluster_by_work: dict[int, str],
) -> _SignalMetrics:
    signal_version = signal.get("signal_version") if isinstance(signal.get("signal_version"), str) else None
    eligible = _as_bool(signal.get("eligible"))
    mix_score = _as_float(signal.get("mix_score"))
    foreign_neighbor_count = _as_int(signal.get("foreign_neighbor_count"))
    k = _as_int(signal.get("k"))
    if mix_score is None and foreign_neighbor_count is not None and k and k > 0:
        mix_score = float(foreign_neighbor_count) / float(k)

    anchor_cluster = signal.get("anchor_cluster_id")
    if not isinstance(anchor_cluster, str) or not anchor_cluster.strip():
        anchor_cluster = cluster_by_work.get(work_id)
    neighbor_ids = signal.get("neighbor_work_ids")
    distinct_neighbor_cluster_count: int | None = None
    same_cluster_neighbor_share: float | None = None
    cross_cluster_neighbor_share: float | None = None
    if isinstance(neighbor_ids, list) and neighbor_ids:
        neighbor_clusters: list[str] = []
        same_count = 0
        valid_count = 0
        for nid in neighbor_ids:
            if not isinstance(nid, int):
                continue
            ncluster = cluster_by_work.get(int(nid))
            if ncluster is None:
                continue
            valid_count += 1
            neighbor_clusters.append(ncluster)
            if anchor_cluster is not None and ncluster == anchor_cluster:
                same_count += 1
        if valid_count > 0:
            distinct_neighbor_cluster_count = len(set(neighbor_clusters))
            same_cluster_neighbor_share = same_count / float(valid_count)
            cross_cluster_neighbor_share = 1.0 - same_cluster_neighbor_share

    if cross_cluster_neighbor_share is None and mix_score is not None:
        cross_cluster_neighbor_share = mix_score
    if same_cluster_neighbor_share is None and cross_cluster_neighbor_share is not None:
        same_cluster_neighbor_share = 1.0 - cross_cluster_neighbor_share

    return _SignalMetrics(
        signal_version=signal_version,
        eligible=eligible,
        mix_score=mix_score,
        cross_cluster_neighbor_share=cross_cluster_neighbor_share,
        same_cluster_neighbor_share=same_cluster_neighbor_share,
        distinct_neighbor_cluster_count=distinct_neighbor_cluster_count,
    )


def _median_bridge_score(ids: list[int], row_by_work: dict[int, _BridgeRow]) -> float | None:
    vals = [row_by_work[i].bridge_score for i in ids if i in row_by_work and row_by_work[i].bridge_score is not None]
    if not vals:
        return None
    return float(round(statistics.median(vals), 6))


def _top_percent_cutoff(rows: list[_BridgeRow], percent: float) -> float | None:
    scores = sorted((r.bridge_score for r in rows if r.bridge_score is not None), reverse=True)
    if not scores:
        return None
    count = max(1, int(round(len(scores) * percent)))
    return float(scores[count - 1])


def _jaccard(a: list[int], b: list[int]) -> float:
    _, _, j = overlap_count_and_jaccard(a, b)
    return float(j)


def _variant_result(
    *,
    variant_id: str,
    rule: str,
    eligible_ids_total: list[int],
    k: int,
    full_bridge_top_k_ids: list[int],
    emerging_top_k_ids: list[int],
    full_bridge_vs_emerging_jaccard: float,
    row_by_work: dict[int, _BridgeRow],
) -> dict[str, Any]:
    top_k_ids = eligible_ids_total[:k]
    eligible_top_k_count = len(top_k_ids)
    full_vs_variant_j = _jaccard(full_bridge_top_k_ids, top_k_ids)
    variant_vs_emerging_j = _jaccard(top_k_ids, emerging_top_k_ids)
    overlap_with_emerging = len(set(top_k_ids) & set(emerging_top_k_ids))
    delta = round(full_bridge_vs_emerging_jaccard - variant_vs_emerging_j, 6)
    bridge_only_count = len(set(top_k_ids) - set(emerging_top_k_ids))
    distinctness_improves = delta >= 0.10
    candidate_for_zero_weight_rerun = (
        eligible_top_k_count >= k
        and distinctness_improves
        and len(eligible_ids_total) >= max(30, k * 2)
    )
    warning = None
    if eligible_top_k_count < k:
        warning = f"eligible_top_k_count {eligible_top_k_count} is below k={k}."
    return {
        "variant_id": variant_id,
        "rule": rule,
        "eligible_count_total": len(eligible_ids_total),
        "eligible_share_total": round(len(eligible_ids_total) / max(1, len(row_by_work)), 6),
        "eligible_top_k_ids": top_k_ids,
        "eligible_top_k_count": eligible_top_k_count,
        "full_bridge_vs_variant_jaccard": full_vs_variant_j,
        "variant_vs_emerging_jaccard": variant_vs_emerging_j,
        "overlap_count_with_emerging_top_k": overlap_with_emerging,
        "emerging_overlap_delta_vs_full_bridge": delta,
        "bridge_only_count_vs_emerging": bridge_only_count,
        "median_bridge_score": _median_bridge_score(eligible_ids_total, row_by_work),
        "distinctness_improves": distinctness_improves,
        "candidate_for_zero_weight_rerun": candidate_for_zero_weight_rerun,
        "warning": warning,
    }


def _variant_filtered_ids(
    rows: list[_BridgeRow],
    *,
    predicate: Callable[[_BridgeRow], bool],
) -> list[int]:
    return [r.work_id for r in rows if predicate(r)]


def build_bridge_eligibility_sensitivity_payload(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    k: int,
) -> dict[str, Any]:
    rid = str(ranking_run_id or "").strip()
    if not rid:
        raise BridgeEligibilitySensitivityError("--ranking-run-id is required and must not be blank", code=2)
    if k < 1 or k > 200:
        raise BridgeEligibilitySensitivityError("--k must be between 1 and 200", code=2)

    run = _load_run(conn, ranking_run_id=rid)
    bridge_rows = _load_bridge_rows(conn, ranking_run_id=rid)
    if not bridge_rows:
        raise BridgeEligibilitySensitivityError(f"No bridge-family rows found for ranking_run_id={rid!r}.", code=2)

    cfg = _parse_json(run.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""
    cluster_by_work: dict[int, str] = {}
    warnings: list[str] = []
    if cluster_version:
        cluster_by_work = load_cluster_assignments(conn, cluster_version=cluster_version)
        if not cluster_by_work:
            warnings.append(
                f"No cluster assignments found for cluster_version={cluster_version!r}; neighbor-cluster variants may be sparse."
            )
    else:
        warnings.append("cluster_version missing from ranking run config; neighbor-cluster variants may be sparse.")

    full_bridge_top_k_ids = [r.work_id for r in bridge_rows[:k]]
    emerging_top_k_ids = fetch_top_k_work_ids(
        conn, ranking_run_id=rid, family="emerging", k=k, bridge_eligible_true_only=False
    )
    full_bridge_vs_emerging_jaccard = _jaccard(full_bridge_top_k_ids, emerging_top_k_ids)

    signal_by_work: dict[int, _SignalMetrics] = {}
    field_cov = {
        "signal_version": 0,
        "eligible": 0,
        "mix_score": 0,
        "foreign_neighbor_count": 0,
        "neighbor_work_ids": 0,
        "anchor_cluster_id": 0,
        "distinct_neighbor_cluster_count": 0,
        "same_cluster_neighbor_share": 0,
        "cross_cluster_neighbor_share": 0,
    }
    for row in bridge_rows:
        signal = _parse_json(row.signal_raw)
        if not signal:
            continue
        for key in ("signal_version", "eligible", "mix_score", "foreign_neighbor_count", "neighbor_work_ids", "anchor_cluster_id"):
            if key in signal and signal.get(key) is not None:
                field_cov[key] += 1
        metrics = _signal_metrics_for_row(signal=signal, work_id=row.work_id, cluster_by_work=cluster_by_work)
        signal_by_work[row.work_id] = metrics
        if metrics.distinct_neighbor_cluster_count is not None:
            field_cov["distinct_neighbor_cluster_count"] += 1
        if metrics.same_cluster_neighbor_share is not None:
            field_cov["same_cluster_neighbor_share"] += 1
        if metrics.cross_cluster_neighbor_share is not None:
            field_cov["cross_cluster_neighbor_share"] += 1

    row_by_work = {r.work_id: r for r in bridge_rows}
    score_75 = _top_percent_cutoff(bridge_rows, 0.75)
    score_50 = _top_percent_cutoff(bridge_rows, 0.50)
    score_25 = _top_percent_cutoff(bridge_rows, 0.25)

    variants: list[dict[str, Any]] = []
    variants.append(
        _variant_result(
            variant_id="existing_bridge_eligible",
            rule="paper_scores.bridge_eligible IS TRUE",
            eligible_ids_total=_variant_filtered_ids(bridge_rows, predicate=lambda r: r.bridge_eligible is True),
            k=k,
            full_bridge_top_k_ids=full_bridge_top_k_ids,
            emerging_top_k_ids=emerging_top_k_ids,
            full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
            row_by_work=row_by_work,
        )
    )
    if score_75 is not None:
        variants.append(
            _variant_result(
                variant_id="bridge_score_top_75pct",
                rule=f"bridge_score >= {score_75}",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows, predicate=lambda r: r.bridge_score is not None and float(r.bridge_score) >= score_75
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )
    if score_50 is not None:
        variants.append(
            _variant_result(
                variant_id="bridge_score_top_50pct",
                rule=f"bridge_score >= {score_50}",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows, predicate=lambda r: r.bridge_score is not None and float(r.bridge_score) >= score_50
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )
    if score_25 is not None:
        variants.append(
            _variant_result(
                variant_id="bridge_score_top_25pct",
                rule=f"bridge_score >= {score_25}",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows, predicate=lambda r: r.bridge_score is not None and float(r.bridge_score) >= score_25
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )

    def _sig(wid: int) -> _SignalMetrics | None:
        return signal_by_work.get(wid)

    for n in (2, 3):
        variants.append(
            _variant_result(
                variant_id=f"distinct_neighbor_clusters_gte_{n}",
                rule=f"distinct_neighbor_cluster_count >= {n}",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows,
                    predicate=lambda r, n=n: (
                        _sig(r.work_id) is not None
                        and _sig(r.work_id).distinct_neighbor_cluster_count is not None
                        and int(_sig(r.work_id).distinct_neighbor_cluster_count) >= n
                    ),
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )
    for x in (0.75, 0.60, 0.50):
        variants.append(
            _variant_result(
                variant_id=f"same_cluster_share_lte_{str(x).replace('.', '_')}",
                rule=f"same_cluster_neighbor_share <= {x}",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows,
                    predicate=lambda r, x=x: (
                        _sig(r.work_id) is not None
                        and _sig(r.work_id).same_cluster_neighbor_share is not None
                        and float(_sig(r.work_id).same_cluster_neighbor_share) <= x
                    ),
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )
    for x in (0.25, 0.40, 0.50):
        variants.append(
            _variant_result(
                variant_id=f"cross_cluster_share_gte_{str(x).replace('.', '_')}",
                rule=f"cross_cluster_neighbor_share >= {x}",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows,
                    predicate=lambda r, x=x: (
                        _sig(r.work_id) is not None
                        and _sig(r.work_id).cross_cluster_neighbor_share is not None
                        and float(_sig(r.work_id).cross_cluster_neighbor_share) >= x
                    ),
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )

    if score_50 is not None:
        variants.append(
            _variant_result(
                variant_id="top50_and_cross_cluster_gte_0_40",
                rule=f"bridge_score >= {score_50} AND cross_cluster_neighbor_share >= 0.40",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows,
                    predicate=lambda r: (
                        r.bridge_score is not None
                        and float(r.bridge_score) >= score_50
                        and _sig(r.work_id) is not None
                        and _sig(r.work_id).cross_cluster_neighbor_share is not None
                        and float(_sig(r.work_id).cross_cluster_neighbor_share) >= 0.40
                    ),
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )
        variants.append(
            _variant_result(
                variant_id="top50_and_same_cluster_lte_0_60",
                rule=f"bridge_score >= {score_50} AND same_cluster_neighbor_share <= 0.60",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows,
                    predicate=lambda r: (
                        r.bridge_score is not None
                        and float(r.bridge_score) >= score_50
                        and _sig(r.work_id) is not None
                        and _sig(r.work_id).same_cluster_neighbor_share is not None
                        and float(_sig(r.work_id).same_cluster_neighbor_share) <= 0.60
                    ),
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )
    if score_25 is not None:
        variants.append(
            _variant_result(
                variant_id="top25_and_cross_cluster_gte_0_40",
                rule=f"bridge_score >= {score_25} AND cross_cluster_neighbor_share >= 0.40",
                eligible_ids_total=_variant_filtered_ids(
                    bridge_rows,
                    predicate=lambda r: (
                        r.bridge_score is not None
                        and float(r.bridge_score) >= score_25
                        and _sig(r.work_id) is not None
                        and _sig(r.work_id).cross_cluster_neighbor_share is not None
                        and float(_sig(r.work_id).cross_cluster_neighbor_share) >= 0.40
                    ),
                ),
                k=k,
                full_bridge_top_k_ids=full_bridge_top_k_ids,
                emerging_top_k_ids=emerging_top_k_ids,
                full_bridge_vs_emerging_jaccard=full_bridge_vs_emerging_jaccard,
                row_by_work=row_by_work,
            )
        )

    if field_cov["cross_cluster_neighbor_share"] == 0 and field_cov["mix_score"] == 0:
        warnings.append("No cross-cluster share signal fields resolved; threshold variants may be uninformative.")
    if field_cov["distinct_neighbor_cluster_count"] == 0:
        warnings.append("No distinct-neighbor-cluster counts resolved from signal + cluster assignments.")

    mapped_fields = {
        "cross_cluster_neighbor_share": "mix_score (fallback) or 1 - same_cluster_neighbor_share",
        "same_cluster_neighbor_share": "derived from anchor_cluster_id + neighbor_work_ids + clusters table",
        "distinct_neighbor_cluster_count": "derived from neighbor_work_ids + clusters table",
    }

    any_candidate = any(bool(v.get("candidate_for_zero_weight_rerun")) for v in variants)
    any_improve = any(bool(v.get("distinctness_improves")) for v in variants)
    if any_candidate:
        recommended_next_step = "rerun_zero_bridge_with_candidate_threshold"
    elif field_cov["mix_score"] == 0 and field_cov["distinct_neighbor_cluster_count"] == 0:
        recommended_next_step = "inspect_bridge_signal_schema"
    elif any_improve:
        recommended_next_step = "tighten_bridge_eligibility"
    else:
        recommended_next_step = "keep_current_eligibility_and_label"

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": {
            "ranking_run_id": str(run["ranking_run_id"]),
            "ranking_version": str(run["ranking_version"]),
            "corpus_snapshot_version": str(run["corpus_snapshot_version"]),
            "embedding_version": str(run["embedding_version"]),
            "cluster_version": cluster_version,
            "k": k,
        },
        "baseline": {
            "full_bridge_vs_emerging_jaccard": full_bridge_vs_emerging_jaccard,
            "full_bridge_top_k_ids": full_bridge_top_k_ids,
            "emerging_top_k_ids": emerging_top_k_ids,
        },
        "signal_field_coverage": {
            "bridge_row_count": len(bridge_rows),
            "field_present_count": field_cov,
            "field_mapping_used": mapped_fields,
        },
        "variants": variants,
        "recommended_next_step": recommended_next_step,
        "warnings": warnings,
    }


def markdown_from_sensitivity(payload: dict[str, Any]) -> str:
    prov = payload.get("provenance", {})
    baseline = payload.get("baseline", {})
    cov = payload.get("signal_field_coverage", {})
    lines = [
        "# Bridge eligibility sensitivity",
        "",
        "Read-only diagnostic artifact; this does **not** validate ranking quality and does **not** validate bridge behavior.",
        "No bridge weights were changed.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **ranking_version:** `{prov.get('ranking_version')}`",
        f"- **corpus_snapshot_version:** `{prov.get('corpus_snapshot_version')}`",
        f"- **embedding_version:** `{prov.get('embedding_version')}`",
        f"- **cluster_version:** `{prov.get('cluster_version')}`",
        f"- **k:** `{prov.get('k')}`",
        "",
        "## Baseline overlap",
        "",
        f"- **full_bridge_vs_emerging_jaccard:** `{baseline.get('full_bridge_vs_emerging_jaccard')}`",
        f"- **full_bridge_top_k_ids:** `{baseline.get('full_bridge_top_k_ids')}`",
        f"- **emerging_top_k_ids:** `{baseline.get('emerging_top_k_ids')}`",
        "",
        "## Signal field coverage",
        "",
        f"- **field_present_count:** `{json.dumps(cov.get('field_present_count') or {}, sort_keys=True)}`",
        f"- **field_mapping_used:** `{json.dumps(cov.get('field_mapping_used') or {}, sort_keys=True)}`",
        "",
        "## Variant comparison",
        "",
        "| variant_id | eligible_total | eligible_top_k | variant_vs_emerging_jaccard | delta_vs_full | distinctness_improves | candidate_for_zero_weight_rerun |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for v in payload.get("variants") or []:
        lines.append(
            f"| {v.get('variant_id')} | {v.get('eligible_count_total')} | {v.get('eligible_top_k_count')} | "
            f"{v.get('variant_vs_emerging_jaccard')} | {v.get('emerging_overlap_delta_vs_full_bridge')} | "
            f"{v.get('distinctness_improves')} | {v.get('candidate_for_zero_weight_rerun')} |"
        )
    lines.extend(
        [
            "",
            "## Recommended next step",
            "",
            f"- `{payload.get('recommended_next_step')}`",
            "",
            "> Caveat: read-only diagnostic, not ranking validation, not bridge validation.",
            "> Caveat: no bridge weights changed.",
            "",
        ]
    )
    warnings = payload.get("warnings") or []
    if warnings:
        lines.append("## Warnings")
        lines.append("")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_eligibility_sensitivity(
    *,
    ranking_run_id: str,
    k: int,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None = None,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id=ranking_run_id, k=k)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_sensitivity(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeEligibilitySensitivityError",
    "build_bridge_eligibility_sensitivity_payload",
    "markdown_from_sensitivity",
    "run_bridge_eligibility_sensitivity",
]
