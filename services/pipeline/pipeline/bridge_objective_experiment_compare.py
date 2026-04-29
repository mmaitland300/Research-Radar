"""Read-only comparison for same-weight, different bridge_eligibility_mode objective experiments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.bridge_weight_experiment_compare import (
    _fetch_top_k_rows,
    _jaccard,
    _load_labeled_baseline_paper_ids,
    _load_run,
)


class BridgeObjectiveExperimentCompareError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def build_bridge_objective_experiment_compare_payload(
    conn: psycopg.Connection,
    *,
    baseline_ranking_run_id: str,
    experiment_ranking_run_id: str,
    k: int,
    baseline_bridge_worksheet_path: Path,
) -> dict[str, Any]:
    if k < 1 or k > 200:
        raise BridgeObjectiveExperimentCompareError("--k must be between 1 and 200", code=2)
    baseline = _load_run(conn, ranking_run_id=baseline_ranking_run_id)
    experiment = _load_run(conn, ranking_run_id=experiment_ranking_run_id)
    if baseline.corpus_snapshot_version != experiment.corpus_snapshot_version:
        raise BridgeObjectiveExperimentCompareError("baseline/experiment corpus snapshot versions differ", code=2)
    if baseline.embedding_version != experiment.embedding_version:
        raise BridgeObjectiveExperimentCompareError("baseline/experiment embedding versions differ", code=2)
    if baseline.cluster_version != experiment.cluster_version:
        raise BridgeObjectiveExperimentCompareError("baseline/experiment cluster versions differ", code=2)
    if baseline.bridge_eligibility_mode == experiment.bridge_eligibility_mode:
        raise BridgeObjectiveExperimentCompareError(
            "baseline/experiment bridge eligibility modes must differ for objective experiment compare",
            code=2,
        )
    if baseline.bridge_weight_for_family_bridge != experiment.bridge_weight_for_family_bridge:
        raise BridgeObjectiveExperimentCompareError(
            "baseline/experiment bridge_weight_for_family_bridge must match (same-weight experiment)",
            code=2,
        )
    if baseline.bridge_weight_for_family_bridge is None:
        raise BridgeObjectiveExperimentCompareError("baseline run missing resolved bridge weight in config", code=2)

    b_bridge_rows = _fetch_top_k_rows(
        conn, ranking_run_id=baseline.ranking_run_id, family="bridge", k=k, bridge_eligible_true_only=False
    )
    e_bridge_rows = _fetch_top_k_rows(
        conn, ranking_run_id=experiment.ranking_run_id, family="bridge", k=k, bridge_eligible_true_only=False
    )
    b_eligible_rows = _fetch_top_k_rows(
        conn, ranking_run_id=baseline.ranking_run_id, family="bridge", k=k, bridge_eligible_true_only=True
    )
    e_eligible_rows = _fetch_top_k_rows(
        conn, ranking_run_id=experiment.ranking_run_id, family="bridge", k=k, bridge_eligible_true_only=True
    )
    b_emerging_rows = _fetch_top_k_rows(
        conn, ranking_run_id=baseline.ranking_run_id, family="emerging", k=k, bridge_eligible_true_only=False
    )
    e_emerging_rows = _fetch_top_k_rows(
        conn, ranking_run_id=experiment.ranking_run_id, family="emerging", k=k, bridge_eligible_true_only=False
    )
    b_undercited_rows = _fetch_top_k_rows(
        conn, ranking_run_id=baseline.ranking_run_id, family="undercited", k=k, bridge_eligible_true_only=False
    )
    e_undercited_rows = _fetch_top_k_rows(
        conn, ranking_run_id=experiment.ranking_run_id, family="undercited", k=k, bridge_eligible_true_only=False
    )

    b_bridge_ids = [int(r["work_id"]) for r in b_bridge_rows]
    e_bridge_ids = [int(r["work_id"]) for r in e_bridge_rows]
    b_eligible_ids = [int(r["work_id"]) for r in b_eligible_rows]
    e_eligible_ids = [int(r["work_id"]) for r in e_eligible_rows]
    b_emerging_ids = [int(r["work_id"]) for r in b_emerging_rows]
    e_emerging_ids = [int(r["work_id"]) for r in e_emerging_rows]
    b_undercited_ids = [int(r["work_id"]) for r in b_undercited_rows]
    e_undercited_ids = [int(r["work_id"]) for r in e_undercited_rows]

    b_bridge_rank = {wid: i + 1 for i, wid in enumerate(b_bridge_ids)}
    e_bridge_rank = {wid: i + 1 for i, wid in enumerate(e_bridge_ids)}
    shared_bridge_ids = sorted(set(b_bridge_ids) & set(e_bridge_ids))
    rank_movement = [
        {
            "work_id": wid,
            "baseline_rank": b_bridge_rank[wid],
            "experiment_rank": e_bridge_rank[wid],
            "rank_delta": b_bridge_rank[wid] - e_bridge_rank[wid],
        }
        for wid in shared_bridge_ids
    ]
    new_bridge_ids = sorted(set(e_bridge_ids) - set(b_bridge_ids))
    dropped_bridge_ids = sorted(set(b_bridge_ids) - set(e_bridge_ids))

    labeled_paper_ids = _load_labeled_baseline_paper_ids(baseline_bridge_worksheet_path)
    baseline_eligible_paper_ids = {str(row.get("paper_id") or "") for row in b_eligible_rows}
    unlabeled_experiment_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(e_eligible_rows, start=1):
        pid = str(row.get("paper_id") or "")
        if pid not in baseline_eligible_paper_ids and pid not in labeled_paper_ids:
            unlabeled_experiment_rows.append(
                {
                    "rank": idx,
                    "work_id": int(row["work_id"]),
                    "paper_id": pid or None,
                    "title": row.get("title"),
                }
            )

    b_eligible_vs_emerging = _jaccard(b_eligible_ids, b_emerging_ids)
    e_eligible_vs_emerging = _jaccard(e_eligible_ids, e_emerging_ids)
    distinctness_delta = round(
        float(e_eligible_vs_emerging["jaccard"]) - float(b_eligible_vs_emerging["jaccard"]),
        6,
    )

    same_stack = {
        "same_corpus_snapshot_version": baseline.corpus_snapshot_version == experiment.corpus_snapshot_version,
        "same_embedding_version": baseline.embedding_version == experiment.embedding_version,
        "same_cluster_version": baseline.cluster_version == experiment.cluster_version,
        "same_bridge_weight_for_family_bridge": baseline.bridge_weight_for_family_bridge
        == experiment.bridge_weight_for_family_bridge,
        "bridge_eligibility_modes_differ": baseline.bridge_eligibility_mode != experiment.bridge_eligibility_mode,
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "bridge_objective_experiment_compare",
        "provenance": {
            "baseline": baseline.__dict__,
            "experiment": experiment.__dict__,
            "k": k,
            "baseline_bridge_worksheet_path": str(baseline_bridge_worksheet_path),
        },
        "same_stack_check": same_stack,
        "bridge_top_k_comparison": {
            "baseline_full_bridge_top_k_ids": b_bridge_ids,
            "experiment_full_bridge_top_k_ids": e_bridge_ids,
            "baseline_eligible_bridge_top_k_ids": b_eligible_ids,
            "experiment_eligible_bridge_top_k_ids": e_eligible_ids,
            "full_bridge_overlap": _jaccard(b_bridge_ids, e_bridge_ids),
            "eligible_bridge_overlap": _jaccard(b_eligible_ids, e_eligible_ids),
            "rank_movement_for_shared_full_bridge": rank_movement,
            "new_full_bridge_work_ids": new_bridge_ids,
            "dropped_full_bridge_work_ids": dropped_bridge_ids,
        },
        "emerging_comparison": {
            "baseline_top_k_ids": b_emerging_ids,
            "experiment_top_k_ids": e_emerging_ids,
            "overlap": _jaccard(b_emerging_ids, e_emerging_ids),
            "changed": b_emerging_ids != e_emerging_ids,
        },
        "undercited_comparison": {
            "baseline_top_k_ids": b_undercited_ids,
            "experiment_top_k_ids": e_undercited_ids,
            "overlap": _jaccard(b_undercited_ids, e_undercited_ids),
            "changed": b_undercited_ids != e_undercited_ids,
        },
        "distinctness": {
            "baseline_eligible_bridge_vs_emerging_jaccard": b_eligible_vs_emerging["jaccard"],
            "experiment_eligible_bridge_vs_emerging_jaccard": e_eligible_vs_emerging["jaccard"],
            "delta_experiment_minus_baseline": distinctness_delta,
        },
        "quality_risk": {
            "experiment_eligible_top_k_not_in_labeled_baseline_rows": unlabeled_experiment_rows,
            "unlabeled_experiment_eligible_top_k_count": len(unlabeled_experiment_rows),
            "unlabeled_new_experiment_eligible_top_k_rows": unlabeled_experiment_rows,
            "unlabeled_new_experiment_eligible_top_k_count": len(unlabeled_experiment_rows),
        },
        "decision": {
            "candidate_for_labeling": bool(unlabeled_experiment_rows),
            "ready_for_default": False,
        },
        "caveats": [
            "Objective eligibility experiment: not validation.",
            "Do not change defaults from this artifact alone.",
        ],
    }


def markdown_from_bridge_objective_experiment_compare(payload: dict[str, Any]) -> str:
    prov = payload.get("provenance", {})
    b = prov.get("baseline", {})
    e = prov.get("experiment", {})
    same = payload.get("same_stack_check", {})
    bridge = payload.get("bridge_top_k_comparison", {})
    emerg = payload.get("emerging_comparison", {})
    under = payload.get("undercited_comparison", {})
    dist = payload.get("distinctness", {})
    qr = payload.get("quality_risk", {})
    dec = payload.get("decision", {})
    lines = [
        "# Bridge objective experiment comparison",
        "",
        "Same bridge weight; different `bridge_eligibility_mode`. Diagnostic only, not validation.",
        "",
        "## Provenance",
        "",
        f"- baseline ranking_run_id: `{b.get('ranking_run_id')}`",
        f"- baseline bridge_eligibility_mode: `{b.get('bridge_eligibility_mode')}`",
        f"- experiment ranking_run_id: `{e.get('ranking_run_id')}`",
        f"- experiment bridge_eligibility_mode: `{e.get('bridge_eligibility_mode')}`",
        "",
        "## Same-stack check",
        "",
        f"- same_corpus_snapshot_version: `{same.get('same_corpus_snapshot_version')}`",
        f"- same_embedding_version: `{same.get('same_embedding_version')}`",
        f"- same_cluster_version: `{same.get('same_cluster_version')}`",
        f"- same_bridge_weight_for_family_bridge: `{same.get('same_bridge_weight_for_family_bridge')}`",
        f"- bridge_eligibility_modes_differ: `{same.get('bridge_eligibility_modes_differ')}`",
        "",
        "## Distinctness",
        "",
        f"- baseline eligible bridge vs emerging jaccard: `{dist.get('baseline_eligible_bridge_vs_emerging_jaccard')}`",
        f"- experiment eligible bridge vs emerging jaccard: `{dist.get('experiment_eligible_bridge_vs_emerging_jaccard')}`",
        f"- delta (experiment - baseline): `{dist.get('delta_experiment_minus_baseline')}`",
        "",
        "## Labeling",
        "",
        f"- unlabeled_experiment_eligible_top_k_count: `{qr.get('unlabeled_experiment_eligible_top_k_count')}`",
        f"- candidate_for_labeling: `{dec.get('candidate_for_labeling')}`",
        f"- ready_for_default: `{dec.get('ready_for_default')}`",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_objective_experiment_compare(
    *,
    baseline_ranking_run_id: str,
    experiment_ranking_run_id: str,
    k: int,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None,
    baseline_bridge_worksheet_path: Path,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_bridge_objective_experiment_compare_payload(
            conn,
            baseline_ranking_run_id=baseline_ranking_run_id,
            experiment_ranking_run_id=experiment_ranking_run_id,
            k=k,
            baseline_bridge_worksheet_path=baseline_bridge_worksheet_path,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_bridge_objective_experiment_compare(payload), encoding="utf-8", newline="\n"
        )
    return payload


__all__ = [
    "BridgeObjectiveExperimentCompareError",
    "build_bridge_objective_experiment_compare_payload",
    "markdown_from_bridge_objective_experiment_compare",
    "run_bridge_objective_experiment_compare",
]
