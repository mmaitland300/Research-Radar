"""Read-only comparison between baseline and experiment bridge-weight ranking runs."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.recommendation_review_worksheet import cluster_version_from_config


class BridgeWeightExperimentCompareError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _RunProvenance:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    cluster_version: str
    bridge_eligibility_mode: str
    bridge_weight_for_family_bridge: float | None
    status: str


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


def _load_run(conn: psycopg.Connection, *, ranking_run_id: str) -> _RunProvenance:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise BridgeWeightExperimentCompareError(f"ranking_run_id not found: {ranking_run_id!r}", code=2)
    status = str(row["status"])
    if status != "succeeded":
        raise BridgeWeightExperimentCompareError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={status!r}).",
            code=2,
        )
    cfg = _parse_config_json(row.get("config_json"))
    bridge_mode = cfg.get("bridge_eligibility_mode")
    if not isinstance(bridge_mode, str) or not bridge_mode.strip():
        raise BridgeWeightExperimentCompareError(
            f"ranking run {ranking_run_id!r} missing config_json.bridge_eligibility_mode.",
            code=2,
        )
    bridge_weight: float | None = None
    fam = cfg.get("family_weights")
    if isinstance(fam, dict):
        bridge_fam = fam.get("bridge")
        if isinstance(bridge_fam, dict):
            raw_weight = bridge_fam.get("bridge")
            if isinstance(raw_weight, (int, float)):
                bridge_weight = float(raw_weight)
    if bridge_weight is None:
        cl = cfg.get("clustering_artifact")
        if isinstance(cl, dict):
            raw = cl.get("bridge_weight_in_final_score")
            if isinstance(raw, (int, float)):
                bridge_weight = float(raw)
    return _RunProvenance(
        ranking_run_id=str(row["ranking_run_id"]),
        ranking_version=str(row["ranking_version"]),
        corpus_snapshot_version=str(row["corpus_snapshot_version"]),
        embedding_version=str(row["embedding_version"]),
        cluster_version=cluster_version_from_config(cfg) or "",
        bridge_eligibility_mode=bridge_mode.strip(),
        bridge_weight_for_family_bridge=bridge_weight,
        status=status,
    )


def _fetch_top_k_rows(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    family: str,
    k: int,
    bridge_eligible_true_only: bool,
) -> list[dict[str, Any]]:
    if bridge_eligible_true_only and family != "bridge":
        raise BridgeWeightExperimentCompareError(
            "bridge_eligible_true_only applies only to family='bridge'.",
            code=2,
        )
    elig = "AND ps.bridge_eligible IS TRUE" if bridge_eligible_true_only else ""
    rows = conn.execute(
        f"""
        SELECT
            ps.work_id,
            w.openalex_id AS paper_id,
            w.title,
            ps.final_score
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = %s
          {elig}
        ORDER BY ps.final_score DESC, ps.work_id ASC
        LIMIT %s
        """,
        (ranking_run_id, family, k),
    ).fetchall()
    return [dict(r) for r in rows]


def _jaccard(ids_a: list[int], ids_b: list[int]) -> dict[str, Any]:
    set_a = set(ids_a)
    set_b = set(ids_b)
    inter = set_a & set_b
    union = set_a | set_b
    j = 1.0 if not union else round(len(inter) / len(union), 6)
    return {"overlap_count": len(inter), "union_count": len(union), "jaccard": float(j)}


def _default_baseline_worksheet_path(*, baseline_ranking_run_id: str) -> Path:
    repo_root = Path(__file__).resolve().parents[3]
    return (
        repo_root
        / "docs"
        / "audit"
        / "manual-review"
        / f"bridge_eligible_{baseline_ranking_run_id}_top20.csv"
    )


def _load_labeled_baseline_paper_ids(path: Path) -> set[str]:
    if not path.is_file():
        raise BridgeWeightExperimentCompareError(f"Baseline bridge worksheet file not found: {path}", code=2)
    out: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise BridgeWeightExperimentCompareError(f"Baseline bridge worksheet CSV has no header: {path}", code=2)
        if "paper_id" not in reader.fieldnames:
            raise BridgeWeightExperimentCompareError(
                f"Baseline bridge worksheet CSV missing paper_id column: {path}",
                code=2,
            )
        for row in reader:
            pid = (row.get("paper_id") or "").strip()
            if pid:
                out.add(pid)
    return out


def build_bridge_weight_experiment_compare_payload(
    conn: psycopg.Connection,
    *,
    baseline_ranking_run_id: str,
    experiment_ranking_run_id: str,
    k: int,
    baseline_bridge_worksheet_path: Path,
) -> dict[str, Any]:
    if k < 1 or k > 200:
        raise BridgeWeightExperimentCompareError("--k must be between 1 and 200", code=2)
    baseline = _load_run(conn, ranking_run_id=baseline_ranking_run_id)
    experiment = _load_run(conn, ranking_run_id=experiment_ranking_run_id)
    if baseline.corpus_snapshot_version != experiment.corpus_snapshot_version:
        raise BridgeWeightExperimentCompareError("baseline/experiment corpus snapshot versions differ", code=2)
    if baseline.embedding_version != experiment.embedding_version:
        raise BridgeWeightExperimentCompareError("baseline/experiment embedding versions differ", code=2)
    if baseline.cluster_version != experiment.cluster_version:
        raise BridgeWeightExperimentCompareError("baseline/experiment cluster versions differ", code=2)
    if baseline.bridge_eligibility_mode != experiment.bridge_eligibility_mode:
        raise BridgeWeightExperimentCompareError("baseline/experiment bridge eligibility modes differ", code=2)
    if baseline.bridge_weight_for_family_bridge == experiment.bridge_weight_for_family_bridge:
        raise BridgeWeightExperimentCompareError("baseline/experiment bridge weights must differ", code=2)

    b_bridge_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=baseline.ranking_run_id,
        family="bridge",
        k=k,
        bridge_eligible_true_only=False,
    )
    e_bridge_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=experiment.ranking_run_id,
        family="bridge",
        k=k,
        bridge_eligible_true_only=False,
    )
    b_eligible_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=baseline.ranking_run_id,
        family="bridge",
        k=k,
        bridge_eligible_true_only=True,
    )
    e_eligible_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=experiment.ranking_run_id,
        family="bridge",
        k=k,
        bridge_eligible_true_only=True,
    )
    b_emerging_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=baseline.ranking_run_id,
        family="emerging",
        k=k,
        bridge_eligible_true_only=False,
    )
    e_emerging_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=experiment.ranking_run_id,
        family="emerging",
        k=k,
        bridge_eligible_true_only=False,
    )
    b_undercited_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=baseline.ranking_run_id,
        family="undercited",
        k=k,
        bridge_eligible_true_only=False,
    )
    e_undercited_rows = _fetch_top_k_rows(
        conn,
        ranking_run_id=experiment.ranking_run_id,
        family="undercited",
        k=k,
        bridge_eligible_true_only=False,
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

    baseline_labeled_paper_ids = _load_labeled_baseline_paper_ids(baseline_bridge_worksheet_path)
    unlabeled_experiment_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(e_eligible_rows, start=1):
        pid = str(row.get("paper_id") or "")
        if pid not in baseline_labeled_paper_ids:
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
        "same_bridge_eligibility_mode": baseline.bridge_eligibility_mode == experiment.bridge_eligibility_mode,
        "only_bridge_weight_differs": baseline.bridge_weight_for_family_bridge
        != experiment.bridge_weight_for_family_bridge,
    }

    changed_eligible_head = b_eligible_ids != e_eligible_ids
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
        },
        "decision": {
            "candidate_for_labeling": changed_eligible_head,
            "candidate_for_weight_increase": False,
            "ready_for_default": False,
        },
        "caveats": [
            "This is a ranking movement experiment, not validation.",
            "Do not make positive bridge weight the default until the moved/new rows are reviewed.",
        ],
    }


def markdown_from_bridge_weight_experiment_compare(payload: dict[str, Any]) -> str:
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
        "# Bridge weight experiment comparison",
        "",
        "This is a ranking movement experiment, not validation.",
        "Do not make positive bridge weight the default until the moved/new rows are reviewed.",
        "",
        "## Provenance",
        "",
        f"- baseline ranking_run_id: `{b.get('ranking_run_id')}`",
        f"- baseline ranking_version: `{b.get('ranking_version')}`",
        f"- baseline bridge_weight_for_family_bridge: `{b.get('bridge_weight_for_family_bridge')}`",
        f"- experiment ranking_run_id: `{e.get('ranking_run_id')}`",
        f"- experiment ranking_version: `{e.get('ranking_version')}`",
        f"- experiment bridge_weight_for_family_bridge: `{e.get('bridge_weight_for_family_bridge')}`",
        "",
        "## Same-stack check",
        "",
        f"- same_corpus_snapshot_version: `{same.get('same_corpus_snapshot_version')}`",
        f"- same_embedding_version: `{same.get('same_embedding_version')}`",
        f"- same_cluster_version: `{same.get('same_cluster_version')}`",
        f"- same_bridge_eligibility_mode: `{same.get('same_bridge_eligibility_mode')}`",
        f"- only_bridge_weight_differs: `{same.get('only_bridge_weight_differs')}`",
        "",
        "## Bridge top-k movement",
        "",
        f"- full_bridge_overlap_jaccard: `{(bridge.get('full_bridge_overlap') or {}).get('jaccard')}`",
        f"- eligible_bridge_overlap_jaccard: `{(bridge.get('eligible_bridge_overlap') or {}).get('jaccard')}`",
        f"- new_full_bridge_work_ids: `{bridge.get('new_full_bridge_work_ids')}`",
        f"- dropped_full_bridge_work_ids: `{bridge.get('dropped_full_bridge_work_ids')}`",
        "",
        "## Family stability checks",
        "",
        f"- emerging_changed: `{emerg.get('changed')}` (jaccard `{(emerg.get('overlap') or {}).get('jaccard')}`)",
        f"- undercited_changed: `{under.get('changed')}` (jaccard `{(under.get('overlap') or {}).get('jaccard')}`)",
        "",
        "## Distinctness",
        "",
        f"- baseline eligible bridge vs emerging jaccard: `{dist.get('baseline_eligible_bridge_vs_emerging_jaccard')}`",
        f"- experiment eligible bridge vs emerging jaccard: `{dist.get('experiment_eligible_bridge_vs_emerging_jaccard')}`",
        f"- delta (experiment - baseline): `{dist.get('delta_experiment_minus_baseline')}`",
        "",
        "## Labeling risk and decision",
        "",
        f"- unlabeled_experiment_eligible_top_k_count: `{qr.get('unlabeled_experiment_eligible_top_k_count')}`",
        f"- candidate_for_labeling: `{dec.get('candidate_for_labeling')}`",
        f"- candidate_for_weight_increase: `{dec.get('candidate_for_weight_increase')}`",
        f"- ready_for_default: `{dec.get('ready_for_default')}`",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_weight_experiment_compare(
    *,
    baseline_ranking_run_id: str,
    experiment_ranking_run_id: str,
    k: int,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None,
    baseline_bridge_worksheet_path: Path | None = None,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    worksheet = baseline_bridge_worksheet_path or _default_baseline_worksheet_path(
        baseline_ranking_run_id=baseline_ranking_run_id
    )
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_bridge_weight_experiment_compare_payload(
            conn,
            baseline_ranking_run_id=baseline_ranking_run_id,
            experiment_ranking_run_id=experiment_ranking_run_id,
            k=k,
            baseline_bridge_worksheet_path=worksheet,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_bridge_weight_experiment_compare(payload), encoding="utf-8", newline="\n"
        )
    return payload


__all__ = [
    "BridgeWeightExperimentCompareError",
    "build_bridge_weight_experiment_compare_payload",
    "markdown_from_bridge_weight_experiment_compare",
    "run_bridge_weight_experiment_compare",
]
