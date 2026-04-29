"""Read-only simulation of alternative bridge objective rules (no ranking writes)."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.bridge_experiment_readiness import fetch_top_k_work_ids, overlap_count_and_jaccard


class BridgeObjectiveRedesignSimulationError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BridgeSimRow:
    work_id: int
    paper_id: str
    final_score: float
    bridge_score: float | None
    bridge_eligible: bool
    emerging_final_score: float


def load_persistent_ids_from_failure_json(path: Path) -> list[int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    raw = data.get("persistent_shared_with_emerging_ids") or []
    return [int(x) for x in raw]


def load_baseline_eligible_top_k_from_sensitivity(path: Path) -> list[int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    for v in data.get("variants") or []:
        if v.get("variant_id") == "existing_bridge_eligible":
            return [int(x) for x in v.get("eligible_top_k_ids") or []]
    raise BridgeObjectiveRedesignSimulationError(
        f"existing_bridge_eligible variant missing in {path}", code=2
    )


def load_emerging_top_k_from_sensitivity(path: Path) -> list[int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [int(x) for x in (data.get("baseline") or {}).get("emerging_top_k_ids") or []]


def load_baseline_jaccard_from_sensitivity(path: Path) -> float:
    data = json.loads(path.read_text(encoding="utf-8"))
    for v in data.get("variants") or []:
        if v.get("variant_id") == "existing_bridge_eligible":
            return float(v["variant_vs_emerging_jaccard"])
    raise BridgeObjectiveRedesignSimulationError("baseline jaccard not found in sensitivity JSON", code=2)


def load_labeled_paper_ids_from_worksheet_csv(path: Path) -> set[str]:
    """Paper IDs considered labeled for coverage (non-empty relevance or bridge_like columns)."""
    out: set[str] = set()
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rel = (row.get("relevance_label") or "").strip()
            bl = (row.get("bridge_like_label") or "").strip()
            pid = (row.get("paper_id") or "").strip()
            if not pid:
                continue
            if rel or bl:
                out.add(pid)
    return out


def _fetch_bridge_family_rows(conn: psycopg.Connection, *, ranking_run_id: str) -> list[BridgeSimRow]:
    em_scores: dict[int, float] = {}
    em_rows = conn.execute(
        """
        SELECT ps.work_id, ps.final_score
        FROM paper_scores ps
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'emerging'
        """,
        (ranking_run_id,),
    ).fetchall()
    for r in em_rows:
        wid = int(r["work_id"])
        fs = r.get("final_score")
        em_scores[wid] = float(fs) if fs is not None else 0.0

    rows = conn.execute(
        """
        SELECT
            ps.work_id,
            w.openalex_id AS paper_id,
            ps.final_score,
            ps.bridge_score,
            ps.bridge_eligible
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'bridge'
        ORDER BY ps.final_score DESC, ps.work_id ASC
        """,
        (ranking_run_id,),
    ).fetchall()
    out: list[BridgeSimRow] = []
    for r in rows:
        wid = int(r["work_id"])
        pid = str(r["paper_id"] or "")
        fs = r.get("final_score")
        bs = r.get("bridge_score")
        elig = r.get("bridge_eligible")
        out.append(
            BridgeSimRow(
                work_id=wid,
                paper_id=pid,
                final_score=float(fs) if fs is not None else 0.0,
                bridge_score=float(bs) if bs is not None else None,
                bridge_eligible=bool(elig) if elig is not None else False,
                emerging_final_score=float(em_scores.get(wid, 0.0)),
            )
        )
    return out


def _eligible_count_total(rows: Iterable[BridgeSimRow], *, pred: Callable[[BridgeSimRow], bool]) -> int:
    return sum(1 for r in rows if pred(r))


def _take_eligible_top_20(
    rows_global: list[BridgeSimRow],
    *,
    pool_pred: Callable[[BridgeSimRow], bool],
    sort_key: Callable[[BridgeSimRow], tuple[Any, ...]] | None,
) -> list[BridgeSimRow]:
    if sort_key is not None:
        pool = [r for r in rows_global if pool_pred(r)]
        pool = sorted(pool, key=sort_key)
        return pool[:20]
    out: list[BridgeSimRow] = []
    for r in rows_global:
        if pool_pred(r):
            out.append(r)
            if len(out) == 20:
                break
    return out


def _residual_key(lam: float) -> Callable[[BridgeSimRow], tuple[float, int]]:
    def key(r: BridgeSimRow) -> tuple[float, int]:
        b = float(r.bridge_score) if r.bridge_score is not None else 0.0
        score = b - lam * float(r.emerging_final_score)
        return (-score, r.work_id)

    return key


def _jaccard_vs_emerging(top: list[BridgeSimRow], emerging_top_k: list[int]) -> float:
    ids = [r.work_id for r in top]
    _o, _u, j = overlap_count_and_jaccard(ids, emerging_top_k)
    return float(j)


def _overlap_with_emerging(top: list[BridgeSimRow], emerging_set: set[int]) -> list[int]:
    return sorted({r.work_id for r in top if r.work_id in emerging_set})


def _compute_variant(
    *,
    variant_id: str,
    variant_type: str,
    rows_global: list[BridgeSimRow],
    emerging_top_k: list[int],
    emerging_top_50: set[int],
    persistent_ids: set[int],
    baseline_eligible_top20_ids: set[int],
    labeled_paper_ids: set[str],
    current_jaccard: float,
    pool_pred: Callable[[BridgeSimRow], bool],
    sort_key: Callable[[BridgeSimRow], tuple[Any, ...]] | None,
    hard_exclusion: bool,
) -> dict[str, Any]:
    n_eligible_total = _eligible_count_total(rows_global, pred=pool_pred)
    top20 = _take_eligible_top_20(rows_global, pool_pred=pool_pred, sort_key=sort_key)
    eligible_top_20_ids = [r.work_id for r in top20]
    eligible_top_20_count = len(top20)
    em_set = set(emerging_top_k)
    jacc = _jaccard_vs_emerging(top20, emerging_top_k)
    overlap_ids = _overlap_with_emerging(top20, em_set)
    delta = round(float(jacc) - float(current_jaccard), 6)

    persistent_removed = sorted(persistent_ids & baseline_eligible_top20_ids - set(eligible_top_20_ids))
    persistent_retained = sorted(persistent_ids & set(eligible_top_20_ids))

    already_labeled = sum(1 for r in top20 if r.paper_id in labeled_paper_ids)
    new_unlabeled = eligible_top_20_count - already_labeled

    cohort_risk = n_eligible_total < 50
    too_blunt = (eligible_top_20_count < 20) or (hard_exclusion and n_eligible_total < 30)

    candidate = (
        (jacc < current_jaccard)
        and (eligible_top_20_count == 20)
        and (n_eligible_total >= 50)
        and (new_unlabeled <= 10)
        and (not too_blunt)
        and (not cohort_risk)
    )

    return {
        "variant_id": variant_id,
        "variant_type": variant_type,
        "eligible_count_total": n_eligible_total,
        "eligible_top_20_count": eligible_top_20_count,
        "eligible_top_20_ids": eligible_top_20_ids,
        "eligible_bridge_vs_emerging_jaccard": jacc,
        "delta_vs_current_eligible_jaccard": delta,
        "overlap_ids_with_emerging_top_20": overlap_ids,
        "persistent_ids_removed": persistent_removed,
        "persistent_ids_retained": persistent_retained,
        "already_labeled_top20_count": already_labeled,
        "new_unlabeled_top20_count": new_unlabeled,
        "candidate_for_zero_weight_rerun": candidate,
        "too_blunt": too_blunt,
        "cohort_risk": cohort_risk,
    }


def build_simulation_payload(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    k: int,
    sensitivity_json_path: Path,
    failure_analysis_json_path: Path,
    bridge_worksheet_csv_path: Path,
) -> dict[str, Any]:
    rid = str(ranking_run_id).strip()
    if not rid:
        raise BridgeObjectiveRedesignSimulationError("ranking_run_id is required", code=2)
    if k != 20:
        raise BridgeObjectiveRedesignSimulationError("this simulation is implemented for k=20 only", code=2)

    emerging_top_k = load_emerging_top_k_from_sensitivity(sensitivity_json_path)
    if len(emerging_top_k) != k:
        raise BridgeObjectiveRedesignSimulationError("emerging_top_k length mismatch", code=2)

    current_j = load_baseline_jaccard_from_sensitivity(sensitivity_json_path)
    baseline_top_ids = set(load_baseline_eligible_top_k_from_sensitivity(sensitivity_json_path))
    persistent_list = load_persistent_ids_from_failure_json(failure_analysis_json_path)
    persistent_set = set(persistent_list)
    labeled = load_labeled_paper_ids_from_worksheet_csv(bridge_worksheet_csv_path)

    emerging_top_50_list = fetch_top_k_work_ids(
        conn, ranking_run_id=rid, family="emerging", k=50, bridge_eligible_true_only=False
    )
    emerging_top_50 = set(emerging_top_50_list)

    rows_global = _fetch_bridge_family_rows(conn, ranking_run_id=rid)

    def base_eligible(r: BridgeSimRow) -> bool:
        return r.bridge_eligible

    variants_out: list[dict[str, Any]] = []

    def add(
        variant_id: str,
        variant_type: str,
        pool_pred: Callable[[BridgeSimRow], bool],
        sort_key: Callable[[BridgeSimRow], tuple[Any, ...]] | None,
        *,
        hard_exclusion: bool,
    ) -> None:
        variants_out.append(
            _compute_variant(
                variant_id=variant_id,
                variant_type=variant_type,
                rows_global=rows_global,
                emerging_top_k=emerging_top_k,
                emerging_top_50=emerging_top_50,
                persistent_ids=persistent_set,
                baseline_eligible_top20_ids=baseline_top_ids,
                labeled_paper_ids=labeled,
                current_jaccard=current_j,
                pool_pred=pool_pred,
                sort_key=sort_key,
                hard_exclusion=hard_exclusion,
            )
        )

    add(
        "existing_bridge_eligible_baseline",
        "baseline",
        base_eligible,
        None,
        hard_exclusion=False,
    )

    em20 = set(emerging_top_k)

    add(
        "eligible_exclude_emerging_top_20",
        "hard_exclusion",
        lambda r: base_eligible(r) and r.work_id not in em20,
        None,
        hard_exclusion=True,
    )

    add(
        "eligible_exclude_emerging_top_50",
        "hard_exclusion",
        lambda r: base_eligible(r) and r.work_id not in emerging_top_50,
        None,
        hard_exclusion=True,
    )

    add(
        "eligible_exclude_persistent_shared_with_emerging",
        "hard_exclusion",
        lambda r: base_eligible(r) and r.work_id not in persistent_set,
        None,
        hard_exclusion=True,
    )

    for lam in (0.10, 0.25, 0.50, 1.00):
        add(
            f"residual_bridge_score_minus_{lam:g}_emerging_final_score",
            "residual_penalty",
            base_eligible,
            _residual_key(lam),
            hard_exclusion=False,
        )

    lam_c = 0.25
    add(
        "eligible_and_residual_lambda_0_25",
        "combined",
        base_eligible,
        _residual_key(lam_c),
        hard_exclusion=False,
    )

    add(
        "eligible_exclude_emerging_top_50_and_residual_lambda_0_25",
        "combined",
        lambda r: base_eligible(r) and r.work_id not in emerging_top_50,
        _residual_key(lam_c),
        hard_exclusion=True,
    )

    hard_ok = [v for v in variants_out if v["variant_type"] == "hard_exclusion" and v["eligible_top_20_count"] == 20]
    best_hard = (
        min(
            hard_ok,
            key=lambda v: (float(v["eligible_bridge_vs_emerging_jaccard"]), int(v["new_unlabeled_top20_count"])),
        )
        if hard_ok
        else None
    )
    hard_candidates = [v for v in hard_ok if v["candidate_for_zero_weight_rerun"]]
    best_hard_candidate_review = (
        min(hard_candidates, key=lambda v: int(v["new_unlabeled_top20_count"])) if hard_candidates else None
    )

    best_residual = None
    for v in variants_out:
        if v["variant_type"] == "residual_penalty" and v["eligible_top_20_count"] == 20:
            if best_residual is None or v["eligible_bridge_vs_emerging_jaccard"] < best_residual["eligible_bridge_vs_emerging_jaccard"]:
                best_residual = v

    beats_floor = any(
        float(v["eligible_bridge_vs_emerging_jaccard"]) < current_j
        and int(v["eligible_count_total"]) >= 50
        and int(v["eligible_top_20_count"]) == 20
        for v in variants_out
    )

    candidates = [v for v in variants_out if v["candidate_for_zero_weight_rerun"]]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "bridge_objective_redesign_simulation",
        "ranking_run_id": rid,
        "k": k,
        "inputs": {
            "sensitivity_json": str(sensitivity_json_path.resolve()),
            "failure_analysis_json": str(failure_analysis_json_path.resolve()),
            "bridge_worksheet_csv": str(bridge_worksheet_csv_path.resolve()),
            "persistent_shared_with_emerging_ids": persistent_list,
            "reference_eligible_vs_emerging_jaccard": current_j,
        },
        "variants": variants_out,
        "summary": {
            "best_hard_exclusion_variant_id": best_hard["variant_id"] if best_hard else None,
            "best_hard_exclusion_candidate_lowest_new_unlabeled_variant_id": (
                best_hard_candidate_review["variant_id"] if best_hard_candidate_review else None
            ),
            "best_residual_penalty_variant_id": best_residual["variant_id"] if best_residual else None,
            "any_variant_beats_jaccard_floor_with_full_top20_and_cohort_ge_50": beats_floor,
            "candidate_variant_ids": [v["variant_id"] for v in candidates],
        },
    }


def markdown_from_simulation(payload: dict[str, Any]) -> str:
    lines = [
        "# Bridge objective redesign simulation",
        "",
        "> Diagnostic only; not validation.",
        "> No ranking/default/product claim.",
        "> No DB writes; read-only `SELECT` against `paper_scores` / `works` only.",
        "> No new eligibility rule is selected from this artifact alone.",
        "> Any candidate requires a follow-up zero-weight ranking run and labels before policy change.",
        "",
        f"- **ranking_run_id:** `{payload.get('ranking_run_id')}`",
        f"- **reference Jaccard (existing eligible vs emerging top-{payload.get('k')}):** `{payload.get('inputs', {}).get('reference_eligible_vs_emerging_jaccard')}`",
        "",
        "## Variants",
        "",
        "| variant_id | type | eligible_total | top20 | Jaccard | delta | new_unlabeled | candidate | too_blunt | cohort_risk |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for v in payload.get("variants") or []:
        lines.append(
            f"| `{v.get('variant_id')}` | {v.get('variant_type')} | {v.get('eligible_count_total')} | "
            f"{v.get('eligible_top_20_count')} | {v.get('eligible_bridge_vs_emerging_jaccard')} | "
            f"{v.get('delta_vs_current_eligible_jaccard')} | {v.get('new_unlabeled_top20_count')} | "
            f"{v.get('candidate_for_zero_weight_rerun')} | {v.get('too_blunt')} | {v.get('cohort_risk')} |"
        )
    summ = payload.get("summary") or {}
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- **best_hard_exclusion_variant_id (lowest Jaccard, tie-break fewer new unlabeled):** `{summ.get('best_hard_exclusion_variant_id')}`",
            f"- **best_hard_exclusion_candidate_lowest_new_unlabeled_variant_id:** `{summ.get('best_hard_exclusion_candidate_lowest_new_unlabeled_variant_id')}`",
            f"- **best_residual_penalty_variant_id:** `{summ.get('best_residual_penalty_variant_id')}`",
            f"- **any_variant_beats_jaccard_floor_with_full_top20_and_cohort_ge_50:** `{summ.get('any_variant_beats_jaccard_floor_with_full_top20_and_cohort_ge_50')}`",
            f"- **candidate_variant_ids:** `{summ.get('candidate_variant_ids')}`",
            "",
            "## Recommended next step",
            "",
            "At most: plan a follow-up **zero-weight** ranking experiment if a candidate passes the gate; "
            "do not treat simulation output as a production rule or default bridge arm.",
            "",
        ]
    )
    return "\n".join(lines)


def run_bridge_objective_redesign_simulation(
    *,
    ranking_run_id: str,
    k: int,
    sensitivity_json_path: Path,
    failure_analysis_json_path: Path,
    bridge_worksheet_csv_path: Path,
    output_json_path: Path,
    markdown_path: Path,
    database_url: str | None = None,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_simulation_payload(
            conn,
            ranking_run_id=ranking_run_id,
            k=k,
            sensitivity_json_path=sensitivity_json_path,
            failure_analysis_json_path=failure_analysis_json_path,
            bridge_worksheet_csv_path=bridge_worksheet_csv_path,
        )
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown_from_simulation(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeObjectiveRedesignSimulationError",
    "BridgeSimRow",
    "build_simulation_payload",
    "load_labeled_paper_ids_from_worksheet_csv",
    "load_persistent_ids_from_failure_json",
    "markdown_from_simulation",
    "run_bridge_objective_redesign_simulation",
]
