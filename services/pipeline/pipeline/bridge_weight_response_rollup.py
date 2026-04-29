"""Synthesize bridge-weight experiments (zero / w005 / w010) into one audit artifact (read-only)."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env


class BridgeWeightResponseRollupError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise BridgeWeightResponseRollupError(f"{label} not found: {path}", code=2)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BridgeWeightResponseRollupError(f"{label} is not valid JSON: {path}", code=2) from exc
    if not isinstance(parsed, dict):
        raise BridgeWeightResponseRollupError(f"{label} must be a JSON object: {path}", code=2)
    return parsed


def _load_paper_ids_from_csv(path: Path, *, label: str) -> set[str]:
    if not path.is_file():
        raise BridgeWeightResponseRollupError(f"{label} not found: {path}", code=2)
    out: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "paper_id" not in reader.fieldnames:
            raise BridgeWeightResponseRollupError(f"{label} missing paper_id column: {path}", code=2)
        for row in reader:
            pid = (row.get("paper_id") or "").strip()
            if pid:
                out.add(pid)
    return out


def _stack_from_comparison_prov(prov: dict[str, Any]) -> dict[str, str]:
    b = prov.get("baseline")
    if not isinstance(b, dict):
        raise BridgeWeightResponseRollupError("comparison provenance.baseline missing", code=2)
    return {
        "corpus_snapshot_version": str(b.get("corpus_snapshot_version") or ""),
        "embedding_version": str(b.get("embedding_version") or ""),
        "cluster_version": str(b.get("cluster_version") or ""),
        "bridge_eligibility_mode": str(b.get("bridge_eligibility_mode") or ""),
    }


def _validate_same_stack(*stacks: dict[str, str]) -> None:
    keys = ("corpus_snapshot_version", "embedding_version", "cluster_version", "bridge_eligibility_mode")
    ref = stacks[0]
    for i, s in enumerate(stacks[1:], start=1):
        for k in keys:
            if ref[k] != s[k]:
                raise BridgeWeightResponseRollupError(
                    f"stack mismatch on {k!r}: {ref[k]!r} vs {s[k]!r} (artifact index {i}).",
                    code=2,
                )


def _weights_from_prov(prov: dict[str, Any]) -> tuple[float, float]:
    b = prov.get("baseline")
    e = prov.get("experiment")
    if not isinstance(b, dict) or not isinstance(e, dict):
        raise BridgeWeightResponseRollupError("comparison provenance missing baseline/experiment", code=2)
    bw_b = b.get("bridge_weight_for_family_bridge")
    bw_e = e.get("bridge_weight_for_family_bridge")
    if not isinstance(bw_b, (int, float)) or not isinstance(bw_e, (int, float)):
        raise BridgeWeightResponseRollupError("bridge_weight_for_family_bridge missing or not numeric", code=2)
    return float(bw_b), float(bw_e)


def _eligible_movement(cmp: dict[str, Any]) -> dict[str, Any]:
    btc = cmp.get("bridge_top_k_comparison")
    if not isinstance(btc, dict):
        raise BridgeWeightResponseRollupError("comparison missing bridge_top_k_comparison", code=2)
    base_ids = btc.get("baseline_eligible_bridge_top_k_ids")
    exp_ids = btc.get("experiment_eligible_bridge_top_k_ids")
    ov = btc.get("eligible_bridge_overlap")
    if not isinstance(base_ids, list) or not isinstance(exp_ids, list):
        raise BridgeWeightResponseRollupError("comparison missing eligible bridge top-k ids", code=2)
    if not isinstance(ov, dict):
        raise BridgeWeightResponseRollupError("comparison missing eligible_bridge_overlap", code=2)
    sb = {int(x) for x in base_ids}
    se = {int(x) for x in exp_ids}
    return {
        "eligible_bridge_jaccard": float(ov.get("jaccard")),
        "overlap_count": int(ov.get("overlap_count")),
        "union_count": int(ov.get("union_count")),
        "new_eligible_work_ids": sorted(se - sb),
        "dropped_eligible_work_ids": sorted(sb - se),
    }


def _family_stable(cmp: dict[str, Any]) -> tuple[bool, bool]:
    em = cmp.get("emerging_comparison") or {}
    uc = cmp.get("undercited_comparison") or {}
    return bool(not em.get("changed")), bool(not uc.get("changed"))


def _distinctness_pair(cmp: dict[str, Any]) -> tuple[float, float]:
    d = cmp.get("distinctness") or {}
    bj = d.get("baseline_eligible_bridge_vs_emerging_jaccard")
    ej = d.get("experiment_eligible_bridge_vs_emerging_jaccard")
    if not isinstance(bj, (int, float)) or not isinstance(ej, (int, float)):
        raise BridgeWeightResponseRollupError("comparison missing distinctness Jaccard fields", code=2)
    return float(bj), float(ej)


def _classify_distinctness_trend(zero_j: float, w005_j: float, w010_j: float) -> str:
    """Higher eligible-vs-emerging Jaccard means more emerging-like overlap (weaker bridge distinctness)."""
    tol = 1e-6
    seq = [zero_j, w005_j, w010_j]
    if max(seq) - min(seq) <= tol:
        return "stable"
    if w010_j > w005_j + tol or w005_j > zero_j + tol:
        return "degraded"
    if w010_j < w005_j - tol or w005_j < zero_j - tol:
        return "improved"
    return "mixed"


def fetch_openalex_ids_for_work_ids(conn: psycopg.Connection, work_ids: list[int]) -> dict[int, str]:
    if not work_ids:
        return {}
    rows = conn.execute(
        """
        SELECT id, openalex_id
        FROM works
        WHERE id = ANY(%s)
        """,
        (work_ids,),
    ).fetchall()
    out: dict[int, str] = {}
    for r in rows:
        wid = int(r["id"])
        oid = r.get("openalex_id")
        if oid is not None:
            out[wid] = str(oid).strip()
    return out


def build_bridge_weight_response_rollup_payload(
    *,
    baseline_review_rollup: dict[str, Any],
    compare_zero_vs_w005: dict[str, Any],
    delta_review_summary: dict[str, Any],
    compare_w005_vs_w010: dict[str, Any],
    compare_zero_vs_w010: dict[str, Any],
    labeled_baseline_bridge_worksheet: Path,
    delta_review_csv: Path,
    conn: psycopg.Connection | None,
) -> dict[str, Any]:
    prov0 = compare_zero_vs_w005.get("provenance")
    prov1 = compare_w005_vs_w010.get("provenance")
    prov2 = compare_zero_vs_w010.get("provenance")
    if not isinstance(prov0, dict) or not isinstance(prov1, dict) or not isinstance(prov2, dict):
        raise BridgeWeightResponseRollupError("comparison missing provenance", code=2)

    s0 = _stack_from_comparison_prov(prov0)
    s1 = _stack_from_comparison_prov(prov1)
    s2 = _stack_from_comparison_prov(prov2)
    _validate_same_stack(s0, s1, s2)

    wb0, we0 = _weights_from_prov(prov0)
    wb1, we1 = _weights_from_prov(prov1)
    wb2, we2 = _weights_from_prov(prov2)
    if not (wb0 == 0.0 and we0 == 0.05):
        raise BridgeWeightResponseRollupError(
            f"zero-vs-w005 weights expected (0.0, 0.05), got ({wb0}, {we0}).",
            code=2,
        )
    if not (wb1 == 0.05 and we1 == 0.10):
        raise BridgeWeightResponseRollupError(
            f"w005-vs-w010 weights expected (0.05, 0.10), got ({wb1}, {we1}).",
            code=2,
        )
    if not (wb2 == 0.0 and we2 == 0.10):
        raise BridgeWeightResponseRollupError(
            f"zero-vs-w010 weights expected (0.0, 0.10), got ({wb2}, {we2}).",
            code=2,
        )

    rr_prov = baseline_review_rollup.get("provenance")
    if not isinstance(rr_prov, dict):
        raise BridgeWeightResponseRollupError("review rollup missing provenance", code=2)
    if str(rr_prov.get("ranking_run_id") or "") != str(prov0["baseline"]["ranking_run_id"]):
        raise BridgeWeightResponseRollupError("review rollup ranking_run_id does not match zero baseline run.", code=2)
    for k in ("corpus_snapshot_version", "embedding_version", "cluster_version"):
        if str(rr_prov.get(k) or "") != str(s0[k]):
            raise BridgeWeightResponseRollupError(
                f"review rollup provenance {k!r} does not match comparison stack ({rr_prov.get(k)!r} vs {s0[k]!r}).",
                code=2,
            )

    movement_z_w005 = _eligible_movement(compare_zero_vs_w005)
    movement_w005_w010 = _eligible_movement(compare_w005_vs_w010)
    movement_z_w010 = _eligible_movement(compare_zero_vs_w010)

    em0, um0 = _family_stable(compare_zero_vs_w005)
    em1, um1 = _family_stable(compare_w005_vs_w010)
    em2, um2 = _family_stable(compare_zero_vs_w010)

    dz_z, dz_w005_a = _distinctness_pair(compare_zero_vs_w005)
    dz_w005_b, dz_w010_a = _distinctness_pair(compare_w005_vs_w010)
    dz_z_b, dz_w010_b = _distinctness_pair(compare_zero_vs_w010)

    if abs(dz_z - dz_z_b) > 1e-5 or abs(dz_w010_a - dz_w010_b) > 1e-5:
        raise BridgeWeightResponseRollupError(
            "distinctness Jaccard mismatch across comparison artifacts for the same runs.",
            code=2,
        )
    if abs(dz_w005_a - dz_w005_b) > 1e-5:
        raise BridgeWeightResponseRollupError(
            "distinctness Jaccard mismatch for w005 across comparison artifacts.",
            code=2,
        )

    zero_j, w005_j, w010_j = dz_z, dz_w005_a, dz_w010_a
    distinctness_trend = _classify_distinctness_trend(zero_j, w005_j, w010_j)

    eligible_j_z_w005 = movement_z_w005["eligible_bridge_jaccard"]
    eligible_j_w005_w010 = movement_w005_w010["eligible_bridge_jaccard"]
    eligible_j_z_w010 = movement_z_w010["eligible_bridge_jaccard"]

    moved_eligible_vs_zero = (
        movement_z_w005["new_eligible_work_ids"]
        or movement_z_w005["dropped_eligible_work_ids"]
        or eligible_j_z_w005 < 1.0 - 1e-9
    )

    qr = compare_w005_vs_w010.get("quality_risk") or {}
    unlabeled_new_vs_w005 = int(qr.get("unlabeled_new_experiment_eligible_top_k_count") or 0)

    saturated = eligible_j_w005_w010 >= 1.0 - 1e-9

    distinctness_ok = w010_j <= w005_j + 1e-5 and w010_j <= zero_j + 1e-5
    non_bridge_ok = em0 and um0 and em1 and um1 and em2 and um2

    weight_response_controlled = bool(
        moved_eligible_vs_zero
        and unlabeled_new_vs_w005 == 0
        and distinctness_ok
        and non_bridge_ok
    )

    # Spec: false when saturated; otherwise remain conservative (no larger-weight recommendation here).
    recommend_next_weight_increase = False

    bridge_review = (baseline_review_rollup.get("per_family") or {}).get("bridge") or {}
    bridge_metrics = bridge_review.get("metrics") if isinstance(bridge_review, dict) else None

    labeled_ids = _load_paper_ids_from_csv(labeled_baseline_bridge_worksheet, label="baseline bridge worksheet")
    labeled_ids |= _load_paper_ids_from_csv(delta_review_csv, label="delta review CSV")

    exp_eligible_wids = compare_zero_vs_w010["bridge_top_k_comparison"]["experiment_eligible_bridge_top_k_ids"]
    if not isinstance(exp_eligible_wids, list):
        raise BridgeWeightResponseRollupError("missing experiment eligible ids", code=2)
    exp_eligible_wids_int = [int(x) for x in exp_eligible_wids]

    label_coverage: dict[str, Any]
    if conn is None:
        label_coverage = {
            "complete": None,
            "missing_paper_ids": [],
            "missing_work_ids": [],
            "warning": "No DB connection; skipped resolving work ids to paper_ids for label coverage.",
        }
    else:
        pid_map = fetch_openalex_ids_for_work_ids(conn, exp_eligible_wids_int)
        missing_wids: list[int] = []
        missing_pids: list[str] = []
        for wid in exp_eligible_wids_int:
            pid = pid_map.get(wid)
            if not pid or pid not in labeled_ids:
                missing_wids.append(wid)
                if pid:
                    missing_pids.append(pid)
        label_coverage = {
            "complete": len(missing_wids) == 0,
            "missing_paper_ids": missing_pids,
            "missing_work_ids": missing_wids,
            "warning": None,
        }

    lines = []
    if saturated:
        lines.append(
            "0.10 did not improve eligible top-20 membership over 0.05; stop increasing weight until broader labels "
            "or a different scoring objective justify it."
        )
    if weight_response_controlled:
        lines.append("0.05 remains a plausible experimental bridge-weight arm, not a default.")

    recommendation_text = " ".join(lines).strip()
    if not recommendation_text:
        recommendation_text = (
            "Review bridge-weight evidence cautiously; do not increase weight or adopt a default without broader evaluation."
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "bridge_weight_response_rollup",
        "stack": s0,
        "runs": {
            "zero": {
                "ranking_run_id": str(prov0["baseline"]["ranking_run_id"]),
                "bridge_weight_for_family_bridge": 0.0,
            },
            "w005": {
                "ranking_run_id": str(prov0["experiment"]["ranking_run_id"]),
                "bridge_weight_for_family_bridge": 0.05,
            },
            "w010": {
                "ranking_run_id": str(prov1["experiment"]["ranking_run_id"]),
                "bridge_weight_for_family_bridge": 0.10,
            },
        },
        "movement": {
            "eligible_bridge_jaccard": {
                "zero_vs_w005": eligible_j_z_w005,
                "w005_vs_w010": eligible_j_w005_w010,
                "zero_vs_w010": eligible_j_z_w010,
            },
            "eligible_bridge_sets": {
                "zero_vs_w005": movement_z_w005,
                "w005_vs_w010": movement_w005_w010,
                "zero_vs_w010": movement_z_w010,
            },
        },
        "distinctness": {
            "eligible_bridge_vs_emerging_jaccard_by_run": {
                "zero": zero_j,
                "w005": w005_j,
                "w010": w010_j,
            },
            "trend_vs_emerging_overlap": distinctness_trend,
        },
        "quality_evidence": {
            "baseline_bridge_eligible_review_metrics": bridge_metrics,
            "delta_w005_review_summary": delta_review_summary.get("metrics"),
            "delta_w005_gates": delta_review_summary.get("gates"),
            "w010_new_unlabeled_eligible_vs_w005_count": unlabeled_new_vs_w005,
            "w010_eligible_label_coverage_vs_baseline_plus_delta": label_coverage,
        },
        "non_bridge_stability": {
            "emerging_top_k_unchanged_all_comparisons": em0 and em1 and em2,
            "undercited_top_k_unchanged_all_comparisons": um0 and um1 and um2,
            "per_comparison": {
                "zero_vs_w005": {"emerging": em0, "undercited": um0},
                "w005_vs_w010": {"emerging": em1, "undercited": um1},
                "zero_vs_w010": {"emerging": em2, "undercited": um2},
            },
        },
        "decision": {
            "weight_response_controlled": weight_response_controlled,
            "weight_response_saturated": saturated,
            "recommend_next_weight_increase": recommend_next_weight_increase,
            "ready_for_default": False,
            "recommendation_text": recommendation_text,
        },
        "caveats": [
            "This is not validation of bridge ranking quality.",
            "Evidence is largely single-reviewer, small-n (including top-20 worksheets / delta rows).",
            "No user study or product-facing evaluation is implied.",
            "Do not change defaults based on this artifact alone.",
            "This does not claim ML ranking superiority over simpler baselines.",
        ],
    }


def markdown_from_bridge_weight_response_rollup(payload: dict[str, Any]) -> str:
    d = payload.get("decision") or {}
    mv = payload.get("movement") or {}
    ej = (mv.get("eligible_bridge_jaccard") or {}) if isinstance(mv, dict) else {}
    ds = payload.get("distinctness") or {}
    q = payload.get("quality_evidence") or {}
    nb = payload.get("non_bridge_stability") or {}
    lines = [
        "# Bridge weight response rollup",
        "",
        "This artifact is **not** validation and does **not** justify changing defaults.",
        "",
        "## Stack",
        "",
        f"- **corpus_snapshot_version:** `{payload.get('stack', {}).get('corpus_snapshot_version')}`",
        f"- **embedding_version:** `{payload.get('stack', {}).get('embedding_version')}`",
        f"- **cluster_version:** `{payload.get('stack', {}).get('cluster_version')}`",
        f"- **bridge_eligibility_mode:** `{payload.get('stack', {}).get('bridge_eligibility_mode')}`",
        "",
        "## Movement (eligible bridge top-k)",
        "",
        f"- **zero vs w005 Jaccard:** `{ej.get('zero_vs_w005')}`",
        f"- **w005 vs w010 Jaccard:** `{ej.get('w005_vs_w010')}`",
        f"- **zero vs w010 Jaccard:** `{ej.get('zero_vs_w010')}`",
        "",
        "## Distinctness (eligible bridge vs emerging)",
        "",
        f"- **by run:** `{ds.get('eligible_bridge_vs_emerging_jaccard_by_run')}`",
        f"- **trend (overlap semantics):** `{ds.get('trend_vs_emerging_overlap')}`",
        "",
        "## Quality evidence",
        "",
        f"- **w010 new unlabeled eligible vs w005:** `{q.get('w010_new_unlabeled_eligible_vs_w005_count')}`",
        f"- **w010 eligible label coverage complete:** `{ (q.get('w010_eligible_label_coverage_vs_baseline_plus_delta') or {}).get('complete')}`",
        "",
        "## Non-bridge stability",
        "",
        f"- **emerging unchanged (all comparisons):** `{nb.get('emerging_top_k_unchanged_all_comparisons')}`",
        f"- **undercited unchanged (all comparisons):** `{nb.get('undercited_top_k_unchanged_all_comparisons')}`",
        "",
        "## Decision",
        "",
        f"- **weight_response_controlled:** `{d.get('weight_response_controlled')}`",
        f"- **weight_response_saturated:** `{d.get('weight_response_saturated')}`",
        f"- **recommend_next_weight_increase:** `{d.get('recommend_next_weight_increase')}`",
        f"- **ready_for_default:** `{d.get('ready_for_default')}`",
        "",
        "### Recommendation",
        "",
        str(d.get("recommendation_text") or ""),
        "",
        "## Caveats",
        "",
    ]
    for c in payload.get("caveats") or []:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_weight_response_rollup(
    *,
    baseline_review_rollup_path: Path,
    compare_zero_vs_w005_path: Path,
    delta_review_summary_path: Path,
    compare_w005_vs_w010_path: Path,
    compare_zero_vs_w010_path: Path,
    labeled_baseline_bridge_worksheet_path: Path,
    delta_review_csv_path: Path,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None,
) -> dict[str, Any]:
    baseline_review_rollup = _read_json(baseline_review_rollup_path, label="baseline review rollup")
    compare_zero_vs_w005 = _read_json(compare_zero_vs_w005_path, label="compare zero vs w005")
    delta_review_summary = _read_json(delta_review_summary_path, label="delta review summary")
    compare_w005_vs_w010 = _read_json(compare_w005_vs_w010_path, label="compare w005 vs w010")
    compare_zero_vs_w010 = _read_json(compare_zero_vs_w010_path, label="compare zero vs w010")

    dsn = database_url or database_url_from_env()
    try:
        with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
            payload = build_bridge_weight_response_rollup_payload(
                baseline_review_rollup=baseline_review_rollup,
                compare_zero_vs_w005=compare_zero_vs_w005,
                delta_review_summary=delta_review_summary,
                compare_w005_vs_w010=compare_w005_vs_w010,
                compare_zero_vs_w010=compare_zero_vs_w010,
                labeled_baseline_bridge_worksheet=labeled_baseline_bridge_worksheet_path,
                delta_review_csv=delta_review_csv_path,
                conn=conn,
            )
    except Exception:
        payload = build_bridge_weight_response_rollup_payload(
            baseline_review_rollup=baseline_review_rollup,
            compare_zero_vs_w005=compare_zero_vs_w005,
            delta_review_summary=delta_review_summary,
            compare_w005_vs_w010=compare_w005_vs_w010,
            compare_zero_vs_w010=compare_zero_vs_w010,
            labeled_baseline_bridge_worksheet=labeled_baseline_bridge_worksheet_path,
            delta_review_csv=delta_review_csv_path,
            conn=None,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_bridge_weight_response_rollup(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeWeightResponseRollupError",
    "build_bridge_weight_response_rollup_payload",
    "markdown_from_bridge_weight_response_rollup",
    "run_bridge_weight_response_rollup",
]
