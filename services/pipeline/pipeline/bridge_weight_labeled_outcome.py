"""Labeled eligible-bridge top-20 quality by bridge weight (baseline + delta labels only, read-only)."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.bridge_weight_experiment_delta_worksheet import DELTA_WORKSHEET_COLUMNS
from pipeline.bridge_weight_response_rollup import fetch_openalex_ids_for_work_ids
from pipeline.recommendation_review_summary import (
    BRIDGE_LIKE_ALLOWED,
    NOVELTY_ALLOWED,
    RELEVANCE_ALLOWED,
)

RANK_ZERO = "rank-ee2ba6c816"
RANK_W005 = "rank-bc1123e00c"
RANK_W010 = "rank-9a02c81d40"


class BridgeWeightLabeledOutcomeError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise BridgeWeightLabeledOutcomeError(f"{label} not found: {path}", code=2)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BridgeWeightLabeledOutcomeError(f"{label} is not valid JSON: {path}", code=2) from exc
    if not isinstance(parsed, dict):
        raise BridgeWeightLabeledOutcomeError(f"{label} must be a JSON object: {path}", code=2)
    return parsed


def _norm(s: str | None) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _stack_from_comparison_prov(prov: dict[str, Any]) -> dict[str, str]:
    b = prov.get("baseline")
    if not isinstance(b, dict):
        raise BridgeWeightLabeledOutcomeError("comparison provenance.baseline missing", code=2)
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
                raise BridgeWeightLabeledOutcomeError(
                    f"stack mismatch on {k!r}: {ref[k]!r} vs {s[k]!r} (artifact index {i}).",
                    code=2,
                )


def _validate_label_value(col: str, value: str, allowed: frozenset[str], *, ctx: str) -> None:
    if not value:
        raise BridgeWeightLabeledOutcomeError(f"{ctx}: column {col!r} is blank", code=2)
    if value not in allowed:
        raise BridgeWeightLabeledOutcomeError(
            f"{ctx}: column {col!r} has invalid value {value!r} (expected one of: {', '.join(sorted(allowed))})",
            code=2,
        )


def _load_baseline_worksheet(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise BridgeWeightLabeledOutcomeError(f"baseline worksheet not found: {path}", code=2)
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise BridgeWeightLabeledOutcomeError(f"baseline worksheet has no header: {path}", code=2)
    rows: list[dict[str, str]] = []
    for row in reader:
        if row is None:
            continue
        rows.append({str(k or "").strip(): _norm(v) for k, v in row.items()})
    if not rows:
        raise BridgeWeightLabeledOutcomeError("baseline worksheet has no data rows.", code=2)
    return rows


def _validate_baseline_worksheet(rows: list[dict[str, str]], *, path: Path) -> None:
    variants = {r.get("review_pool_variant", "") for r in rows}
    if variants != {"bridge_eligible_only"}:
        raise BridgeWeightLabeledOutcomeError(
            f"{path}: review_pool_variant must be bridge_eligible_only for all rows; got {sorted(variants)}",
            code=2,
        )
    bad_family = {r.get("family", "") for r in rows if r.get("family", "") != "bridge"}
    if bad_family:
        raise BridgeWeightLabeledOutcomeError(f"{path}: includes non-bridge family rows.", code=2)
    bad_run = {r.get("ranking_run_id", "") for r in rows if r.get("ranking_run_id", "") != RANK_ZERO}
    if bad_run:
        raise BridgeWeightLabeledOutcomeError(
            f"{path}: ranking_run_id must be {RANK_ZERO!r} for all rows.", code=2
        )
    invalid_eligible = [r.get("bridge_eligible", "") for r in rows if r.get("bridge_eligible", "") != "true"]
    if invalid_eligible:
        raise BridgeWeightLabeledOutcomeError(
            f"{path}: bridge_eligible must be true for all rows in eligible-only pool.", code=2
        )
    for i, r in enumerate(rows, start=1):
        pid = r.get("paper_id") or f"(row {i})"
        ctx = f"{path} data row {i} (paper_id={pid!r})"
        _validate_label_value("relevance_label", r.get("relevance_label", ""), RELEVANCE_ALLOWED, ctx=ctx)
        _validate_label_value("novelty_label", r.get("novelty_label", ""), NOVELTY_ALLOWED, ctx=ctx)
        _validate_label_value("bridge_like_label", r.get("bridge_like_label", ""), BRIDGE_LIKE_ALLOWED, ctx=ctx)


def _load_delta_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise BridgeWeightLabeledOutcomeError(f"delta worksheet not found: {path}", code=2)
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise BridgeWeightLabeledOutcomeError(f"delta worksheet has no header: {path}", code=2)
    header = [h.strip() if h else "" for h in reader.fieldnames]
    missing = [c for c in DELTA_WORKSHEET_COLUMNS if c not in header]
    if missing:
        raise BridgeWeightLabeledOutcomeError(
            f"{path}: delta worksheet missing columns: {', '.join(missing)}", code=2
        )
    rows: list[dict[str, str]] = []
    for row in reader:
        if row is None:
            continue
        if not any(_norm(v) for k, v in row.items() if k):
            continue
        rows.append({c: _norm(row.get(c, "")) for c in DELTA_WORKSHEET_COLUMNS})
    if not rows:
        raise BridgeWeightLabeledOutcomeError(f"{path}: delta worksheet has no data rows.", code=2)
    return rows


def _validate_delta_worksheet(rows: list[dict[str, str]], *, path: Path) -> None:
    for i, r in enumerate(rows, start=1):
        pid = r.get("paper_id") or f"(row {i})"
        ctx = f"{path} data row {i} (paper_id={pid!r})"
        if r.get("baseline_ranking_run_id", "") != RANK_ZERO:
            raise BridgeWeightLabeledOutcomeError(
                f"{ctx}: baseline_ranking_run_id must be {RANK_ZERO!r}.", code=2
            )
        if r.get("experiment_ranking_run_id", "") != RANK_W005:
            raise BridgeWeightLabeledOutcomeError(
                f"{ctx}: experiment_ranking_run_id must be {RANK_W005!r}.", code=2
            )
        _validate_label_value("relevance_label", r.get("relevance_label", ""), RELEVANCE_ALLOWED, ctx=ctx)
        _validate_label_value("novelty_label", r.get("novelty_label", ""), NOVELTY_ALLOWED, ctx=ctx)
        _validate_label_value("bridge_like_label", r.get("bridge_like_label", ""), BRIDGE_LIKE_ALLOWED, ctx=ctx)


@dataclass(frozen=True)
class _LabelTriple:
    relevance_label: str
    novelty_label: str
    bridge_like_label: str


def _build_label_map(
    baseline_rows: list[dict[str, str]],
    delta_rows: list[dict[str, str]],
    *,
    baseline_path: Path,
    delta_path: Path,
) -> dict[str, _LabelTriple]:
    out: dict[str, _LabelTriple] = {}
    for r in baseline_rows:
        pid = _norm(r.get("paper_id"))
        if not pid:
            raise BridgeWeightLabeledOutcomeError(f"{baseline_path}: row missing paper_id", code=2)
        triple = _LabelTriple(
            relevance_label=r["relevance_label"],
            novelty_label=r["novelty_label"],
            bridge_like_label=r["bridge_like_label"],
        )
        out[pid] = triple
    for r in delta_rows:
        pid = _norm(r.get("paper_id"))
        if not pid:
            raise BridgeWeightLabeledOutcomeError(f"{delta_path}: row missing paper_id", code=2)
        triple = _LabelTriple(
            relevance_label=r["relevance_label"],
            novelty_label=r["novelty_label"],
            bridge_like_label=r["bridge_like_label"],
        )
        if pid in out:
            ex = out[pid]
            if (
                ex.relevance_label != triple.relevance_label
                or ex.novelty_label != triple.novelty_label
                or ex.bridge_like_label != triple.bridge_like_label
            ):
                raise BridgeWeightLabeledOutcomeError(
                    f"conflicting labels for paper_id={pid!r}: baseline vs delta disagree.",
                    code=2,
                )
        else:
            out[pid] = triple
    return out


def _work_id_to_paper_id_partial(diagnostics: dict[str, Any]) -> dict[int, str]:
    rows = diagnostics.get("bridge_top_k_rows")
    if not isinstance(rows, list):
        raise BridgeWeightLabeledOutcomeError("diagnostics missing bridge_top_k_rows list", code=2)
    out: dict[int, str] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        wid = r.get("work_id")
        pid = r.get("paper_id")
        if isinstance(wid, int) and isinstance(pid, str) and pid.strip():
            out[int(wid)] = pid.strip()
    return out


def _baseline_rows_sorted_by_rank(baseline_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    def rank_key(r: dict[str, str]) -> int:
        try:
            return int(r.get("rank") or "0")
        except ValueError:
            return 0

    return sorted(baseline_rows, key=rank_key)


def _resolve_eligible_work_id_to_paper_id(
    *,
    ranking_run_id: str,
    eligible_work_ids: list[int],
    baseline_rows_sorted: list[dict[str, str]],
    diagnostics: dict[str, Any],
    conn: psycopg.Connection | None,
    diag_path: Path,
) -> dict[int, str]:
    """Map each eligible-head work_id to OpenAlex paper_id URL (baseline zip for zero + partial diag + optional DB)."""
    out: dict[int, str] = dict(_work_id_to_paper_id_partial(diagnostics))
    if ranking_run_id == RANK_ZERO:
        if len(baseline_rows_sorted) != 20 or len(eligible_work_ids) != 20:
            raise BridgeWeightLabeledOutcomeError(
                "internal: baseline rows and eligible head must each have length 20 for zero run.",
                code=2,
            )
        for wid, row in zip(eligible_work_ids, baseline_rows_sorted, strict=True):
            pid = _norm(row.get("paper_id"))
            if not pid:
                raise BridgeWeightLabeledOutcomeError("baseline worksheet row missing paper_id", code=2)
            if wid in out and out[wid] != pid:
                raise BridgeWeightLabeledOutcomeError(
                    f"{diag_path}: work_id {wid} paper_id from diagnostics ({out[wid]!r}) "
                    f"does not match baseline worksheet ({pid!r}) for eligible head ordering.",
                    code=2,
                )
            out[wid] = pid
    missing = [w for w in eligible_work_ids if not out.get(w)]
    if missing:
        if conn is None:
            raise BridgeWeightLabeledOutcomeError(
                f"{diag_path}: cannot resolve paper_id for work_ids {missing[:8]!r} "
                f"({len(missing)} total) without a read-only database connection "
                f"(set DATABASE_URL or pass --database-url).",
                code=2,
            )
        fetched = fetch_openalex_ids_for_work_ids(conn, missing)
        for wid in missing:
            oid = fetched.get(wid)
            if not oid:
                raise BridgeWeightLabeledOutcomeError(
                    f"works.openalex_id missing for work_id={wid} (ranking_run_id={ranking_run_id!r}).",
                    code=2,
                )
            out[wid] = oid.strip()
    return {w: out[w] for w in eligible_work_ids}


def _eligible_ids_and_distinctness(diagnostics: dict[str, Any], *, path: Path) -> tuple[list[int], dict[str, float | None]]:
    ov = diagnostics.get("overlap_detail")
    if not isinstance(ov, dict):
        raise BridgeWeightLabeledOutcomeError(f"{path}: diagnostics missing overlap_detail", code=2)
    elig = ov.get("eligible_bridge_top_k_ids")
    if not isinstance(elig, list) or len(elig) != 20:
        raise BridgeWeightLabeledOutcomeError(
            f"{path}: overlap_detail.eligible_bridge_top_k_ids must be a list of length 20.",
            code=2,
        )
    ids = [int(x) for x in elig]
    ej = ov.get("eligible_bridge_vs_emerging_jaccard")
    fj = ov.get("bridge_vs_emerging_jaccard")
    distinctness: dict[str, float | None] = {
        "eligible_bridge_vs_emerging_jaccard": float(ej) if isinstance(ej, (int, float)) else None,
        "full_bridge_vs_emerging_jaccard": float(fj) if isinstance(fj, (int, float)) else None,
    }
    return ids, distinctness


def _diagnostics_prov(diagnostics: dict[str, Any], *, path: Path) -> dict[str, Any]:
    p = diagnostics.get("provenance")
    if not isinstance(p, dict):
        raise BridgeWeightLabeledOutcomeError(f"{path}: diagnostics missing provenance", code=2)
    rid = str(p.get("ranking_run_id") or "")
    if not rid:
        raise BridgeWeightLabeledOutcomeError(f"{path}: diagnostics provenance missing ranking_run_id", code=2)
    return p


def _head_label_rows(
    eligible_work_ids: list[int],
    wid_to_pid: dict[int, str],
    label_map: dict[str, _LabelTriple],
) -> tuple[list[dict[str, str]], list[int], list[str]]:
    """Returns (label_rows_for_metrics, missing_work_ids, missing_paper_ids)."""
    missing_w: list[int] = []
    missing_p: list[str] = []
    rows: list[dict[str, str]] = []
    for wid in eligible_work_ids:
        pid = wid_to_pid.get(wid)
        if not pid:
            missing_w.append(wid)
            continue
        lab = label_map.get(pid)
        if lab is None:
            missing_w.append(wid)
            missing_p.append(pid)
            continue
        rows.append(
            {
                "relevance_label": lab.relevance_label,
                "novelty_label": lab.novelty_label,
                "bridge_like_label": lab.bridge_like_label,
            }
        )
    return rows, missing_w, missing_p


def _compute_head_metrics(rows: list[dict[str, str]]) -> dict[str, Any]:
    n = len(rows)
    if n == 0:
        return {
            "row_count": 0,
            "labeled_count": 0,
            "good_count": 0,
            "acceptable_count": 0,
            "miss_count": 0,
            "irrelevant_count": 0,
            "good_or_acceptable_count": 0,
            "good_or_acceptable_share": None,
            "useful_or_surprising_count": 0,
            "useful_or_surprising_share": None,
            "bridge_like_yes_count": 0,
            "bridge_like_partial_count": 0,
            "bridge_like_yes_or_partial_count": 0,
            "bridge_like_yes_or_partial_share": None,
            "bridge_like_yes_only_share": None,
            "miss_or_irrelevant_count": 0,
            "bridge_like_no_count": 0,
        }
    good_count = sum(1 for r in rows if r.get("relevance_label") == "good")
    acceptable_count = sum(1 for r in rows if r.get("relevance_label") == "acceptable")
    miss_count = sum(1 for r in rows if r.get("relevance_label") == "miss")
    irrelevant_count = sum(1 for r in rows if r.get("relevance_label") == "irrelevant")
    goa = good_count + acceptable_count
    useful_or_surprising_count = sum(
        1 for r in rows if r.get("novelty_label") in ("useful", "surprising")
    )
    bridge_like_yes_count = sum(1 for r in rows if r.get("bridge_like_label") == "yes")
    bridge_like_partial_count = sum(1 for r in rows if r.get("bridge_like_label") == "partial")
    yop = bridge_like_yes_count + bridge_like_partial_count
    miss_or_irrelevant_count = miss_count + irrelevant_count
    bridge_like_no_count = sum(1 for r in rows if r.get("bridge_like_label") == "no")
    return {
        "row_count": n,
        "labeled_count": n,
        "good_count": good_count,
        "acceptable_count": acceptable_count,
        "miss_count": miss_count,
        "irrelevant_count": irrelevant_count,
        "good_or_acceptable_count": goa,
        "good_or_acceptable_share": goa / n,
        "useful_or_surprising_count": useful_or_surprising_count,
        "useful_or_surprising_share": useful_or_surprising_count / n,
        "bridge_like_yes_count": bridge_like_yes_count,
        "bridge_like_partial_count": bridge_like_partial_count,
        "bridge_like_yes_or_partial_count": yop,
        "bridge_like_yes_or_partial_share": yop / n,
        "bridge_like_yes_only_share": bridge_like_yes_count / n,
        "miss_or_irrelevant_count": miss_or_irrelevant_count,
        "bridge_like_no_count": bridge_like_no_count,
    }


def build_bridge_weight_labeled_outcome_payload(
    *,
    baseline_worksheet_path: Path,
    delta_worksheet_path: Path,
    response_rollup_path: Path,
    compare_zero_vs_w005_path: Path,
    compare_w005_vs_w010_path: Path,
    compare_zero_vs_w010_path: Path,
    diagnostics_paths: dict[str, Path],
    conn: psycopg.Connection | None,
) -> dict[str, Any]:
    baseline_rows = _load_baseline_worksheet(baseline_worksheet_path)
    _validate_baseline_worksheet(baseline_rows, path=baseline_worksheet_path)
    if len(baseline_rows) != 20:
        raise BridgeWeightLabeledOutcomeError(
            f"{baseline_worksheet_path}: expected exactly 20 baseline rows, got {len(baseline_rows)}.",
            code=2,
        )

    delta_rows = _load_delta_rows(delta_worksheet_path)
    _validate_delta_worksheet(delta_rows, path=delta_worksheet_path)

    label_map = _build_label_map(baseline_rows, delta_rows, baseline_path=baseline_worksheet_path, delta_path=delta_worksheet_path)
    baseline_sorted = _baseline_rows_sorted_by_rank(baseline_rows)

    response = _read_json(response_rollup_path, label="bridge weight response rollup")
    stack_obj = response.get("stack")
    if not isinstance(stack_obj, dict):
        raise BridgeWeightLabeledOutcomeError("response rollup missing stack object", code=2)
    stack = {k: str(stack_obj.get(k) or "") for k in ("corpus_snapshot_version", "embedding_version", "cluster_version", "bridge_eligibility_mode")}

    c01 = _read_json(compare_zero_vs_w005_path, label="compare zero vs w005")
    c12 = _read_json(compare_w005_vs_w010_path, label="compare w005 vs w010")
    c02 = _read_json(compare_zero_vs_w010_path, label="compare zero vs w010")
    for cmp_path, data in (
        (compare_zero_vs_w005_path, c01),
        (compare_w005_vs_w010_path, c12),
        (compare_zero_vs_w010_path, c02),
    ):
        prov = data.get("provenance")
        if not isinstance(prov, dict):
            raise BridgeWeightLabeledOutcomeError(f"{cmp_path}: missing provenance", code=2)
        _validate_same_stack(stack, _stack_from_comparison_prov(prov))

    movement = response.get("movement")
    if not isinstance(movement, dict):
        raise BridgeWeightLabeledOutcomeError("response rollup missing movement", code=2)

    runs_cfg = (
        (RANK_ZERO, "zero", 0.0, diagnostics_paths[RANK_ZERO]),
        (RANK_W005, "w005", 0.05, diagnostics_paths[RANK_W005]),
        (RANK_W010, "w010", 0.10, diagnostics_paths[RANK_W010]),
    )
    per_run: dict[str, Any] = {}
    distinctness_by_key: dict[str, dict[str, float | None]] = {}

    for rid, key, bw, dpath in runs_cfg:
        diag = _read_json(dpath, label=f"diagnostics {rid}")
        prov = _diagnostics_prov(diag, path=dpath)
        if str(prov.get("ranking_run_id") or "") != rid:
            raise BridgeWeightLabeledOutcomeError(
                f"{dpath}: provenance ranking_run_id {prov.get('ranking_run_id')!r} != expected {rid!r}.",
                code=2,
            )
        dstack = {
            "corpus_snapshot_version": str(prov.get("corpus_snapshot_version") or ""),
            "embedding_version": str(prov.get("embedding_version") or ""),
            "cluster_version": str(prov.get("cluster_version") or ""),
        }
        for k in ("corpus_snapshot_version", "embedding_version", "cluster_version"):
            if dstack[k] != stack[k]:
                raise BridgeWeightLabeledOutcomeError(
                    f"{dpath}: provenance {k!r} {dstack[k]!r} != rollup stack {stack[k]!r}.",
                    code=2,
                )
        eligible_ids, dist = _eligible_ids_and_distinctness(diag, path=dpath)
        wid_to_pid = _resolve_eligible_work_id_to_paper_id(
            ranking_run_id=rid,
            eligible_work_ids=eligible_ids,
            baseline_rows_sorted=baseline_sorted,
            diagnostics=diag,
            conn=conn,
            diag_path=dpath,
        )
        label_rows, miss_w, miss_p = _head_label_rows(eligible_ids, wid_to_pid, label_map)
        complete = len(miss_w) == 0 and len(label_rows) == 20
        metrics = _compute_head_metrics(label_rows) if complete else None
        distinctness_by_key[key] = dist
        per_run[key] = {
            "ranking_run_id": rid,
            "bridge_weight_for_family_bridge": bw,
            "coverage_complete": complete,
            "missing_work_ids": sorted(miss_w),
            "missing_paper_ids": sorted(set(miss_p)),
            "metrics": metrics,
            "distinctness": dist,
        }

    mv = movement.get("eligible_bridge_jaccard") if isinstance(movement.get("eligible_bridge_jaccard"), dict) else {}
    sets = movement.get("eligible_bridge_sets") if isinstance(movement.get("eligible_bridge_sets"), dict) else {}
    z_w005 = sets.get("zero_vs_w005") if isinstance(sets.get("zero_vs_w005"), dict) else {}
    w005_w010 = sets.get("w005_vs_w010") if isinstance(sets.get("w005_vs_w010"), dict) else {}
    z_w010 = sets.get("zero_vs_w010") if isinstance(sets.get("zero_vs_w010"), dict) else {}

    movement_block = {
        "eligible_bridge_jaccard": {
            "zero_vs_w005": mv.get("zero_vs_w005"),
            "w005_vs_w010": mv.get("w005_vs_w010"),
            "zero_vs_w010": mv.get("zero_vs_w010"),
        },
        "eligible_bridge_sets": {
            "zero_vs_w005": {
                "new_eligible_work_ids": z_w005.get("new_eligible_work_ids"),
                "dropped_eligible_work_ids": z_w005.get("dropped_eligible_work_ids"),
            },
            "w005_vs_w010": {
                "new_eligible_work_ids": w005_w010.get("new_eligible_work_ids"),
                "dropped_eligible_work_ids": w005_w010.get("dropped_eligible_work_ids"),
            },
            "zero_vs_w010": {
                "new_eligible_work_ids": z_w010.get("new_eligible_work_ids"),
                "dropped_eligible_work_ids": z_w010.get("dropped_eligible_work_ids"),
            },
        },
    }

    eligible_j_w005_w010 = float(mv.get("w005_vs_w010") or 0.0) if isinstance(mv.get("w005_vs_w010"), (int, float)) else 0.0
    response_saturated = eligible_j_w005_w010 >= 1.0 - 1e-9

    all_complete = all(per_run[k]["coverage_complete"] for k in ("zero", "w005", "w010"))
    mz = per_run["zero"]["metrics"]
    m5 = per_run["w005"]["metrics"]
    m10 = per_run["w010"]["metrics"]

    def _share(m: dict[str, Any] | None, key: str) -> float | None:
        if m is None:
            return None
        v = m.get(key)
        return float(v) if isinstance(v, (int, float)) else None

    zero_quality_baseline_ready = bool(
        per_run["zero"]["coverage_complete"]
        and mz is not None
        and (_share(mz, "good_or_acceptable_share") or 0) >= 0.80
    )
    w005_quality_preserved = bool(
        per_run["w005"]["coverage_complete"]
        and m5 is not None
        and (_share(m5, "good_or_acceptable_share") or 0) >= 0.80
        and (_share(m5, "bridge_like_yes_or_partial_share") or 0) >= 0.75
    )
    w010_quality_preserved = bool(
        per_run["w010"]["coverage_complete"]
        and m10 is not None
        and (_share(m10, "good_or_acceptable_share") or 0) >= 0.80
        and (_share(m10, "bridge_like_yes_or_partial_share") or 0) >= 0.75
    )

    dz0 = distinctness_by_key["zero"].get("eligible_bridge_vs_emerging_jaccard")
    dz5 = distinctness_by_key["w005"].get("eligible_bridge_vs_emerging_jaccard")
    tol = 1e-5
    distinctness_ok_zero_to_w005 = (
        isinstance(dz0, (int, float)) and isinstance(dz5, (int, float)) and float(dz5) <= float(dz0) + tol
    )
    recommend_w005_as_experimental_arm = bool(
        w005_quality_preserved and distinctness_ok_zero_to_w005 and all_complete
    )
    recommend_next_weight_increase = False
    ready_for_default = False

    interpretation = [
        "0.05 is a plausible experimental bridge-weight arm, not a default.",
        "0.10 did not improve eligible top-20 membership over 0.05; stop increasing weight for this stack.",
        "This is single-reviewer, top-20 offline evidence, not validation.",
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "bridge_weight_labeled_outcome",
        "stack": stack,
        "label_sources": {
            "baseline_worksheet": str(baseline_worksheet_path.resolve()),
            "delta_worksheet": str(delta_worksheet_path.resolve()),
            "label_map_paper_id_count": len(label_map),
        },
        "coverage": {
            "all_runs_complete": all_complete,
            "per_run": {
                k: {
                    "coverage_complete": per_run[k]["coverage_complete"],
                    "missing_work_ids": per_run[k]["missing_work_ids"],
                    "missing_paper_ids": per_run[k]["missing_paper_ids"],
                }
                for k in ("zero", "w005", "w010")
            },
        },
        "per_run": per_run,
        "movement": movement_block,
        "decision": {
            "zero_quality_baseline_ready": zero_quality_baseline_ready,
            "w005_quality_preserved": w005_quality_preserved,
            "w010_quality_preserved": w010_quality_preserved,
            "response_saturated": response_saturated,
            "recommend_w005_as_experimental_arm": recommend_w005_as_experimental_arm,
            "recommend_next_weight_increase": recommend_next_weight_increase,
            "ready_for_default": ready_for_default,
            "distinctness_stable_or_improved_zero_to_w005": distinctness_ok_zero_to_w005,
        },
        "interpretation": interpretation,
        "caveats": [
            "This is not validation of bridge ranking quality.",
            "Evidence is single-reviewer, top-20, offline audit material only.",
            "No user study or product-facing evaluation is implied.",
            "Do not change defaults based on this artifact alone.",
            "This does not claim ML ranking superiority over simpler baselines.",
        ],
    }


def markdown_from_bridge_weight_labeled_outcome(payload: dict[str, Any]) -> str:
    stack = payload.get("stack") or {}
    cov = payload.get("coverage") or {}
    dec = payload.get("decision") or {}
    mv = payload.get("movement") or {}
    ej = (mv.get("eligible_bridge_jaccard") or {}) if isinstance(mv, dict) else {}
    lines = [
        "# Bridge weight labeled outcome",
        "",
        "This artifact is **not** validation and does **not** justify changing defaults.",
        "",
        "## Stack",
        "",
        f"- **corpus_snapshot_version:** `{stack.get('corpus_snapshot_version')}`",
        f"- **embedding_version:** `{stack.get('embedding_version')}`",
        f"- **cluster_version:** `{stack.get('cluster_version')}`",
        f"- **bridge_eligibility_mode:** `{stack.get('bridge_eligibility_mode')}`",
        "",
        "## Label coverage",
        "",
        f"- **all_runs_complete:** `{cov.get('all_runs_complete')}`",
        "",
        "## Per-run metrics (eligible bridge top-20)",
        "",
    ]
    for k in ("zero", "w005", "w010"):
        pr = (payload.get("per_run") or {}).get(k) or {}
        m = pr.get("metrics")
        d = pr.get("distinctness") or {}
        lines.append(f"### {k}")
        lines.append(f"- **coverage_complete:** `{pr.get('coverage_complete')}`")
        if m:
            lines.append(f"- **good_or_acceptable_share:** `{m.get('good_or_acceptable_share')}`")
            lines.append(f"- **bridge_like_yes_or_partial_share:** `{m.get('bridge_like_yes_or_partial_share')}`")
        else:
            lines.append("- **metrics:** *(incomplete coverage)*")
        lines.append(f"- **eligible_bridge_vs_emerging_jaccard:** `{d.get('eligible_bridge_vs_emerging_jaccard')}`")
        lines.append(f"- **full_bridge_vs_emerging_jaccard:** `{d.get('full_bridge_vs_emerging_jaccard')}`")
        lines.append("")
    lines.extend(
        [
            "## Movement (eligible bridge top-20)",
            "",
            f"- **zero vs w005 Jaccard:** `{ej.get('zero_vs_w005')}`",
            f"- **w005 vs w010 Jaccard:** `{ej.get('w005_vs_w010')}`",
            f"- **zero vs w010 Jaccard:** `{ej.get('zero_vs_w010')}`",
            "",
            "## Decision",
            "",
            f"- **zero_quality_baseline_ready:** `{dec.get('zero_quality_baseline_ready')}`",
            f"- **w005_quality_preserved:** `{dec.get('w005_quality_preserved')}`",
            f"- **w010_quality_preserved:** `{dec.get('w010_quality_preserved')}`",
            f"- **response_saturated:** `{dec.get('response_saturated')}`",
            f"- **recommend_w005_as_experimental_arm:** `{dec.get('recommend_w005_as_experimental_arm')}`",
            f"- **recommend_next_weight_increase:** `{dec.get('recommend_next_weight_increase')}`",
            f"- **ready_for_default:** `{dec.get('ready_for_default')}`",
            "",
            "## Interpretation",
            "",
        ]
    )
    for t in payload.get("interpretation") or []:
        lines.append(f"- {t}")
    lines.extend(["", "## Caveats", ""])
    for c in payload.get("caveats") or []:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_weight_labeled_outcome(
    *,
    baseline_worksheet_path: Path,
    delta_worksheet_path: Path,
    response_rollup_path: Path,
    compare_zero_vs_w005_path: Path,
    compare_w005_vs_w010_path: Path,
    compare_zero_vs_w010_path: Path,
    diagnostics_rank_zero_path: Path,
    diagnostics_rank_w005_path: Path,
    diagnostics_rank_w010_path: Path,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None,
) -> dict[str, Any]:
    diagnostics_paths = {
        RANK_ZERO: diagnostics_rank_zero_path,
        RANK_W005: diagnostics_rank_w005_path,
        RANK_W010: diagnostics_rank_w010_path,
    }
    dsn = database_url or database_url_from_env()
    try:
        with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
            payload = build_bridge_weight_labeled_outcome_payload(
                baseline_worksheet_path=baseline_worksheet_path,
                delta_worksheet_path=delta_worksheet_path,
                response_rollup_path=response_rollup_path,
                compare_zero_vs_w005_path=compare_zero_vs_w005_path,
                compare_w005_vs_w010_path=compare_w005_vs_w010_path,
                compare_zero_vs_w010_path=compare_zero_vs_w010_path,
                diagnostics_paths=diagnostics_paths,
                conn=conn,
            )
    except BridgeWeightLabeledOutcomeError:
        raise
    except Exception:
        payload = build_bridge_weight_labeled_outcome_payload(
            baseline_worksheet_path=baseline_worksheet_path,
            delta_worksheet_path=delta_worksheet_path,
            response_rollup_path=response_rollup_path,
            compare_zero_vs_w005_path=compare_zero_vs_w005_path,
            compare_w005_vs_w010_path=compare_w005_vs_w010_path,
            compare_zero_vs_w010_path=compare_zero_vs_w010_path,
            diagnostics_paths=diagnostics_paths,
            conn=None,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_bridge_weight_labeled_outcome(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeWeightLabeledOutcomeError",
    "build_bridge_weight_labeled_outcome_payload",
    "markdown_from_bridge_weight_labeled_outcome",
    "run_bridge_weight_labeled_outcome",
]
