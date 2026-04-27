"""Join recommendation review rollup with paper_scores top-k overlap for bridge experiment gating."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.recommendation_review_worksheet import cluster_version_from_config


class BridgeExperimentReadinessError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


_PROVENANCE_KEYS = (
    "ranking_run_id",
    "ranking_version",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise BridgeExperimentReadinessError(f"Rollup file not found: {path}", code=2)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BridgeExperimentReadinessError(f"Invalid JSON in rollup file: {path}", code=2) from exc


def _rollup_str(rollup: dict[str, Any], key: str) -> str:
    prov = rollup.get("provenance")
    if not isinstance(prov, dict):
        raise BridgeExperimentReadinessError("Rollup missing provenance object.", code=2)
    v = prov.get(key)
    if not isinstance(v, str) or not v.strip():
        raise BridgeExperimentReadinessError(f"Rollup provenance.{key} missing or not a string.", code=2)
    return v.strip()


def _family_metrics(rollup: dict[str, Any], family: str) -> dict[str, Any]:
    per = rollup.get("per_family")
    if not isinstance(per, dict):
        raise BridgeExperimentReadinessError("Rollup missing per_family object.", code=2)
    block = per.get(family)
    if not isinstance(block, dict):
        raise BridgeExperimentReadinessError(f"Rollup missing per_family.{family}.", code=2)
    m = block.get("metrics")
    if not isinstance(m, dict):
        raise BridgeExperimentReadinessError(f"Rollup missing per_family.{family}.metrics.", code=2)
    return m


def extract_label_metrics_from_rollup(rollup: dict[str, Any]) -> dict[str, float | None]:
    bridge_m = _family_metrics(rollup, "bridge")
    emerging_m = _family_metrics(rollup, "emerging")
    under_m = _family_metrics(rollup, "undercited")
    for k in ("precision_at_k_good_only", "precision_at_k_good_or_acceptable", "bridge_like_yes_or_partial_share"):
        if k not in bridge_m:
            raise BridgeExperimentReadinessError(f"bridge metrics missing {k!r}.", code=2)
    for fam_name, m in ("emerging", emerging_m), ("undercited", under_m):
        if "precision_at_k_good_only" not in m:
            raise BridgeExperimentReadinessError(
                f"{fam_name} metrics missing precision_at_k_good_only.",
                code=2,
            )

    def _f(x: Any) -> float | None:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        raise BridgeExperimentReadinessError("rollup metric is not numeric.", code=2)

    return {
        "bridge_good_only_precision": _f(bridge_m.get("precision_at_k_good_only")),
        "bridge_good_or_acceptable_precision": _f(bridge_m.get("precision_at_k_good_or_acceptable")),
        "bridge_like_yes_or_partial_share": _f(bridge_m.get("bridge_like_yes_or_partial_share")),
        "emerging_good_only_precision": _f(emerging_m.get("precision_at_k_good_only")),
        "undercited_good_only_precision": _f(under_m.get("precision_at_k_good_only")),
    }


def top_k_work_ids_sql_fragment(*, bridge_eligible_true_only: bool) -> str:
    """SQL body contract tests assert on (FROM paper_scores … ORDER BY …)."""
    elig = "          AND ps.bridge_eligible IS TRUE\n" if bridge_eligible_true_only else ""
    return (
        "FROM paper_scores ps\n"
        "        WHERE ps.ranking_run_id = %s\n"
        "          AND ps.recommendation_family = %s\n"
        f"{elig}"
        "        ORDER BY ps.final_score DESC, ps.work_id ASC\n"
        "        LIMIT %s"
    )


def fetch_top_k_work_ids(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    family: str,
    k: int,
    bridge_eligible_true_only: bool,
) -> list[int]:
    if bridge_eligible_true_only and family != "bridge":
        raise BridgeExperimentReadinessError(
            "bridge_eligible_true_only applies only to recommendation_family='bridge'.",
            code=2,
        )
    sql = f"""
        SELECT ps.work_id
        {top_k_work_ids_sql_fragment(bridge_eligible_true_only=bridge_eligible_true_only)}
    """
    rows = conn.execute(sql, (ranking_run_id, family, k)).fetchall()
    out: list[int] = []
    for r in rows:
        wid = r["work_id"]
        if wid is None:
            continue
        out.append(int(wid))
    return out


def overlap_count_and_jaccard(ids_a: Iterable[int], ids_b: Iterable[int]) -> tuple[int, int, float]:
    set_a = set(ids_a)
    set_b = set(ids_b)
    inter = set_a & set_b
    union = set_a | set_b
    overlap = len(inter)
    if not union:
        jaccard = 1.0
    else:
        jaccard = round(len(inter) / len(union), 6)
    return overlap, len(union), float(jaccard)


def _overlap_block(ids_a: list[int], ids_b: list[int]) -> dict[str, Any]:
    o, u, j = overlap_count_and_jaccard(ids_a, ids_b)
    return {"overlap_count": o, "union_count": u, "jaccard": j}


def compute_readiness_flags(
    *,
    label_metrics: dict[str, float | None],
    full_bridge_top_k: list[int],
    eligible_only_bridge_top_k: list[int],
    full_vs_emerging_jaccard: float,
    eligible_vs_emerging_jaccard: float,
) -> dict[str, Any]:
    bgoa = label_metrics.get("bridge_good_or_acceptable_precision")
    blike = label_metrics.get("bridge_like_yes_or_partial_share")
    label_quality_ready = bool(
        isinstance(bgoa, float)
        and bgoa >= 0.8
        and isinstance(blike, float)
        and blike >= 0.5
    )
    eligible_differs_from_full = set(full_bridge_top_k) != set(eligible_only_bridge_top_k)
    emerging_overlap_delta = round(float(full_vs_emerging_jaccard) - float(eligible_vs_emerging_jaccard), 6)
    materially_lower_emerging_overlap = emerging_overlap_delta >= 0.10
    distinctness_ready = bool(eligible_differs_from_full and materially_lower_emerging_overlap)
    ready_for_small_bridge_weight_experiment = bool(label_quality_ready and distinctness_ready)

    if ready_for_small_bridge_weight_experiment:
        suggested = "Candidate for a small gated bridge-weight experiment; not validation."
    elif label_quality_ready and not distinctness_ready:
        suggested = (
            "Bridge labels are promising, but distinctness is not yet strong enough for a weight experiment."
        )
    else:
        suggested = (
            "Defer a bridge-weight experiment: strengthen label quality and/or bridge-vs-emerging separation evidence."
        )

    return {
        "label_quality_ready": label_quality_ready,
        "distinctness_ready": distinctness_ready,
        "ready_for_small_bridge_weight_experiment": ready_for_small_bridge_weight_experiment,
        "suggested_next_step": suggested,
        "eligible_only_bridge_differs_from_full_bridge": eligible_differs_from_full,
        "emerging_overlap_delta": emerging_overlap_delta,
        "materially_lower_emerging_overlap": materially_lower_emerging_overlap,
    }


def markdown_from_readiness(payload: dict[str, Any]) -> str:
    prov = payload.get("provenance", {})
    overlaps = payload.get("overlaps", {})
    thr = payload.get("overlap_thresholds", {})
    lm = payload.get("label_metrics", {})
    rd = payload.get("readiness", {})
    lines: list[str] = [
        "# Bridge experiment readiness",
        "",
        "This artifact joins a completed recommendation review rollup with `paper_scores` top-k overlap for one "
        "explicit `ranking_run_id`. It is **not** validation of bridge ranking quality and does **not** prove that "
        "ML ranking is better.",
        "",
        "## Provenance",
        "",
    ]
    for k in _PROVENANCE_KEYS:
        lines.append(f"- **{k}:** `{prov.get(k, '')}`")
    lines.append(f"- **k:** `{prov.get('k', '')}`")
    lines.extend(
        [
            "",
            "## Operational threshold (smoke evaluation)",
            "",
            f"- **emerging_overlap_delta:** `{thr.get('emerging_overlap_delta')}` "
            f"(full_bridge_vs_emerging_jaccard − eligible_only_bridge_vs_emerging_jaccard)",
            f"- **materially_lower_emerging_overlap:** `{thr.get('materially_lower_emerging_overlap')}` "
            f"(true when delta ≥ `{thr.get('required_delta')}`)",
            "",
            "The 0.10 Jaccard delta is an **operational threshold for this smoke evaluation**, not a universal "
            "statistical cutoff.",
            "",
            "## Label metrics (from rollup)",
            "",
            f"- **bridge_good_only_precision:** `{lm.get('bridge_good_only_precision')}`",
            f"- **bridge_good_or_acceptable_precision:** `{lm.get('bridge_good_or_acceptable_precision')}`",
            f"- **bridge_like_yes_or_partial_share:** `{lm.get('bridge_like_yes_or_partial_share')}`",
            f"- **emerging_good_only_precision:** `{lm.get('emerging_good_only_precision')}`",
            f"- **undercited_good_only_precision:** `{lm.get('undercited_good_only_precision')}`",
            "",
            "## Top-k overlap (Jaccard on work_id sets)",
            "",
        ]
    )
    for title, key in (
        ("full_bridge vs emerging", "full_bridge_vs_emerging"),
        ("full_bridge vs undercited", "full_bridge_vs_undercited"),
        ("emerging vs undercited", "emerging_vs_undercited"),
        ("full_bridge vs eligible_only_bridge", "full_bridge_vs_eligible_only_bridge"),
        ("eligible_only_bridge vs emerging", "eligible_only_bridge_vs_emerging"),
    ):
        block = overlaps.get(key) or {}
        lines.append(
            f"- **{title}:** overlap={block.get('overlap_count')}, union={block.get('union_count')}, "
            f"jaccard={block.get('jaccard')}"
        )
    lines.extend(
        [
            "",
            "## Readiness (conservative go / no-go)",
            "",
            f"- **label_quality_ready:** `{rd.get('label_quality_ready')}`",
            f"- **distinctness_ready:** `{rd.get('distinctness_ready')}`",
            f"- **ready_for_small_bridge_weight_experiment:** `{rd.get('ready_for_small_bridge_weight_experiment')}`",
            "",
            "### Suggested next step",
            "",
            f"- {rd.get('suggested_next_step', '')}",
            "",
            "## Remaining gap before validation-grade evidence",
            "",
            "- Human rollup here is single-reviewer and small-n; it is not a reproducible benchmark.",
            "- Even strong overlap separation is not causal evidence that a weight change will improve user outcomes.",
            "- Validation would require multi-reviewer agreement, held-out runs, and product-facing evaluation — not "
            "this artifact alone.",
            "",
        ]
    )
    warns = payload.get("warnings") or []
    if warns:
        lines.append("## Warnings")
        lines.append("")
        for w in warns:
            lines.append(f"- {w}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


@dataclass(frozen=True)
class _RunRow:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    config_json: Any
    status: str


def _load_run_row(conn: psycopg.Connection, *, ranking_run_id: str) -> _RunRow:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise BridgeExperimentReadinessError(f"ranking_run_id not found: {ranking_run_id!r}", code=2)
    if str(row["status"]) != "succeeded":
        raise BridgeExperimentReadinessError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={row['status']!r}).",
            code=2,
        )
    return _RunRow(
        ranking_run_id=str(row["ranking_run_id"]),
        ranking_version=str(row["ranking_version"]),
        corpus_snapshot_version=str(row["corpus_snapshot_version"]),
        embedding_version=str(row["embedding_version"]),
        config_json=row.get("config_json"),
        status=str(row["status"]),
    )


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


def _assert_rollup_matches_db_provenance(rollup: dict[str, Any], *, run: _RunRow) -> None:
    cfg = _parse_config_json(run.config_json)
    db_cluster = cluster_version_from_config(cfg) or ""
    db_prov = {
        "ranking_run_id": run.ranking_run_id,
        "ranking_version": run.ranking_version,
        "corpus_snapshot_version": run.corpus_snapshot_version,
        "embedding_version": run.embedding_version,
        "cluster_version": db_cluster,
    }
    for key in _PROVENANCE_KEYS:
        expected = _rollup_str(rollup, key)
        actual = str(db_prov[key])
        if actual != expected:
            raise BridgeExperimentReadinessError(
                f"Provenance mismatch for rollup vs database on {key!r}: rollup={expected!r}, db={actual!r}.",
                code=2,
            )


def build_bridge_experiment_readiness_payload(
    conn: psycopg.Connection,
    *,
    rollup: dict[str, Any],
    ranking_run_id: str,
    k: int,
) -> dict[str, Any]:
    rid = str(ranking_run_id).strip()
    if not rid:
        raise BridgeExperimentReadinessError("--ranking-run-id is required and must not be blank", code=2)
    if k < 1 or k > 200:
        raise BridgeExperimentReadinessError("--k must be between 1 and 200", code=2)

    rollup_rid = _rollup_str(rollup, "ranking_run_id")
    if rollup_rid != rid:
        raise BridgeExperimentReadinessError(
            f"--ranking-run-id {rid!r} does not match rollup provenance.ranking_run_id={rollup_rid!r}.",
            code=2,
        )

    run = _load_run_row(conn, ranking_run_id=rid)
    _assert_rollup_matches_db_provenance(rollup, run=run)

    full_bridge = fetch_top_k_work_ids(
        conn, ranking_run_id=rid, family="bridge", k=k, bridge_eligible_true_only=False
    )
    eligible_bridge = fetch_top_k_work_ids(
        conn, ranking_run_id=rid, family="bridge", k=k, bridge_eligible_true_only=True
    )
    emerging = fetch_top_k_work_ids(conn, ranking_run_id=rid, family="emerging", k=k, bridge_eligible_true_only=False)
    undercited = fetch_top_k_work_ids(
        conn, ranking_run_id=rid, family="undercited", k=k, bridge_eligible_true_only=False
    )

    ob_fe = _overlap_block(full_bridge, emerging)
    ob_fu = _overlap_block(full_bridge, undercited)
    ob_eu = _overlap_block(emerging, undercited)
    ob_fel = _overlap_block(full_bridge, eligible_bridge)
    ob_ee = _overlap_block(eligible_bridge, emerging)

    label_metrics = extract_label_metrics_from_rollup(rollup)
    flags = compute_readiness_flags(
        label_metrics=label_metrics,
        full_bridge_top_k=full_bridge,
        eligible_only_bridge_top_k=eligible_bridge,
        full_vs_emerging_jaccard=float(ob_fe["jaccard"]),
        eligible_vs_emerging_jaccard=float(ob_ee["jaccard"]),
    )

    warnings: list[str] = []
    if len(full_bridge) < k:
        warnings.append(f"full bridge top-k returned {len(full_bridge)} rows (< k={k}); overlap metrics are partial")
    if len(eligible_bridge) < k:
        warnings.append(
            f"eligible-only bridge top-k returned {len(eligible_bridge)} rows (< k={k}); "
            "eligible head may be sparse"
        )
    if len(emerging) < k:
        warnings.append(f"emerging top-k returned {len(emerging)} rows (< k={k}); overlap metrics are partial")
    if len(undercited) < k:
        warnings.append(f"undercited top-k returned {len(undercited)} rows (< k={k}); overlap metrics are partial")

    eligible_differs = bool(flags["eligible_only_bridge_differs_from_full_bridge"])
    emerging_delta = float(flags["emerging_overlap_delta"])
    materially_lower = bool(flags["materially_lower_emerging_overlap"])

    provenance = {key: _rollup_str(rollup, key) for key in _PROVENANCE_KEYS}
    provenance["k"] = k

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": provenance,
        "top_k_ids": {
            "full_bridge": full_bridge,
            "eligible_only_bridge": eligible_bridge,
            "emerging": emerging,
            "undercited": undercited,
        },
        "overlaps": {
            "full_bridge_vs_emerging": ob_fe,
            "full_bridge_vs_undercited": ob_fu,
            "emerging_vs_undercited": ob_eu,
            "full_bridge_vs_eligible_only_bridge": ob_fel,
            "eligible_only_bridge_vs_emerging": ob_ee,
        },
        "overlap_thresholds": {
            "emerging_overlap_delta": emerging_delta,
            "materially_lower_emerging_overlap": materially_lower,
            "required_delta": 0.10,
            "eligible_only_bridge_differs_from_full_bridge": eligible_differs,
        },
        "label_metrics": {
            "bridge_good_only_precision": label_metrics["bridge_good_only_precision"],
            "bridge_good_or_acceptable_precision": label_metrics["bridge_good_or_acceptable_precision"],
            "bridge_like_yes_or_partial_share": label_metrics["bridge_like_yes_or_partial_share"],
            "emerging_good_only_precision": label_metrics["emerging_good_only_precision"],
            "undercited_good_only_precision": label_metrics["undercited_good_only_precision"],
        },
        "readiness": {
            "label_quality_ready": flags["label_quality_ready"],
            "distinctness_ready": flags["distinctness_ready"],
            "ready_for_small_bridge_weight_experiment": flags["ready_for_small_bridge_weight_experiment"],
            "suggested_next_step": flags["suggested_next_step"],
        },
        "warnings": warnings,
    }


def run_bridge_experiment_readiness(
    *,
    rollup_path: Path,
    ranking_run_id: str,
    k: int,
    output_path: Path,
    markdown_path: Path | None,
    database_url: str | None,
) -> dict[str, Any]:
    rollup = _read_json(rollup_path)
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_bridge_experiment_readiness_payload(conn, rollup=rollup, ranking_run_id=ranking_run_id, k=k)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_readiness(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeExperimentReadinessError",
    "build_bridge_experiment_readiness_payload",
    "compute_readiness_flags",
    "extract_label_metrics_from_rollup",
    "fetch_top_k_work_ids",
    "markdown_from_readiness",
    "overlap_count_and_jaccard",
    "run_bridge_experiment_readiness",
    "top_k_work_ids_sql_fragment",
]
