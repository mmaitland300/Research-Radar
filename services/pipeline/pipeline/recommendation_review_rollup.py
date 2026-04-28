"""Roll up completed recommendation review family summaries into one run-level report."""

from __future__ import annotations

import json
import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ReviewRollupError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _LoadedSummary:
    path: Path
    data: dict[str, Any]
    family: str


_PROVENANCE_KEYS = (
    "ranking_run_id",
    "ranking_version",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
)

_METRIC_KEYS = (
    "precision_at_k_good_only",
    "precision_at_k_good_or_acceptable",
    "bridge_like_yes_or_partial_share",
    "surprising_or_useful_share",
)


_DISTINCTNESS_KEYS = (
    "bridge_vs_emerging_jaccard",
    "eligible_bridge_vs_emerging_jaccard",
    "emerging_overlap_delta_from_full_to_eligible",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ReviewRollupError(f"Summary file not found: {path}", code=2)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ReviewRollupError(f"Invalid JSON in summary file: {path}", code=2) from exc


def _single_str_list(v: Any, *, field: str, source: Path) -> str:
    if not isinstance(v, list) or len(v) != 1 or not isinstance(v[0], str):
        raise ReviewRollupError(
            f"Summary {source} has invalid {field!r}; expected one string value.",
            code=2,
        )
    return v[0]


def _load_one(path: Path, data: dict[str, Any]) -> _LoadedSummary:
    is_complete = data.get("is_complete")
    if is_complete is not True:
        raise ReviewRollupError(
            f"Summary {path} is not complete (is_complete={is_complete!r}).",
            code=2,
        )
    fam = _single_str_list(data.get("family"), field="family", source=path)
    metrics = data.get("metrics")
    if not isinstance(metrics, dict):
        raise ReviewRollupError(f"Summary {path} missing metrics object.", code=2)
    for k in _METRIC_KEYS:
        if k not in metrics:
            raise ReviewRollupError(f"Summary {path} missing metrics.{k}.", code=2)
    for k in _PROVENANCE_KEYS:
        _single_str_list(data.get(k), field=k, source=path)
    return _LoadedSummary(path=path, data=data, family=fam)


def _validate_expected_families(by_family: dict[str, dict[str, Any]]) -> None:
    expected = {"bridge", "emerging", "undercited"}
    got = set(by_family.keys())
    if got != expected:
        raise ReviewRollupError(
            f"Expected exactly families {sorted(expected)}, got {sorted(got)}.",
            code=2,
        )


def _read_worksheet_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ReviewRollupError(f"Bridge worksheet not found: {path}", code=2)
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ReviewRollupError(f"Bridge worksheet has no header: {path}", code=2)
    rows: list[dict[str, str]] = []
    for row in reader:
        if row is None:
            continue
        rows.append({str(k or "").strip(): str(v or "").strip() for k, v in row.items()})
    if not rows:
        raise ReviewRollupError("Bridge worksheet has no data rows.", code=2)
    return rows


def _validate_bridge_worksheet(
    *,
    worksheet_rows: list[dict[str, str]],
    expected_ranking_run_id: str,
) -> dict[str, Any]:
    variants = {r.get("review_pool_variant", "") for r in worksheet_rows}
    if variants != {"bridge_eligible_only"}:
        raise ReviewRollupError(
            f"Bridge worksheet review_pool_variant must be bridge_eligible_only for all rows; got {sorted(variants)}",
            code=2,
        )
    bad_family = {r.get("family", "") for r in worksheet_rows if r.get("family", "") != "bridge"}
    if bad_family:
        raise ReviewRollupError("Bridge worksheet includes non-bridge family rows.", code=2)
    bad_run = {r.get("ranking_run_id", "") for r in worksheet_rows if r.get("ranking_run_id", "") != expected_ranking_run_id}
    if bad_run:
        raise ReviewRollupError(
            f"Bridge worksheet includes rows not matching ranking_run_id={expected_ranking_run_id!r}.",
            code=2,
        )
    invalid_eligible = [r.get("bridge_eligible", "") for r in worksheet_rows if r.get("bridge_eligible", "") != "true"]
    if invalid_eligible:
        raise ReviewRollupError(
            "Bridge worksheet contains bridge_eligible values that are not true; eligible-only pool is invalid.",
            code=2,
        )
    return {
        "row_count": len(worksheet_rows),
        "review_pool_variant": "bridge_eligible_only",
        "all_bridge_rows_are_bridge_eligible_true": True,
    }


def _extract_distinctness(diagnostics: dict[str, Any]) -> dict[str, Any]:
    ov = diagnostics.get("overlap_detail")
    diag = diagnostics.get("diagnosis")
    if not isinstance(ov, dict):
        raise ReviewRollupError("Bridge diagnostics missing overlap_detail.", code=2)
    if not isinstance(diag, dict):
        raise ReviewRollupError("Bridge diagnostics missing diagnosis.", code=2)
    for key in _DISTINCTNESS_KEYS:
        if key not in ov:
            raise ReviewRollupError(f"Bridge diagnostics missing overlap_detail.{key}.", code=2)
    for key in (
        "eligible_head_differs_from_full",
        "eligible_head_less_emerging_like_than_full",
        "eligible_distinctness_improves_by_threshold",
    ):
        if key not in diag:
            raise ReviewRollupError(f"Bridge diagnostics missing diagnosis.{key}.", code=2)
    return {
        "full_bridge_vs_emerging_jaccard": float(ov["bridge_vs_emerging_jaccard"]),
        "eligible_bridge_vs_emerging_jaccard": float(ov["eligible_bridge_vs_emerging_jaccard"]),
        "emerging_overlap_delta_from_full_to_eligible": float(ov["emerging_overlap_delta_from_full_to_eligible"]),
        "eligible_head_differs_from_full": bool(diag["eligible_head_differs_from_full"]),
        "eligible_head_less_emerging_like_than_full": bool(diag["eligible_head_less_emerging_like_than_full"]),
        "eligible_distinctness_improves_by_threshold": bool(diag["eligible_distinctness_improves_by_threshold"]),
    }


def _pairwise_deltas(
    by_family: dict[str, dict[str, Any]],
) -> dict[str, dict[str, float | None]]:
    fams = sorted(by_family.keys())
    out: dict[str, dict[str, float | None]] = {}
    for mk in _METRIC_KEYS:
        row: dict[str, float | None] = {}
        for i in range(len(fams)):
            for j in range(i + 1, len(fams)):
                a, b = fams[i], fams[j]
                va = by_family[a]["metrics"].get(mk)
                vb = by_family[b]["metrics"].get(mk)
                key = f"{a}_minus_{b}"
                if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                    row[key] = float(va) - float(vb)
                else:
                    row[key] = None
        out[mk] = row
    return out


def build_recommendation_review_rollup(
    summaries: list[dict[str, Any]],
    *,
    source_paths: list[Path],
    bridge_diagnostics: dict[str, Any] | None = None,
    bridge_worksheet_rows: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    if len(summaries) != len(source_paths):
        raise ReviewRollupError("Internal error: source_paths length mismatch.", code=2)
    if not summaries:
        raise ReviewRollupError("At least one --summary file is required.", code=2)

    loaded: list[_LoadedSummary] = [
        _load_one(p, d) for p, d in zip(source_paths, summaries, strict=True)
    ]
    if len(loaded) != len(summaries):
        raise ReviewRollupError("Internal error: loaded summary count mismatch.", code=2)

    by_family: dict[str, dict[str, Any]] = {}
    for x in loaded:
        if x.family in by_family:
            raise ReviewRollupError(
                f"Duplicate family {x.family!r} across summaries ({x.path}).",
                code=2,
            )
        by_family[x.family] = x.data

    _validate_expected_families(by_family)

    first = loaded[0]
    for key in _PROVENANCE_KEYS:
        expected = _single_str_list(first.data.get(key), field=key, source=first.path)
        for x in loaded[1:]:
            actual = _single_str_list(x.data.get(key), field=key, source=x.path)
            if actual != expected:
                raise ReviewRollupError(
                    f"Provenance mismatch for {key!r}: {first.path}={expected!r}, {x.path}={actual!r}",
                    code=2,
                )

    families_present = sorted(by_family.keys())
    warnings: list[str] = []
    if "bridge" not in by_family:
        warnings.append(
            "bridge family summary missing; bridge-specific metrics and weight readiness are limited"
        )
    if "emerging" not in by_family:
        warnings.append("emerging family summary missing; distinctness readiness is limited")
    if "undercited" not in by_family:
        warnings.append("undercited family summary missing; hard-pool quality signal is missing")

    good_only_values: dict[str, float] = {}
    for fam, s in by_family.items():
        v = s.get("metrics", {}).get("precision_at_k_good_only")
        if not isinstance(v, (int, float)):
            raise ReviewRollupError(
                f"Summary for family {fam!r} has non-numeric precision_at_k_good_only.",
                code=2,
            )
        good_only_values[fam] = float(v)
    best_family = max(good_only_values, key=good_only_values.get)
    weakest_family = min(good_only_values, key=good_only_values.get)

    bridge_share: float | None = None
    bridge_good_only: float | None = None
    bridge_good_or_acc: float | None = None
    if "bridge" in by_family:
        bm = by_family["bridge"]["metrics"]
        if isinstance(bm.get("bridge_like_yes_or_partial_share"), (int, float)):
            bridge_share = float(bm["bridge_like_yes_or_partial_share"])
        if isinstance(bm.get("precision_at_k_good_only"), (int, float)):
            bridge_good_only = float(bm["precision_at_k_good_only"])
        if isinstance(bm.get("precision_at_k_good_or_acceptable"), (int, float)):
            bridge_good_or_acc = float(bm["precision_at_k_good_or_acceptable"])

    family_quality_context_ready = bool(
        set(families_present) == {"bridge", "emerging", "undercited"}
        and all(bool(by_family[f].get("is_complete")) for f in ("bridge", "emerging", "undercited"))
    )
    label_quality_ready = bool(bridge_good_or_acc is not None and bridge_good_or_acc >= 0.80)
    bridge_like_ready = bool(bridge_share is not None and bridge_share >= 0.50)
    distinctness: dict[str, Any] | None = None
    if bridge_diagnostics is not None:
        distinctness = _extract_distinctness(bridge_diagnostics)
    distinctness_ready = False
    if distinctness is not None:
        distinctness_ready = bool(
            distinctness["eligible_bridge_vs_emerging_jaccard"]
            <= (distinctness["full_bridge_vs_emerging_jaccard"] - 0.10)
        )
    ready_for_weight_experiment = bool(
        label_quality_ready and bridge_like_ready and distinctness_ready and family_quality_context_ready
    )
    failed_gates: list[str] = []
    if not label_quality_ready:
        failed_gates.append("label_quality_ready")
    if not bridge_like_ready:
        failed_gates.append("bridge_like_ready")
    if not distinctness_ready:
        failed_gates.append("distinctness_ready")
    if not family_quality_context_ready:
        failed_gates.append("family_quality_context_ready")
    if ready_for_weight_experiment:
        suggested = "Candidate for a small gated bridge-weight experiment; not validation."
    else:
        suggested = "Not ready for bridge-weight experiment; failed gates: " + ", ".join(failed_gates)

    per_family: dict[str, Any] = {}
    for fam in families_present:
        s = by_family[fam]
        per_family[fam] = {
            "input_path": s.get("input_path"),
            "row_count": s.get("row_count"),
            "is_complete": s.get("is_complete"),
            "metrics": s.get("metrics"),
            "label_counts": s.get("label_counts"),
            "warnings": s.get("warnings") or [],
        }

    provenance = {
        key: _single_str_list(first.data.get(key), field=key, source=first.path)
        for key in _PROVENANCE_KEYS
    }
    out: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": provenance,
        "family_count": len(families_present),
        "families_present": families_present,
        "per_family": per_family,
        "cross_family_metric_deltas": _pairwise_deltas(by_family),
        "best_good_only_family": best_family,
        "weakest_good_only_family": weakest_family,
        "bridge_specific": {
            "bridge_like_yes_or_partial_share": bridge_share,
            "bridge_good_only_precision": bridge_good_only,
        },
        "warnings": warnings,
        "readiness": {
            "ready_for_distinctness_analysis": distinctness is not None,
            "ready_for_weight_experiment": ready_for_weight_experiment,
            "label_quality_ready": label_quality_ready,
            "bridge_like_ready": bridge_like_ready,
            "distinctness_ready": distinctness_ready,
            "family_quality_context_ready": family_quality_context_ready,
            "ready_for_small_bridge_weight_experiment": ready_for_weight_experiment,
            "failed_gates": failed_gates,
            "suggested_next_step": suggested,
        },
    }
    if distinctness is not None:
        out["bridge_distinctness"] = distinctness
    if bridge_worksheet_rows is not None:
        out["bridge_review_pool_validation"] = _validate_bridge_worksheet(
            worksheet_rows=bridge_worksheet_rows,
            expected_ranking_run_id=provenance["ranking_run_id"],
        )
    return out


def _fmt_num(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def markdown_from_rollup(rollup: dict[str, Any]) -> str:
    lines = [
        "# Recommendation review rollup",
        "",
        "## Provenance",
        "",
    ]
    prov = rollup.get("provenance", {})
    for k in _PROVENANCE_KEYS:
        lines.append(f"- **{k}:** `{prov.get(k, '')}`")
    lines.extend(["", "## Family metrics", ""])
    lines.append("| Family | P@k good-only | P@k good/acceptable | Bridge-like yes/partial | Surprising/useful |")
    lines.append("| --- | --- | --- | --- | --- |")
    for fam in rollup.get("families_present", []):
        m = (rollup.get("per_family", {}).get(fam) or {}).get("metrics", {})
        lines.append(
            f"| {fam} | {_fmt_num(m.get('precision_at_k_good_only'))} | "
            f"{_fmt_num(m.get('precision_at_k_good_or_acceptable'))} | "
            f"{_fmt_num(m.get('bridge_like_yes_or_partial_share'))} | "
            f"{_fmt_num(m.get('surprising_or_useful_share'))} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            f"- Best good-only family: **{rollup.get('best_good_only_family')}**",
            f"- Weakest good-only family: **{rollup.get('weakest_good_only_family')}**",
            f"- Ready for distinctness analysis: **{rollup.get('readiness', {}).get('ready_for_distinctness_analysis')}**",
            f"- Ready for weight experiment: **{rollup.get('readiness', {}).get('ready_for_weight_experiment')}**",
            "",
            "## Readiness gates",
            "",
            f"- label_quality_ready: **{rollup.get('readiness', {}).get('label_quality_ready')}**",
            f"- bridge_like_ready: **{rollup.get('readiness', {}).get('bridge_like_ready')}**",
            f"- distinctness_ready: **{rollup.get('readiness', {}).get('distinctness_ready')}**",
            f"- family_quality_context_ready: **{rollup.get('readiness', {}).get('family_quality_context_ready')}**",
            f"- ready_for_small_bridge_weight_experiment: **{rollup.get('readiness', {}).get('ready_for_small_bridge_weight_experiment')}**",
            "",
            "## Evidence caveat",
            "",
            "- Single-reviewer, top-20, offline evidence.",
            "- This rollup does not prove bridge ranking superiority.",
            "- This rollup is not validation; it is a conservative gating artifact.",
            "",
            "## Limitations",
            "",
            "- Single-reviewer labels can be noisy; treat as directional evidence.",
            "- Small curated corpus can saturate relevance and novelty metrics.",
            "- This rollup is not run-to-run validation and does not prove weight effectiveness.",
            "",
            "## Suggested next step",
            "",
            f"- {rollup.get('readiness', {}).get('suggested_next_step', '')}",
            "",
        ]
    )
    bd = rollup.get("bridge_distinctness")
    if isinstance(bd, dict):
        lines.extend(
            [
                "## Bridge distinctness",
                "",
                f"- full_bridge_vs_emerging_jaccard: `{bd.get('full_bridge_vs_emerging_jaccard')}`",
                f"- eligible_bridge_vs_emerging_jaccard: `{bd.get('eligible_bridge_vs_emerging_jaccard')}`",
                f"- emerging_overlap_delta_from_full_to_eligible: `{bd.get('emerging_overlap_delta_from_full_to_eligible')}`",
                f"- eligible_head_differs_from_full: `{bd.get('eligible_head_differs_from_full')}`",
                f"- eligible_head_less_emerging_like_than_full: `{bd.get('eligible_head_less_emerging_like_than_full')}`",
                f"- eligible_distinctness_improves_by_threshold: `{bd.get('eligible_distinctness_improves_by_threshold')}`",
                "",
            ]
        )
    for w in rollup.get("warnings") or []:
        lines.append(f"- Warning: {w}")
    return "\n".join(lines).rstrip() + "\n"


def run_recommendation_review_rollup(
    *,
    summary_paths: list[Path],
    output_path: Path,
    markdown_path: Path | None,
    bridge_diagnostics_path: Path | None = None,
    bridge_worksheet_path: Path | None = None,
) -> None:
    loaded_data = [_read_json(p) for p in summary_paths]
    bridge_diagnostics = _read_json(bridge_diagnostics_path) if bridge_diagnostics_path else None
    bridge_worksheet_rows = _read_worksheet_rows(bridge_worksheet_path) if bridge_worksheet_path else None
    rollup = build_recommendation_review_rollup(
        loaded_data,
        source_paths=summary_paths,
        bridge_diagnostics=bridge_diagnostics,
        bridge_worksheet_rows=bridge_worksheet_rows,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(rollup, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_rollup(rollup), encoding="utf-8", newline="\n"
        )


__all__ = [
    "ReviewRollupError",
    "build_recommendation_review_rollup",
    "markdown_from_rollup",
    "run_recommendation_review_rollup",
]

