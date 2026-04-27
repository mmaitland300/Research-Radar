"""Roll up completed recommendation review family summaries into one run-level report."""

from __future__ import annotations

import json
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

    ready_for_distinctness_analysis = (
        "bridge" in by_family and "emerging" in by_family
    )
    ready_for_weight_experiment = bool(
        bridge_good_or_acc is not None
        and bridge_good_or_acc >= 0.8
        and bridge_share is not None
        and bridge_share >= 0.5
    )
    if ready_for_weight_experiment:
        suggested = (
            "candidate signal only (not validation): join this rollup with bridge distinctness and top-k overlap before any small weight experiment"
        )
    elif ready_for_distinctness_analysis:
        suggested = (
            "run distinctness and overlap analysis first; defer positive bridge weight until separation evidence is explicit"
        )
    else:
        suggested = "complete missing family summaries before distinctness or weight decisions"

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
    return {
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
            "ready_for_distinctness_analysis": ready_for_distinctness_analysis,
            "ready_for_weight_experiment": ready_for_weight_experiment,
            "suggested_next_step": suggested,
        },
    }


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
    for w in rollup.get("warnings") or []:
        lines.append(f"- Warning: {w}")
    return "\n".join(lines).rstrip() + "\n"


def run_recommendation_review_rollup(
    *,
    summary_paths: list[Path],
    output_path: Path,
    markdown_path: Path | None,
) -> None:
    loaded_data = [_read_json(p) for p in summary_paths]
    rollup = build_recommendation_review_rollup(loaded_data, source_paths=summary_paths)
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

