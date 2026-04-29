"""Read-only bridge eligibility failure analysis from frozen JSON artifacts.

Imports are restricted to the Python standard library plus ``json``, ``pathlib``,
and ``dataclasses`` (per audit contract). No database access.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from pathlib import Path
from typing import Any, Final

VERBATIM_CAVEATS: Final[tuple[str, ...]] = (
    "Diagnostic only; not validation; no ranking/default/product claim.",
    "No new eligibility rule should be selected without a follow-up zero-weight ranking run and labels.",
)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _top_k_hash(ids: list[Any]) -> str:
    body = json.dumps(ids, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    try:
        return float(statistics.correlation(xs, ys))
    except statistics.StatisticsError:
        return None


def _nonstrictly_increasing(values: list[float]) -> bool:
    return all(values[i] <= values[i + 1] for i in range(len(values) - 1))


def _variant_bridge_percentile_jaccards(variants: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for v in variants:
        vid = str(v.get("variant_id", ""))
        if vid in (
            "bridge_score_top_75pct",
            "bridge_score_top_50pct",
            "bridge_score_top_25pct",
        ):
            out[vid] = float(v["variant_vs_emerging_jaccard"])
    return out


def _percentile_monotonic_distinctness_loss(pct: dict[str, float]) -> bool | None:
    """Loosening bridge_score cutoff (25pct strict → 50 → 75) should not improve distinctness: Jaccard non-decreasing."""
    j25 = pct.get("bridge_score_top_25pct")
    j50 = pct.get("bridge_score_top_50pct")
    j75 = pct.get("bridge_score_top_75pct")
    if j25 is None or j50 is None or j75 is None:
        return None
    return _nonstrictly_increasing([j25, j50, j75])


def _recommendation_for_cause(cause: str) -> str:
    if cause in ("tied_variants_collapse_to_same_top_20", "structural_bridge_emerging_intersection"):
        return (
            "Redefine the bridge objective to penalize high final_score(emerging) (for example, "
            "bridge_eligible := cross_cluster_share >= X AND work_id NOT IN emerging_top_k_50); "
            "do not pick a threshold variant from this sweep."
        )
    if cause == "threshold_too_weak":
        return (
            "Propose a new feature beyond the current four (for example, neighbor cluster diversity score, "
            "anchor cluster minority share)."
        )
    if cause == "cohort_collapse":
        return "Keep the current rule; increase top-k label depth instead."
    if cause == "correlation_dominant":
        return (
            "Propose a residualized bridge score (bridge_score minus an emerging_proxy) before any new variant is added."
        )
    return "Review inputs and extend the classifier if this artifact should be supported."


def analyze_bridge_eligibility_failure(
    sensitivity: dict[str, Any],
    signal_diagnostics: dict[str, Any],
    weight_response: dict[str, Any],
    labeled_outcome: dict[str, Any],
) -> dict[str, Any]:
    variants = list(sensitivity.get("variants") or [])
    emerging = list(sensitivity.get("baseline", {}).get("emerging_top_k_ids") or [])
    emerging_set = set(emerging)

    jaccards = [float(v["variant_vs_emerging_jaccard"]) for v in variants]
    baseline_minimum = min(jaccards) if jaccards else 0.0

    existing = next((v for v in variants if v.get("variant_id") == "existing_bridge_eligible"), None)
    baseline_top_k: list[Any] = list(existing["eligible_top_k_ids"]) if existing else []

    tied = [v for v in variants if float(v["variant_vs_emerging_jaccard"]) == baseline_minimum]

    tied_details: list[dict[str, Any]] = []
    overlap_by_variant: dict[str, list[int]] = {}
    for v in tied:
        vid = str(v["variant_id"])
        top_k = [int(x) for x in v["eligible_top_k_ids"]]
        ov = sorted(set(top_k) & emerging_set)
        overlap_by_variant[vid] = ov
        tied_details.append(
            {
                "variant_id": vid,
                "eligible_count_total": int(v["eligible_count_total"]),
                "eligible_top_k_ids": top_k,
                "eligible_top_k_hash_sha256": _top_k_hash(top_k),
                "variant_vs_emerging_jaccard": float(v["variant_vs_emerging_jaccard"]),
                "eligible_top_k_intersect_emerging_top_k": ov,
            }
        )

    baseline_hash = _top_k_hash(baseline_top_k) if baseline_top_k else ""
    tied_same_as_existing = [
        v for v in tied if list(v.get("eligible_top_k_ids") or []) == baseline_top_k
    ]
    tied_variants_with_same_top_20_count = len(tied_same_as_existing)

    if overlap_by_variant:
        persistent_ids = set(next(iter(overlap_by_variant.values())))
        for s in overlap_by_variant.values():
            persistent_ids &= set(s)
        persistent_sorted = sorted(persistent_ids)
    else:
        persistent_sorted = []

    union_ids: set[int] = set()
    per_id_variant_count: dict[int, int] = {}
    for s in overlap_by_variant.values():
        for wid in s:
            union_ids.add(wid)
            per_id_variant_count[wid] = per_id_variant_count.get(wid, 0) + 1

    distinct_hashes_among_tied = {d["eligible_top_k_hash_sha256"] for d in tied_details}

    below_040 = [
        {
            "variant_id": str(v["variant_id"]),
            "eligible_count_total": int(v["eligible_count_total"]),
            "variant_vs_emerging_jaccard": float(v["variant_vs_emerging_jaccard"]),
        }
        for v in variants
        if float(v["variant_vs_emerging_jaccard"]) < 0.40
    ]
    below_040.sort(key=lambda r: (r["variant_vs_emerging_jaccard"], r["variant_id"]))
    cohort_risk = [r for r in below_040 if r["eligible_count_total"] < 50]

    baseline_jaccard_reference = (
        float(existing["variant_vs_emerging_jaccard"]) if existing is not None else baseline_minimum
    )
    cohort_below_reference = [
        v
        for v in variants
        if float(v["variant_vs_emerging_jaccard"]) < baseline_jaccard_reference
    ]
    cohort_collapse = bool(cohort_below_reference) and all(
        int(v["eligible_count_total"]) < 50 for v in cohort_below_reference
    )
    has_ties_at_minimum = len(tied) >= 2
    threshold_too_weak = (not has_ties_at_minimum) and (baseline_minimum >= 0.40)

    pct_map = _variant_bridge_percentile_jaccards(variants)
    monotonic = _percentile_monotonic_distinctness_loss(pct_map)

    rows = list(signal_diagnostics.get("bridge_top_k_rows") or [])
    bridge_scores: list[float] = []
    final_scores: list[float] = []
    for r in rows:
        if r.get("bridge_score") is not None and r.get("final_score") is not None:
            bridge_scores.append(float(r["bridge_score"]))
            final_scores.append(float(r["final_score"]))
    r_bridge_final = _pearson(bridge_scores, final_scores)
    corr_strong = r_bridge_final is not None and abs(r_bridge_final) >= 0.35
    correlation_dominant = (monotonic is True) and corr_strong

    persistent_n = len(persistent_sorted)
    if (
        tied_variants_with_same_top_20_count >= 2
        and persistent_n >= 5
    ):
        primary = "tied_variants_collapse_to_same_top_20"
    elif persistent_n >= 4 and len(distinct_hashes_among_tied) >= 2 and len(tied) >= 2:
        primary = "structural_bridge_emerging_intersection"
    elif cohort_collapse:
        primary = "cohort_collapse"
    elif threshold_too_weak:
        primary = "threshold_too_weak"
    elif correlation_dominant:
        primary = "correlation_dominant"
    else:
        primary = "structural_bridge_emerging_intersection" if persistent_n >= 4 else "unknown"

    recommendation = _recommendation_for_cause(primary)

    return {
        "generated_note": "bridge_eligibility_failure_analysis v1 (stdlib JSON transform)",
        "verbatim_caveats": list(VERBATIM_CAVEATS),
        "inputs": {
            "sensitivity_provenance": sensitivity.get("provenance"),
            "signal_provenance": signal_diagnostics.get("provenance"),
            "weight_response_review_kind": weight_response.get("review_kind"),
            "labeled_outcome_review_kind": labeled_outcome.get("review_kind"),
        },
        "baseline_minimum_variant_vs_emerging_jaccard": baseline_minimum,
        "baseline_jaccard_reference_existing_bridge_eligible": baseline_jaccard_reference,
        "cohort_collapse_variants_below_reference_baseline_jaccard": [
            {
                "variant_id": str(v["variant_id"]),
                "eligible_count_total": int(v["eligible_count_total"]),
                "variant_vs_emerging_jaccard": float(v["variant_vs_emerging_jaccard"]),
            }
            for v in cohort_below_reference
        ],
        "tied_variants_at_baseline_minimum": tied_details,
        "tied_variants_with_same_top_20_count": tied_variants_with_same_top_20_count,
        "existing_bridge_eligible_top_k_hash_sha256": baseline_hash,
        "persistent_shared_with_emerging_ids": persistent_sorted,
        "union_shared_with_emerging_ids_across_tied_variants": sorted(union_ids),
        "per_work_id_tied_variant_overlap_count": {str(k): v for k, v in sorted(per_id_variant_count.items())},
        "distinct_eligible_top_k_hashes_among_tied_variants": sorted(distinct_hashes_among_tied),
        "cohort_distinctness_cost_variants_jaccard_lt_0_40": below_040,
        "cohort_collapse_risk_eligible_count_lt_50": cohort_risk,
        "supporting": {
            "weight_response_distinctness": (weight_response.get("distinctness") or {}),
            "labeled_per_run_distinctness": {
                k: (v.get("distinctness") or {})
                for k, v in (labeled_outcome.get("per_run") or {}).items()
                if isinstance(v, dict)
            },
            "bridge_score_percentile_variant_jaccards": pct_map,
            "percentile_loosen_monotonic_distinctness_loss": monotonic,
            "bridge_top_k_pearson_bridge_score_vs_final_score": r_bridge_final,
        },
        "primary_suspected_cause": primary,
        "recommended_next_lever": recommendation,
        "classifier_branch_notes": {
            "tied_variants_collapse_to_same_top_20": "same_top_20_count>=2 among tied-at-minimum AND len(persistent_shared)>=5",
            "structural_bridge_emerging_intersection": "len(persistent_shared)>=4 AND >=2 distinct top-k hashes among tied-at-minimum",
            "cohort_collapse": "every variant with jaccard strictly below existing_bridge_eligible.variant_vs_emerging_jaccard has eligible_count_total<50 (requires at least one such variant)",
            "threshold_too_weak": "no ties at baseline_minimum AND baseline_minimum>=0.40",
            "correlation_dominant": "percentile monotonic loss AND strong Pearson on diagnostics bridge_top_k_rows (fallback branch)",
        },
    }


def markdown_from_failure_analysis(payload: dict[str, Any]) -> str:
    lines: list[str] = [
        "# Bridge eligibility failure analysis",
        "",
        f"- **baseline_minimum** `variant_vs_emerging_jaccard`: `{payload.get('baseline_minimum_variant_vs_emerging_jaccard')}`",
        f"- **tied_variants_with_same_top_20_count:** `{payload.get('tied_variants_with_same_top_20_count')}`",
        f"- **primary_suspected_cause:** `{payload.get('primary_suspected_cause')}`",
        "",
        "## Tied variants (at baseline minimum)",
        "",
        "| variant_id | eligible_count_total | top_k_hash (sha256, first 12 hex) | eligible_top_k ∩ emerging |",
        "| --- | ---: | --- | --- |",
    ]
    for t in payload.get("tied_variants_at_baseline_minimum") or []:
        h = str(t.get("eligible_top_k_hash_sha256", ""))
        h12 = h[:12] + "..." if len(h) > 12 else h
        inter = t.get("eligible_top_k_intersect_emerging_top_k") or []
        lines.append(
            f"| `{t.get('variant_id')}` | {t.get('eligible_count_total')} | `{h12}` | `{inter}` |"
        )
    lines.extend(
        [
            "",
            "## Persistent / union overlap (tied variants only)",
            "",
            f"- **persistent_shared_with_emerging_ids:** `{payload.get('persistent_shared_with_emerging_ids')}`",
            f"- **union_shared_with_emerging_ids_across_tied_variants:** `{payload.get('union_shared_with_emerging_ids_across_tied_variants')}`",
            "",
            "### Per work_id: count of tied variants where id appears in (eligible_top_k ∩ emerging)",
            "",
            "```json",
            json.dumps(payload.get("per_work_id_tied_variant_overlap_count") or {}, indent=2),
            "```",
            "",
            "## Cohort cost (variant_vs_emerging_jaccard < 0.40)",
            "",
            "Sorted by Jaccard ascending. Rows with `eligible_count_total` < 50 are cohort-collapse risk.",
            "",
            "| variant_id | eligible_count_total | variant_vs_emerging_jaccard |",
            "| --- | ---: | ---: |",
        ]
    )
    for r in payload.get("cohort_distinctness_cost_variants_jaccard_lt_0_40") or []:
        flag = " **<50**" if r.get("eligible_count_total", 999) < 50 else ""
        lines.append(
            f"| `{r.get('variant_id')}` | {r.get('eligible_count_total')}{flag} | {r.get('variant_vs_emerging_jaccard')} |"
        )
    risk = payload.get("cohort_collapse_risk_eligible_count_lt_50") or []
    if not risk:
        lines.extend(["", "_No variants in the <0.40 Jaccard band have eligible_count_total below 50._"])
    sup = payload.get("supporting") or {}
    if sup:
        lines.extend(
            [
                "",
                "## Supporting signals (from bundled JSON)",
                "",
                f"- **bridge_score percentile Jaccards (75 / 50 / 25 pct):** `{sup.get('bridge_score_percentile_variant_jaccards')}`",
                f"- **Monotonic distinctness loss when loosening bridge_score cutoff:** `{sup.get('percentile_loosen_monotonic_distinctness_loss')}`",
                f"- **Pearson(bridge_score, final_score) on diagnostics `bridge_top_k_rows`:** `{sup.get('bridge_top_k_pearson_bridge_score_vs_final_score')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Why Jaccard cannot drop below the baseline minimum on this sweep",
            "",
            "Several eligibility rewrites tie at the same minimum Jaccard because they either reproduce the same "
            "eligible-only top-20 as `existing_bridge_eligible`, or they reshuffle lower-ranked eligible mass without "
            "removing the persistent intersection with `emerging_top_k`. Any rule that keeps the same seven emerging "
            "hits in the eligible top-20 reproduces the same Jaccard (7 / (20+20-7)). Tightening bridge_score alone "
            "eventually retrains mass into that same floor unless emerging-adjacent works are explicitly down-ranked or excluded.",
            "",
            "## Recommended next lever",
            "",
            payload.get("recommended_next_lever", ""),
            "",
            "## Verbatim caveats",
            "",
        ]
    )
    for c in payload.get("verbatim_caveats") or []:
        lines.append(f"> {c}")
    lines.append("")
    return "\n".join(lines)


def run_bridge_eligibility_failure_analysis(
    *,
    sensitivity_path: Path,
    signal_diagnostics_path: Path,
    weight_response_path: Path,
    labeled_outcome_path: Path,
    output_json_path: Path,
    output_markdown_path: Path,
) -> dict[str, Any]:
    sens = _load_json(sensitivity_path)
    sig = _load_json(signal_diagnostics_path)
    wr = _load_json(weight_response_path)
    lo = _load_json(labeled_outcome_path)
    payload = analyze_bridge_eligibility_failure(sens, sig, wr, lo)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    output_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    output_markdown_path.write_text(markdown_from_failure_analysis(payload), encoding="utf-8", newline="\n")
    return payload

__all__ = [
    "VERBATIM_CAVEATS",
    "analyze_bridge_eligibility_failure",
    "markdown_from_failure_analysis",
    "run_bridge_eligibility_failure_analysis",
]
