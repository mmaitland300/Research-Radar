"""
Derived ranking explanations for API responses. Matches pipeline final_score_partial weighting
when family_weights are present on the ranking run; falls back to defaults.
"""

from __future__ import annotations

from math import fsum
from typing import Any, Literal

SignalRole = Literal["used", "measured", "experimental", "penalty", "not_computed"]

POSITIVE_KEYS = ("semantic", "citation_velocity", "topic_growth", "bridge")

DEFAULT_FAMILY_WEIGHTS: dict[str, dict[str, float]] = {
    "emerging": {
        "semantic": 0.0,
        "citation_velocity": 0.6,
        "topic_growth": 0.4,
        "bridge": 0.0,
        "diversity_penalty": 0.05,
    },
    "bridge": {
        "semantic": 0.0,
        "citation_velocity": 0.35,
        "topic_growth": 0.65,
        "bridge": 0.0,
        "diversity_penalty": 0.2,
    },
    "undercited": {
        "semantic": 0.0,
        "citation_velocity": 0.3,
        "topic_growth": 0.7,
        "bridge": 0.0,
        "diversity_penalty": 0.25,
    },
}


def _base_signal_label(key: str) -> str:
    return {
        "semantic": "Semantic match",
        "citation_velocity": "Recent attention",
        "topic_growth": "Topic momentum",
        "bridge": "Cross-cluster signal",
        "diversity_penalty": "Diversity penalty",
    }.get(key, key)


def signal_display_label(family: str, key: str) -> str:
    if key == "semantic" and family == "emerging":
        return "Embedding slice fit (corpus centroid)"
    if key == "diversity_penalty":
        if family == "emerging":
            return "Similarity penalty"
        if family == "bridge":
            return "Topic breadth penalty"
        return "Pool popularity penalty"
    return _base_signal_label(key)


def family_weights_from_config(config_json: dict[str, Any] | None, family: str) -> dict[str, float]:
    defaults = DEFAULT_FAMILY_WEIGHTS.get(family, DEFAULT_FAMILY_WEIGHTS["emerging"])
    if not config_json:
        return dict(defaults)
    raw = config_json.get("family_weights")
    if not isinstance(raw, dict):
        return dict(defaults)
    fam = raw.get(family)
    if not isinstance(fam, dict):
        return dict(defaults)
    out = dict(defaults)
    for k in out:
        if k in fam and isinstance(fam[k], (int, float)):
            out[k] = float(fam[k])
    return out


def _ordinal_strength(value: float) -> str:
    if value >= 0.66:
        return "high"
    if value >= 0.33:
        return "medium"
    return "low"


def _positive_decomposition(
    *,
    semantic: float | None,
    citation_velocity: float | None,
    topic_growth: float | None,
    bridge: float | None,
    w_sem: float,
    w_cv: float,
    w_tg: float,
    w_br: float,
) -> dict[str, float]:
    terms: list[tuple[float, float, str]] = []
    if semantic is not None:
        terms.append((w_sem, semantic, "semantic"))
    if citation_velocity is not None:
        terms.append((w_cv, citation_velocity, "citation_velocity"))
    if topic_growth is not None:
        terms.append((w_tg, topic_growth, "topic_growth"))
    if bridge is not None:
        terms.append((w_br, bridge, "bridge"))

    contribs = {k: 0.0 for k in POSITIVE_KEYS}
    if not terms:
        return contribs
    w_sum = fsum(w for w, _, _ in terms)
    if w_sum <= 0:
        return contribs
    for w, v, name in terms:
        contribs[name] = (w / w_sum) * v
    return contribs


def build_signal_explanations(
    *,
    family: str,
    semantic: float | None,
    citation_velocity: float | None,
    topic_growth: float | None,
    bridge: float | None,
    diversity_penalty: float | None,
    weights: dict[str, float],
) -> list[dict[str, Any]]:
    w_sem = weights["semantic"]
    w_cv = weights["citation_velocity"]
    w_tg = weights["topic_growth"]
    w_br = weights["bridge"]
    w_dp = weights["diversity_penalty"]

    raw_by_key = {
        "semantic": semantic,
        "citation_velocity": citation_velocity,
        "topic_growth": topic_growth,
        "bridge": bridge,
    }

    pos_contrib = _positive_decomposition(
        semantic=semantic,
        citation_velocity=citation_velocity,
        topic_growth=topic_growth,
        bridge=bridge,
        w_sem=w_sem,
        w_cv=w_cv,
        w_tg=w_tg,
        w_br=w_br,
    )

    out: list[dict[str, Any]] = []
    for key in POSITIVE_KEYS:
        label = signal_display_label(family, key)
        raw = raw_by_key[key]
        w = weights[key]
        if raw is None:
            out.append(
                {
                    "key": key,
                    "label": label,
                    "role": "not_computed",
                    "value": None,
                    "contribution": None,
                    "summary": f"{label}: not computed for this run",
                }
            )
            continue
        strength = _ordinal_strength(float(raw))
        if w > 0:
            out.append(
                {
                    "key": key,
                    "label": label,
                    "role": "used",
                    "value": float(raw),
                    "contribution": round(pos_contrib[key], 6),
                    "summary": f"{label}: {strength}; used in final ranking",
                }
            )
        else:
            out.append(
                {
                    "key": key,
                    "label": label,
                    "role": "measured",
                    "value": float(raw),
                    "contribution": 0.0,
                    "summary": f"{label}: {strength}; measured only, not used in ordering",
                }
            )

    dpl = signal_display_label(family, "diversity_penalty")
    dp_val = diversity_penalty
    if w_dp > 0:
        dp_f = float(dp_val if dp_val is not None else 0.0)
        pen = -(w_dp * dp_f)
        out.append(
            {
                "key": "diversity_penalty",
                "label": dpl,
                "role": "penalty",
                "value": float(dp_f),
                "contribution": round(pen, 6),
                "summary": f"{dpl}: reduces score when non-zero",
            }
        )
    else:
        out.append(
            {
                "key": "diversity_penalty",
                "label": dpl,
                "role": "not_computed",
                "value": float(dp_val) if dp_val is not None else None,
                "contribution": None,
                "summary": f"{dpl}: not applied for this family configuration",
            }
        )

    return out


def build_list_ranking_explanation(*, family: str, weights: dict[str, float]) -> dict[str, Any]:
    """Structured copy for 'How this list is ranked' (all families; UI may show selectively)."""
    used_labels: list[str] = []
    measured_labels: list[str] = []
    penalty_label = signal_display_label(family, "diversity_penalty")

    for key in POSITIVE_KEYS:
        label = signal_display_label(family, key)
        w = weights[key]
        if w > 0:
            used_labels.append(label)
        elif key in ("semantic", "bridge"):
            measured_labels.append(label)

    if weights["diversity_penalty"] > 0:
        used_labels.append(penalty_label)

    if family == "emerging":
        headline = "How this Emerging list is ranked"
        bullets = [
            "Ordering uses recent attention and topic momentum from this corpus snapshot.",
            "Similarity penalty can reduce scores when topic mix is narrow (small weight for Emerging).",
            "Semantic match and cross-cluster signal appear when present but are not used in ordering for this family.",
        ]
    elif family == "undercited":
        headline = "How this Under-cited list is ranked"
        bullets = [
            "Only papers in the frozen low-citation candidate pool are eligible.",
            "Ordering uses recent attention, topic momentum, and a pool popularity penalty.",
            "Semantic and cross-cluster signals are shown when present but are not used by default.",
        ]
    else:
        headline = "How this Bridge list is ranked"
        bullets = [
            "Ordering uses recent attention, topic momentum, and topic breadth penalty.",
            "Cross-cluster signal may be weighted for this family depending on the ranking run.",
            "Semantic match is shown when present but not used in default ordering.",
        ]

    return {
        "family": family,
        "headline": headline,
        "bullets": bullets,
        "used_in_ordering": used_labels,
        "measured_only": measured_labels,
        "experimental": [],
    }
