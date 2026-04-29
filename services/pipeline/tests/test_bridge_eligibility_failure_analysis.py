"""Tests for bridge eligibility failure analysis (JSON-only, no DB)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.bridge_eligibility_failure_analysis import (
    VERBATIM_CAVEATS,
    analyze_bridge_eligibility_failure,
    markdown_from_failure_analysis,
    run_bridge_eligibility_failure_analysis,
)


def _minimal_wr_lo() -> tuple[dict, dict]:
    return (
        {"review_kind": "bridge_weight_response_rollup", "distinctness": {}},
        {"review_kind": "bridge_weight_labeled_outcome", "per_run": {}},
    )


def test_tied_variant_detection_and_hashes() -> None:
    emerging = list(range(100, 120))
    top_a = list(range(20))
    top_b = list(range(10, 30))
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 80,
                "eligible_top_k_ids": top_a,
                "variant_vs_emerging_jaccard": 0.5,
            },
            {
                "variant_id": "other_rule",
                "eligible_count_total": 70,
                "eligible_top_k_ids": top_b,
                "variant_vs_emerging_jaccard": 0.6,
            },
            {
                "variant_id": "tie_b",
                "eligible_count_total": 50,
                "eligible_top_k_ids": top_b,
                "variant_vs_emerging_jaccard": 0.5,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    tied = out["tied_variants_at_baseline_minimum"]
    assert len(tied) == 2
    assert {t["variant_id"] for t in tied} == {"existing_bridge_eligible", "tie_b"}
    assert all("eligible_top_k_hash_sha256" in t for t in tied)
    assert tied[0]["eligible_top_k_hash_sha256"] != tied[1]["eligible_top_k_hash_sha256"]


def test_same_top_20_collapse_count() -> None:
    emerging = [200, 201] + list(range(30, 48))
    top_k = list(range(20))
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 90,
                "eligible_top_k_ids": list(top_k),
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "dup_rule_a",
                "eligible_count_total": 88,
                "eligible_top_k_ids": list(top_k),
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "dup_rule_b",
                "eligible_count_total": 87,
                "eligible_top_k_ids": list(top_k),
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "different_top",
                "eligible_count_total": 40,
                "eligible_top_k_ids": list(range(10, 30)),
                "variant_vs_emerging_jaccard": 0.35,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    assert out["tied_variants_with_same_top_20_count"] == 3


def test_persistent_shared_with_emerging_intersection() -> None:
    emerging = [1, 2, 3, 4, 5, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114]
    top_same = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    top_alt = [1, 2, 3, 4, 5, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": top_same,
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "dup_top",
                "eligible_count_total": 92,
                "eligible_top_k_ids": list(top_same),
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "alt_top",
                "eligible_count_total": 60,
                "eligible_top_k_ids": top_alt,
                "variant_vs_emerging_jaccard": 0.2,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    assert out["persistent_shared_with_emerging_ids"] == [1, 2, 3, 4, 5]


def test_classifier_tied_variants_collapse_to_same_top_20() -> None:
    emerging = [1, 2, 3, 4, 5, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114]
    top_k = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": list(top_k),
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "rule_same_top",
                "eligible_count_total": 92,
                "eligible_top_k_ids": list(top_k),
                "variant_vs_emerging_jaccard": 0.2,
            },
            {
                "variant_id": "alt_top",
                "eligible_count_total": 60,
                "eligible_top_k_ids": [1, 2, 3, 4, 5, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34],
                "variant_vs_emerging_jaccard": 0.2,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    assert out["primary_suspected_cause"] == "tied_variants_collapse_to_same_top_20"
    assert "do not pick a threshold variant" in out["recommended_next_lever"]


def test_classifier_structural_bridge_emerging_intersection() -> None:
    emerging = [1, 2, 3, 4, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65]
    top_a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    top_b = [1, 2, 3, 4, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": top_a,
                "variant_vs_emerging_jaccard": 0.25,
            },
            {
                "variant_id": "variant_b",
                "eligible_count_total": 93,
                "eligible_top_k_ids": top_b,
                "variant_vs_emerging_jaccard": 0.25,
            },
            {
                "variant_id": "high_j",
                "eligible_count_total": 200,
                "eligible_top_k_ids": list(range(200, 220)),
                "variant_vs_emerging_jaccard": 0.9,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    assert out["tied_variants_with_same_top_20_count"] == 1
    assert out["primary_suspected_cause"] == "structural_bridge_emerging_intersection"


def test_classifier_threshold_too_weak() -> None:
    emerging = list(range(1000, 1020))
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.45,
            },
            {
                "variant_id": "other",
                "eligible_count_total": 80,
                "eligible_top_k_ids": list(range(10, 30)),
                "variant_vs_emerging_jaccard": 0.5,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    assert out["primary_suspected_cause"] == "threshold_too_weak"


def test_classifier_cohort_collapse() -> None:
    emerging = list(range(1000, 1020))
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.3,
            },
            {
                "variant_id": "tight_a",
                "eligible_count_total": 40,
                "eligible_top_k_ids": list(range(5, 25)),
                "variant_vs_emerging_jaccard": 0.1,
            },
            {
                "variant_id": "tight_b",
                "eligible_count_total": 30,
                "eligible_top_k_ids": list(range(6, 26)),
                "variant_vs_emerging_jaccard": 0.15,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": []}, wr, lo)
    assert out["baseline_minimum_variant_vs_emerging_jaccard"] == 0.1
    assert out["primary_suspected_cause"] == "cohort_collapse"


def test_classifier_correlation_dominant_branch() -> None:
    emerging = list(range(1000, 1020))
    rows = []
    for i in range(20):
        rows.append({"bridge_score": 0.1 * i, "final_score": 0.05 * i})
    sens = {
        "baseline": {"emerging_top_k_ids": emerging},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.36,
            },
            {
                "variant_id": "bridge_score_top_75pct",
                "eligible_count_total": 200,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.5,
            },
            {
                "variant_id": "bridge_score_top_50pct",
                "eligible_count_total": 150,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.4,
            },
            {
                "variant_id": "bridge_score_top_25pct",
                "eligible_count_total": 100,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.35,
            },
        ],
    }
    wr, lo = _minimal_wr_lo()
    out = analyze_bridge_eligibility_failure(sens, {"bridge_top_k_rows": rows}, wr, lo)
    assert out["primary_suspected_cause"] == "correlation_dominant"


def test_module_has_no_psycopg_and_verbatim_caveats() -> None:
    path = Path(__file__).resolve().parents[1] / "pipeline" / "bridge_eligibility_failure_analysis.py"
    src = path.read_text(encoding="utf-8")
    assert "psycopg" not in src
    assert "connect(" not in src
    assert VERBATIM_CAVEATS[0] in src


def test_markdown_contains_verbatim_caveats() -> None:
    payload = {
        "baseline_minimum_variant_vs_emerging_jaccard": 0.2,
        "tied_variants_with_same_top_20_count": 0,
        "primary_suspected_cause": "x",
        "tied_variants_at_baseline_minimum": [],
        "persistent_shared_with_emerging_ids": [],
        "union_shared_with_emerging_ids_across_tied_variants": [],
        "per_work_id_tied_variant_overlap_count": {},
        "cohort_distinctness_cost_variants_jaccard_lt_0_40": [],
        "cohort_collapse_risk_eligible_count_lt_50": [],
        "recommended_next_lever": "test",
        "verbatim_caveats": list(VERBATIM_CAVEATS),
    }
    md = markdown_from_failure_analysis(payload)
    for c in VERBATIM_CAVEATS:
        assert c in md


@pytest.mark.skipif(
    not (Path(__file__).resolve().parents[3] / "docs" / "audit" / "manual-review" / "bridge_eligibility_sensitivity_rank-ee2ba6c816_top20.json").is_file(),
    reason="frozen audit JSON not present",
)
def test_rank_ee2ba6c816_fixture_primary_is_tied_collapse() -> None:
    root = Path(__file__).resolve().parents[3]
    sens = json.loads(
        (root / "docs/audit/manual-review/bridge_eligibility_sensitivity_rank-ee2ba6c816_top20.json").read_text(
            encoding="utf-8"
        )
    )
    sig = json.loads(
        (root / "docs/audit/manual-review/bridge_signal_diagnostics_rank-ee2ba6c816_top20.json").read_text(
            encoding="utf-8"
        )
    )
    wr = json.loads(
        (
            root / "docs/audit/manual-review/bridge_weight_response_rank-ee2ba6c816_rank-bc1123e00c_rank-9a02c81d40.json"
        ).read_text(encoding="utf-8")
    )
    lo = json.loads(
        (
            root
            / "docs/audit/manual-review/bridge_weight_labeled_outcome_rank-ee2ba6c816_rank-bc1123e00c_rank-9a02c81d40.json"
        ).read_text(encoding="utf-8")
    )
    out = analyze_bridge_eligibility_failure(sens, sig, wr, lo)
    assert out["primary_suspected_cause"] == "tied_variants_collapse_to_same_top_20"
    assert out["tied_variants_with_same_top_20_count"] == 3
    assert set(out["persistent_shared_with_emerging_ids"]) == {10, 14, 125, 131, 138}


def test_run_writes_json_and_md(tmp_path: Path) -> None:
    sens = {
        "provenance": {"ranking_run_id": "rank-x"},
        "baseline": {"emerging_top_k_ids": list(range(1000, 1020))},
        "variants": [
            {
                "variant_id": "existing_bridge_eligible",
                "eligible_count_total": 93,
                "eligible_top_k_ids": list(range(20)),
                "variant_vs_emerging_jaccard": 0.45,
            },
            {"variant_id": "z", "eligible_count_total": 80, "eligible_top_k_ids": list(range(5, 25)), "variant_vs_emerging_jaccard": 0.5},
        ],
    }
    p_s = tmp_path / "s.json"
    p_s.write_text(json.dumps(sens), encoding="utf-8")
    p_sig = tmp_path / "sig.json"
    p_sig.write_text(json.dumps({"bridge_top_k_rows": []}), encoding="utf-8")
    p_wr = tmp_path / "wr.json"
    p_wr.write_text(json.dumps({"review_kind": "x"}), encoding="utf-8")
    p_lo = tmp_path / "lo.json"
    p_lo.write_text(json.dumps({"review_kind": "y", "per_run": {}}), encoding="utf-8")
    out_j = tmp_path / "out.json"
    out_m = tmp_path / "out.md"
    run_bridge_eligibility_failure_analysis(
        sensitivity_path=p_s,
        signal_diagnostics_path=p_sig,
        weight_response_path=p_wr,
        labeled_outcome_path=p_lo,
        output_json_path=out_j,
        output_markdown_path=out_m,
    )
    assert out_j.is_file() and out_m.is_file()
    assert "verbatim_caveats" in json.loads(out_j.read_text(encoding="utf-8"))
