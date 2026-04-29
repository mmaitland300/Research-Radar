"""Tests for objective experiment labeled-outcome rollup."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest

from pipeline.bridge_objective_labeled_outcome import (
    BridgeObjectiveLabeledOutcomeError,
    build_bridge_objective_labeled_outcome_payload,
    markdown_from_bridge_objective_labeled_outcome,
)


def _write_baseline(path: Path) -> None:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "ranking_run_id",
            "rank",
            "paper_id",
            "review_pool_variant",
            "family",
            "bridge_eligible",
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
        ]
    )
    for i in range(1, 21):
        w.writerow(
            [
                "rank-ee2ba6c816",
                str(i),
                f"https://openalex.org/W{i}",
                "bridge_eligible_only",
                "bridge",
                "true",
                "good" if i <= 18 else "acceptable",
                "useful",
                "yes" if i <= 16 else "partial",
            ]
        )
    path.write_text(buf.getvalue(), encoding="utf-8")


def _write_prior_delta(path: Path) -> None:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "baseline_ranking_run_id",
            "experiment_ranking_run_id",
            "paper_id",
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
        ]
    )
    for wid in (4412072221, 4411141538, 4411141958, 7128600794):
        w.writerow(["rank-ee2ba6c816", "rank-bc1123e00c", f"https://openalex.org/W{wid}", "good", "useful", "partial"])
    path.write_text(buf.getvalue(), encoding="utf-8")


def _write_one_row(path: Path, *, relevance: str = "good", novelty: str = "useful", bridge_like: str = "partial") -> None:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "baseline_ranking_run_id",
            "experiment_ranking_run_id",
            "experiment_rank",
            "work_id",
            "paper_id",
            "title",
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
            "reviewer_notes",
        ]
    )
    w.writerow(
        [
            "rank-ee2ba6c816",
            "rank-60910a47b4",
            "20",
            "116",
            "https://openalex.org/W4412072230",
            "t",
            relevance,
            novelty,
            bridge_like,
            "",
        ]
    )
    path.write_text(buf.getvalue(), encoding="utf-8")


def _write_compare(path: Path) -> None:
    data = {
        "review_kind": "bridge_objective_experiment_compare",
        "provenance": {
            "baseline": {
                "ranking_run_id": "rank-ee2ba6c816",
                "bridge_eligibility_mode": "top50_cross_cluster_gte_0_40",
                "bridge_weight_for_family_bridge": 0.0,
                "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
                "embedding_version": "v2-title-abstract-1536-cleantext-r1",
                "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
            },
            "experiment": {
                "ranking_run_id": "rank-60910a47b4",
                "bridge_eligibility_mode": "top50_cross040_exclude_persistent_shared_v1",
                "bridge_weight_for_family_bridge": 0.0,
                "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
                "embedding_version": "v2-title-abstract-1536-cleantext-r1",
                "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
            },
        },
        "bridge_top_k_comparison": {
            "baseline_eligible_bridge_top_k_ids": list(range(1, 21)),
            "experiment_eligible_bridge_top_k_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 117, 124, 127, 1281, 116],
        },
        "quality_risk": {
            "unlabeled_new_experiment_eligible_top_k_rows": [
                {"rank": 16, "work_id": 117, "paper_id": "https://openalex.org/W4412072221", "title": "a"},
                {"rank": 17, "work_id": 124, "paper_id": "https://openalex.org/W4411141538", "title": "b"},
                {"rank": 18, "work_id": 127, "paper_id": "https://openalex.org/W4411141958", "title": "c"},
                {"rank": 19, "work_id": 1281, "paper_id": "https://openalex.org/W7128600794", "title": "d"},
                {"rank": 20, "work_id": 116, "paper_id": "https://openalex.org/W4412072230", "title": "e"},
            ]
        },
        "distinctness": {
            "baseline_eligible_bridge_vs_emerging_jaccard": 0.212121,
            "experiment_eligible_bridge_vs_emerging_jaccard": 0.081081,
        },
    }
    path.write_text(json.dumps(data), encoding="utf-8")


def test_rejects_blank_label_in_one_row_csv(tmp_path: Path) -> None:
    b = tmp_path / "b.csv"
    d = tmp_path / "d.csv"
    o = tmp_path / "o.csv"
    c = tmp_path / "c.json"
    _write_baseline(b)
    _write_prior_delta(d)
    _write_one_row(o, relevance="")
    _write_compare(c)
    with pytest.raises(BridgeObjectiveLabeledOutcomeError, match="is blank"):
        build_bridge_objective_labeled_outcome_payload(
            baseline_worksheet_path=b,
            prior_delta_worksheet_path=d,
            objective_delta_worksheet_path=o,
            objective_comparison_path=c,
        )


def test_merges_label_sources_without_duplicate_papers(tmp_path: Path) -> None:
    b = tmp_path / "b.csv"
    d = tmp_path / "d.csv"
    o = tmp_path / "o.csv"
    c = tmp_path / "c.json"
    _write_baseline(b)
    _write_prior_delta(d)
    _write_one_row(o)
    _write_compare(c)
    payload = build_bridge_objective_labeled_outcome_payload(
        baseline_worksheet_path=b,
        prior_delta_worksheet_path=d,
        objective_delta_worksheet_path=o,
        objective_comparison_path=c,
    )
    assert payload["coverage"]["label_map_paper_id_count"] == 25
    assert payload["coverage"]["objective_delta_labeled_count"] == 1


def test_computes_gates_and_ready_for_default_false(tmp_path: Path) -> None:
    b = tmp_path / "b.csv"
    d = tmp_path / "d.csv"
    o = tmp_path / "o.csv"
    c = tmp_path / "c.json"
    _write_baseline(b)
    _write_prior_delta(d)
    _write_one_row(o, relevance="good", novelty="useful", bridge_like="partial")
    _write_compare(c)
    payload = build_bridge_objective_labeled_outcome_payload(
        baseline_worksheet_path=b,
        prior_delta_worksheet_path=d,
        objective_delta_worksheet_path=o,
        objective_comparison_path=c,
    )
    assert payload["decision"]["quality_preserved_under_new_mode"] is True
    assert payload["decision"]["bridge_like_preserved_under_new_mode"] is True
    assert payload["decision"]["distinctness_improves"] is True
    assert payload["decision"]["recommend_persistent_overlap_exclusion_as_experimental_arm"] is True
    assert payload["decision"]["ready_for_default"] is False


def test_emits_required_caveats(tmp_path: Path) -> None:
    b = tmp_path / "b.csv"
    d = tmp_path / "d.csv"
    o = tmp_path / "o.csv"
    c = tmp_path / "c.json"
    _write_baseline(b)
    _write_prior_delta(d)
    _write_one_row(o)
    _write_compare(c)
    payload = build_bridge_objective_labeled_outcome_payload(
        baseline_worksheet_path=b,
        prior_delta_worksheet_path=d,
        objective_delta_worksheet_path=o,
        objective_comparison_path=c,
    )
    caveats = payload["caveats"]
    assert "This is not validation of bridge ranking quality." in caveats
    assert "Single-reviewer, top-20, offline audit material only." in caveats
    assert (
        "Persistent-overlap exclusion is corpus-snapshot-specific (source-snapshot-v2-candidate-plan-20260428); the rule must not become default without rederivation on the active snapshot."
        in caveats
    )
    md = markdown_from_bridge_objective_labeled_outcome(payload)
    assert "ready_for_default" in md
