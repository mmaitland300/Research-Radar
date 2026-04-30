"""Tests for ml-blind-family-context-eval (read-only blind-source diagnostic)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_blind_family_context_eval import (
    CAVEATS,
    MLBlindFamilyContextEvalError,
    build_blind_family_context_eval_payload,
    compute_family_context_metrics,
    filter_blind_rows,
    markdown_from_blind_family_context_eval,
    write_blind_family_context_eval,
)


def _blind_row(
    *,
    row_id: str,
    paper_id: str = "https://openalex.org/W1",
    relevance: str | None = "good",
    novelty: str | None = "useful",
    bridge_like: str | None = "yes",
    good: bool | None = True,
    surp: bool | None = True,
    bridge_yes: bool | None = True,
    scores: dict[str, float] | None = None,
    ranks: dict[str, int] | None = None,
    family: object | None = None,
) -> dict:
    return {
        "split": "audit_only",
        "review_pool_variant": "ml_blind_snapshot_audit",
        "ranking_run_id": "rank-ee2ba6c816",
        "row_id": row_id,
        "paper_id": paper_id,
        "family": family,
        "relevance_label": relevance,
        "novelty_label": novelty,
        "bridge_like_label": bridge_like,
        "good_or_acceptable": good,
        "surprising_or_useful": surp,
        "bridge_like_yes_or_partial": bridge_yes,
        "ranking_context_family_scores_json": json.dumps(scores) if scores is not None else None,
        "ranking_context_family_ranks_json": json.dumps(ranks) if ranks is not None else None,
    }


def _payload(*rows: dict) -> dict:
    return {"dataset_version": "ml-label-dataset-v4", "rows": list(rows)}


def test_filter_blind_rows_only_includes_blind_pool_for_run() -> None:
    p = _payload(
        _blind_row(row_id="a"),
        {"split": "audit_only", "review_pool_variant": "rank_top_k", "ranking_run_id": "rank-ee2ba6c816", "row_id": "b"},
        {"split": "train", "review_pool_variant": "ml_blind_snapshot_audit", "ranking_run_id": "rank-ee2ba6c816", "row_id": "c"},
        _blind_row(row_id="a"),
    )
    rows, dup = filter_blind_rows(p, ranking_run_id="rank-ee2ba6c816")
    assert len(rows) == 1 and dup == 1


def test_filter_blind_rows_other_run_excluded() -> None:
    p = _payload(
        {"split": "audit_only", "review_pool_variant": "ml_blind_snapshot_audit", "ranking_run_id": "rank-other", "row_id": "z"},
    )
    rows, dup = filter_blind_rows(p, ranking_run_id="rank-ee2ba6c816")
    assert rows == [] and dup == 0


def test_compute_metrics_counts_classes_and_diagnostic_auc() -> None:
    rows = [
        _blind_row(row_id="1", good=True, scores={"bridge": 0.9}, ranks={"bridge": 5}),
        _blind_row(row_id="2", good=True, scores={"bridge": 0.8}, ranks={"bridge": 7}),
        _blind_row(row_id="3", good=False, scores={"bridge": 0.1}, ranks={"bridge": 100}),
        _blind_row(row_id="4", good=False, scores={"bridge": 0.2}, ranks={"bridge": 90}),
    ]
    m = compute_family_context_metrics(rows)
    bridge_go = m["bridge"]["good_or_acceptable"]
    assert bridge_go["positive_count"] == 2
    assert bridge_go["negative_count"] == 2
    assert bridge_go["null_count"] == 0
    assert bridge_go["mean_family_score_positive"] == pytest.approx(0.85)
    assert bridge_go["mean_family_score_negative"] == pytest.approx(0.15)
    assert bridge_go["median_rank_positive"] == pytest.approx(6.0)
    assert bridge_go["median_rank_negative"] == pytest.approx(95.0)
    assert bridge_go["diagnostic_auc_family_score"] == pytest.approx(1.0)


def test_compute_metrics_auc_none_when_only_one_class() -> None:
    rows = [
        _blind_row(row_id="1", good=True, scores={"emerging": 0.5}, ranks={"emerging": 10}),
        _blind_row(row_id="2", good=True, scores={"emerging": 0.6}, ranks={"emerging": 20}),
    ]
    m = compute_family_context_metrics(rows)
    em_go = m["emerging"]["good_or_acceptable"]
    assert em_go["positive_count"] == 2 and em_go["negative_count"] == 0
    assert em_go["diagnostic_auc_family_score"] is None


def test_metrics_skip_rows_lacking_family_context_key() -> None:
    rows = [
        _blind_row(row_id="1", good=True, scores={"bridge": 0.9}, ranks={"bridge": 5}),
        _blind_row(row_id="2", good=True, scores={"emerging": 0.4}, ranks={"emerging": 30}),
    ]
    m = compute_family_context_metrics(rows)
    assert m["bridge"]["good_or_acceptable"]["rows_with_family_score"] == 1
    assert m["emerging"]["good_or_acceptable"]["rows_with_family_score"] == 1
    assert m["undercited"]["good_or_acceptable"]["rows_with_family_score"] == 0


def test_build_payload_does_not_mutate_family(tmp_path: Path) -> None:
    rows = [_blind_row(row_id="1", scores={"bridge": 0.5}, ranks={"bridge": 10})]
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(_payload(*rows)), encoding="utf-8")
    payload = build_blind_family_context_eval_payload(
        label_dataset_path=p, ranking_run_id="rank-ee2ba6c816"
    )
    assert payload["blind_row_summary"]["all_rows_family_null"] is True
    raw = json.loads(p.read_text(encoding="utf-8"))
    assert all(r.get("family") is None for r in raw["rows"])


def test_build_payload_no_inferred_labels(tmp_path: Path) -> None:
    """Diagnostic must not invent labels from context fields; only use explicit derived targets."""
    rows = [
        _blind_row(
            row_id="1",
            relevance=None,
            novelty=None,
            bridge_like=None,
            good=None,
            surp=None,
            bridge_yes=None,
            scores={"bridge": 0.9},
            ranks={"bridge": 1},
        )
    ]
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(_payload(*rows)), encoding="utf-8")
    payload = build_blind_family_context_eval_payload(
        label_dataset_path=p, ranking_run_id="rank-ee2ba6c816"
    )
    bridge_go = payload["metrics"]["by_family_context"]["bridge"]["good_or_acceptable"]
    assert bridge_go["positive_count"] == 0
    assert bridge_go["negative_count"] == 0
    assert bridge_go["null_count"] == 1
    assert bridge_go["diagnostic_auc_family_score"] is None


def test_build_payload_errors_when_no_blind_rows(tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(_payload()), encoding="utf-8")
    with pytest.raises(MLBlindFamilyContextEvalError):
        build_blind_family_context_eval_payload(
            label_dataset_path=p, ranking_run_id="rank-ee2ba6c816"
        )


def test_markdown_caveats_and_diagnostic_framing() -> None:
    payload = {
        "diagnostic_kind": "blind_source_family_context_diagnostic",
        "generated_at": "t",
        "provenance": {
            "ranking_run_id": "r",
            "label_dataset_path": "/x.json",
            "label_dataset_version": "ml-label-dataset-v4",
            "label_dataset_sha256": "h",
            "review_pool_variant": "ml_blind_snapshot_audit",
        },
        "caveats": list(CAVEATS),
        "blind_row_summary": {
            "blind_rows_included": 60,
            "duplicate_row_id_skipped": 0,
            "rows_with_family_scores_context": 60,
            "rows_with_family_ranks_context": 60,
            "context_family_keys_seen": ["bridge", "emerging", "undercited"],
            "all_rows_family_null": True,
        },
        "metrics": {
            "by_family_context": {
                "bridge": {
                    "good_or_acceptable": {
                        "positive_count": 1,
                        "negative_count": 1,
                        "null_count": 0,
                        "rows_with_family_score": 2,
                        "rows_with_family_rank": 2,
                        "median_rank_positive": 5.0,
                        "median_rank_negative": 90.0,
                        "mean_family_score_positive": 0.5,
                        "mean_family_score_negative": 0.1,
                        "diagnostic_auc_family_score": 0.75,
                    },
                    "surprising_or_useful": {
                        "positive_count": 0,
                        "negative_count": 0,
                        "null_count": 0,
                        "rows_with_family_score": 0,
                        "rows_with_family_rank": 0,
                        "median_rank_positive": None,
                        "median_rank_negative": None,
                        "mean_family_score_positive": None,
                        "mean_family_score_negative": None,
                        "diagnostic_auc_family_score": None,
                    },
                    "bridge_like_yes_or_partial": {
                        "positive_count": 0,
                        "negative_count": 0,
                        "null_count": 0,
                        "rows_with_family_score": 0,
                        "rows_with_family_rank": 0,
                        "median_rank_positive": None,
                        "median_rank_negative": None,
                        "mean_family_score_positive": None,
                        "mean_family_score_negative": None,
                        "diagnostic_auc_family_score": None,
                    },
                },
                "emerging": {
                    t: {
                        "positive_count": 0,
                        "negative_count": 0,
                        "null_count": 0,
                        "rows_with_family_score": 0,
                        "rows_with_family_rank": 0,
                        "median_rank_positive": None,
                        "median_rank_negative": None,
                        "mean_family_score_positive": None,
                        "mean_family_score_negative": None,
                        "diagnostic_auc_family_score": None,
                    }
                    for t in (
                        "good_or_acceptable",
                        "surprising_or_useful",
                        "bridge_like_yes_or_partial",
                    )
                },
                "undercited": {
                    t: {
                        "positive_count": 0,
                        "negative_count": 0,
                        "null_count": 0,
                        "rows_with_family_score": 0,
                        "rows_with_family_rank": 0,
                        "median_rank_positive": None,
                        "median_rank_negative": None,
                        "mean_family_score_positive": None,
                        "mean_family_score_negative": None,
                        "diagnostic_auc_family_score": None,
                    }
                    for t in (
                        "good_or_acceptable",
                        "surprising_or_useful",
                        "bridge_like_yes_or_partial",
                    )
                },
            }
        },
    }
    md = markdown_from_blind_family_context_eval(payload)
    for c in CAVEATS:
        assert c in md
    assert "not validation" in md.lower()
    assert "blind-source family-context diagnostic" in md.lower()
    assert "context fields, not labels" in md.lower()
    assert "production ranking" in md.lower()


def test_write_no_db_imports_or_psycopg(tmp_path: Path) -> None:
    """Module must not import psycopg or perform DB writes."""
    import pipeline.ml_blind_family_context_eval as m

    src = Path(m.__file__).read_text(encoding="utf-8")
    assert "import psycopg" not in src
    assert "psycopg.connect" not in src


def test_write_files(tmp_path: Path) -> None:
    rows = [
        _blind_row(row_id="1", good=True, scores={"bridge": 0.9}, ranks={"bridge": 5}),
        _blind_row(row_id="2", good=False, scores={"bridge": 0.1}, ranks={"bridge": 90}),
    ]
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(_payload(*rows)), encoding="utf-8")
    j = tmp_path / "out.json"
    md = tmp_path / "out.md"
    write_blind_family_context_eval(
        label_dataset_path=p,
        ranking_run_id="rank-ee2ba6c816",
        json_path=j,
        markdown_path=md,
    )
    assert j.is_file() and md.is_file()
    payload = json.loads(j.read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "ml_blind_family_context_eval"
    assert payload["diagnostic_kind"] == "blind_source_family_context_diagnostic"


def test_cli_writes_artifact(tmp_path: Path) -> None:
    rows = [
        _blind_row(row_id="1", good=True, scores={"bridge": 0.9}, ranks={"bridge": 5}),
        _blind_row(row_id="2", good=False, scores={"bridge": 0.1}, ranks={"bridge": 90}),
    ]
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(_payload(*rows)), encoding="utf-8")
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-blind-family-context-eval",
        "--label-dataset",
        str(p),
        "--ranking-run-id",
        "rank-ee2ba6c816",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()
    assert out_json.is_file() and out_md.is_file()
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["blind_row_summary"]["blind_rows_included"] == 2
    assert payload["blind_row_summary"]["all_rows_family_null"] is True


def test_cli_requires_non_empty_ranking_run_id(tmp_path: Path) -> None:
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-blind-family-context-eval",
        "--label-dataset",
        str(tmp_path / "nope.json"),
        "--ranking-run-id",
        "   ",
        "--output",
        str(tmp_path / "out.json"),
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()
