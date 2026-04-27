"""Tests for recommendation review worksheet summary (no live Postgres)."""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
from pipeline.recommendation_review_worksheet import WORKSHEET_COLUMNS, render_worksheet_csv
from pipeline.recommendation_review_summary import (
    ReviewSummaryError,
    build_recommendation_review_summary,
    read_worksheet_path,
    run_recommendation_review_summary,
)


def _full_row(
    *,
    rank: str = "1",
    paper_id: str = "P1",
    ranking_run_id: str = "run-a",
    corpus_snapshot_version: str = "snap-1",
    family: str = "bridge",
    relevance_label: str = "good",
    novelty_label: str = "useful",
    bridge_like_label: str = "yes",
) -> dict[str, str]:
    return {
        "ranking_run_id": ranking_run_id,
        "ranking_version": "rv-1",
        "corpus_snapshot_version": corpus_snapshot_version,
        "embedding_version": "emb-1",
        "cluster_version": "cl-1",
        "family": family,
        "rank": rank,
        "paper_id": paper_id,
        "title": "T",
        "year": "2020",
        "citation_count": "0",
        "source_slug": "s",
        "topics": "",
        "final_score": "0.1",
        "reason_short": "",
        "semantic_score": "",
        "citation_velocity_score": "",
        "topic_growth_score": "",
        "bridge_score": "",
        "diversity_penalty": "",
        "bridge_eligible": "true",
        "relevance_label": relevance_label,
        "novelty_label": novelty_label,
        "bridge_like_label": bridge_like_label,
        "reviewer_notes": "",
    }


def test_complete_worksheet_passes() -> None:
    rows = [
        _full_row(rank="1", paper_id="A", relevance_label="good", novelty_label="useful", bridge_like_label="yes"),
        _full_row(rank="2", paper_id="B", relevance_label="acceptable", novelty_label="obvious", bridge_like_label="no"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["is_complete"] is True
    assert s["row_count"] == 2
    assert s["warnings"] == []
    assert s["label_counts"]["relevance_label"]["good"] == 1
    assert s["label_counts"]["relevance_label"]["acceptable"] == 1


def test_blank_relevance_fails() -> None:
    rows = [_full_row(relevance_label="")]
    with pytest.raises(ReviewSummaryError) as ei:
        build_recommendation_review_summary(
            rows, input_path=Path("w.csv"), allow_incomplete=False
        )
    assert ei.value.code == 2
    m = str(ei.value)
    assert "relevance_label" in m
    assert "P1" in m
    assert "data row 1" in m


def test_invalid_relevance_fails() -> None:
    rows = [_full_row(relevance_label="maybe")]
    with pytest.raises(ReviewSummaryError) as ei:
        build_recommendation_review_summary(
            rows, input_path=Path("w.csv"), allow_incomplete=False
        )
    assert ei.value.code == 2
    assert "relevance_label" in str(ei.value)
    assert "invalid" in str(ei.value).lower() or "expected" in str(ei.value).lower()


def test_allow_incomplete_sets_incomplete() -> None:
    rows = [
        _full_row(
            paper_id="X1",
            relevance_label="",
            novelty_label="useful",
            bridge_like_label="yes",
        )
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=True
    )
    assert s["is_complete"] is False
    assert any("incomplete" in w.lower() for w in s["warnings"])


def test_label_counts() -> None:
    rows = [
        _full_row(
            paper_id="1",
            relevance_label="miss",
            novelty_label="not_useful",
            bridge_like_label="partial",
        ),
        _full_row(
            paper_id="2",
            relevance_label="irrelevant",
            novelty_label="surprising",
            bridge_like_label="not_applicable",
        ),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["label_counts"]["relevance_label"]["miss"] == 1
    assert s["label_counts"]["relevance_label"]["irrelevant"] == 1
    assert s["label_counts"]["novelty_label"]["not_useful"] == 1
    assert s["label_counts"]["novelty_label"]["surprising"] == 1


def test_precision_good_only() -> None:
    rows = [
        _full_row(paper_id="1", relevance_label="good"),
        _full_row(paper_id="2", relevance_label="good"),
        _full_row(paper_id="3", relevance_label="miss"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["metrics"]["precision_at_k_good_only"] == pytest.approx(2 / 3)


def test_precision_good_or_acceptable() -> None:
    rows = [
        _full_row(paper_id="1", relevance_label="good"),
        _full_row(paper_id="2", relevance_label="acceptable"),
        _full_row(paper_id="3", relevance_label="irrelevant"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["metrics"]["precision_at_k_good_or_acceptable"] == pytest.approx(2 / 3)


def test_bridge_share_excludes_not_applicable_from_denominator() -> None:
    rows = [
        _full_row(paper_id="1", bridge_like_label="yes"),
        _full_row(paper_id="2", bridge_like_label="not_applicable"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["metrics"]["bridge_like_yes_or_partial_share"] == 1.0


def test_bridge_share_null_all_not_applicable() -> None:
    rows = [
        _full_row(paper_id="1", bridge_like_label="not_applicable"),
        _full_row(paper_id="2", bridge_like_label="not_applicable"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["metrics"]["bridge_like_yes_or_partial_share"] is None


def test_surprising_or_useful_share() -> None:
    rows = [
        _full_row(paper_id="1", novelty_label="useful"),
        _full_row(paper_id="2", novelty_label="obvious"),
        _full_row(paper_id="3", novelty_label="surprising"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert s["metrics"]["surprising_or_useful_share"] == pytest.approx(2 / 3)


def test_mixed_run_and_snapshot_warns() -> None:
    rows = [
        _full_row(
            paper_id="1", ranking_run_id="r1", corpus_snapshot_version="s1"
        ),
        _full_row(
            paper_id="2", ranking_run_id="r2", corpus_snapshot_version="s2"
        ),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    w = " ".join(s["warnings"])
    assert "ranking_run_id" in w
    assert "corpus_snapshot" in w


def test_mixed_family_warns() -> None:
    rows = [
        _full_row(paper_id="1", family="bridge"),
        _full_row(paper_id="2", family="emerging"),
    ]
    s = build_recommendation_review_summary(
        rows, input_path=Path("w.csv"), allow_incomplete=False
    )
    assert any("family" in x.lower() for x in s["warnings"])


def test_read_worksheet_path_roundtrip(tmp_path: Path) -> None:
    rows = [
        _full_row(),
    ]
    csv_text = render_worksheet_csv(rows)
    p = tmp_path / "in.csv"
    p.write_text(csv_text, encoding="utf-8", newline="")
    loaded = read_worksheet_path(p)
    assert len(loaded) == 1
    assert loaded[0]["paper_id"] == "P1"
    for c in WORKSHEET_COLUMNS:
        assert c in loaded[0]


def test_cli_writes_json(tmp_path: Path) -> None:
    rows = [
        _full_row(paper_id="Z9"),
    ]
    inp = tmp_path / "sheet.csv"
    out = tmp_path / "out.json"
    inp.write_text(render_worksheet_csv(rows), encoding="utf-8", newline="")
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-summary",
            "--input",
            str(inp),
            "--output",
            str(out),
        ],
    ):
        cli_main.main()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["is_complete"] is True
    assert data["row_count"] == 1


def test_markdown_output(tmp_path: Path) -> None:
    rows = [_full_row(paper_id="M1")]
    inp = tmp_path / "s.csv"
    out = tmp_path / "j.json"
    md = tmp_path / "m.md"
    inp.write_text(render_worksheet_csv(rows), encoding="utf-8", newline="")
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-summary",
            "--input",
            str(inp),
            "--output",
            str(out),
            "--markdown-output",
            str(md),
        ],
    ):
        cli_main.main()
    assert md.is_file()
    assert "# Recommendation review summary" in md.read_text(encoding="utf-8")


def test_cli_exit_2_on_blank(tmp_path: Path) -> None:
    rows = [
        _full_row(relevance_label=""),
    ]
    inp = tmp_path / "bad.csv"
    out = tmp_path / "nope.json"
    inp.write_text(render_worksheet_csv(rows), encoding="utf-8", newline="")
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-summary",
            "--input",
            str(inp),
            "--output",
            str(out),
        ],
    ):
        with pytest.raises(SystemExit) as ei:
            cli_main.main()
    assert ei.value.code == 2
    assert not out.is_file()
