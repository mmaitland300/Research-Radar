"""Tests for ml-contrastive-review-worksheet (read-only sampling; no ranking/model changes)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.ml_contrastive_review_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    CSV_COLUMNS,
    VERBATIM_MARKDOWN_CAVEATS,
    MLContrastiveReviewWorksheetError,
    build_contrastive_worksheet,
    build_label_key_sets,
    fetch_family_scored_rows,
    markdown_report,
    paper_key_family,
    raw_row_to_candidate,
    render_csv,
    row_dict_to_csv_row,
    select_contrastive_for_family,
    ContrastiveCandidate,
)


def _cand(
    *,
    family: str,
    fr: int,
    wt: str,
    fs: float,
    cv: float | None = 0.5,
    tg: float | None = 0.5,
    bs: float | None = 0.5,
    elig: bool | None = True,
    cites: int = 10,
) -> ContrastiveCandidate:
    pid = f"https://openalex.org/{wt}"
    return ContrastiveCandidate(
        family=family,
        family_rank=fr,
        paper_id=pid,
        work_token=wt.upper(),
        final_score=fs,
        semantic_score=0.1,
        citation_velocity_score=cv,
        topic_growth_score=tg,
        bridge_score=bs,
        diversity_penalty=0.0,
        bridge_eligible=elig,
        reason_short="r",
        title="t",
        year=2020,
        citation_count=cites,
        source_slug="s",
        topics_raw=[],
    )


def test_build_label_key_sets_complete_and_incomplete() -> None:
    payload = {
        "rows": [
            {
                "split": "audit_only",
                "ranking_run_id": "rank-x",
                "family": "bridge",
                "paper_id": "https://openalex.org/W111",
                "relevance_label": "good",
                "novelty_label": "useful",
                "bridge_like_label": "yes",
            },
            {
                "split": "audit_only",
                "ranking_run_id": "rank-x",
                "family": "bridge",
                "paper_id": "https://openalex.org/W222",
                "relevance_label": "good",
                "novelty_label": "",
                "bridge_like_label": "",
            },
            {
                "split": "audit_only",
                "ranking_run_id": "other",
                "family": "bridge",
                "paper_id": "https://openalex.org/W333",
                "relevance_label": "good",
                "novelty_label": "useful",
                "bridge_like_label": "yes",
            },
        ]
    }
    complete, incomplete = build_label_key_sets(payload, ranking_run_id="rank-x")
    assert ("bridge", "W111") in complete
    assert ("bridge", "W222") in incomplete
    assert ("bridge", "W333") not in complete and ("bridge", "W333") not in incomplete


def test_excludes_complete_labeled_papers() -> None:
    c1 = _cand(family="bridge", fr=1, wt="W1", fs=0.99)
    c2 = _cand(family="bridge", fr=2, wt="W2", fs=0.98)
    complete = {("bridge", "W1")}
    incomplete: set[tuple[str, str]] = set()
    sel = select_contrastive_for_family(
        "bridge",
        [c1, c2],
        per_family=5,
        complete_keys=complete,
        incomplete_keys=incomplete,
    )
    tokens = [t[0].work_token for t in sel]
    assert "W1" not in tokens
    assert "W2" in tokens


def test_median_borderline_selects_closest_to_median_first() -> None:
    """median_borderline must follow borderline_sorted (distance to median), not family_rank order."""
    # Ranks 1-6 only: lower_rank_window is empty. Median of scores is between 0.51 and 0.52 -> ~0.515;
    # closest rows are W3 and W4 (tie-break by rank), then W2.
    pool = [
        _cand(family="bridge", fr=1, wt="W1", fs=0.10, bs=0.9, elig=True),
        _cand(family="bridge", fr=2, wt="W2", fs=0.50, bs=0.9, elig=True),
        _cand(family="bridge", fr=3, wt="W3", fs=0.51, bs=0.9, elig=True),
        _cand(family="bridge", fr=4, wt="W4", fs=0.52, bs=0.9, elig=True),
        _cand(family="bridge", fr=5, wt="W5", fs=0.90, bs=0.9, elig=True),
        _cand(family="bridge", fr=6, wt="W6", fs=0.91, bs=0.9, elig=True),
    ]
    sel = select_contrastive_for_family(
        "bridge",
        pool,
        per_family=6,
        complete_keys=set(),
        incomplete_keys=set(),
    )
    mb = [t[0].work_token for t in sel if t[1] == "median_borderline"]
    assert mb[:3] == ["W3", "W4", "W2"]


def test_label_incomplete_bucket_reason() -> None:
    c1 = _cand(family="emerging", fr=50, wt="W50", fs=0.5)
    c2 = _cand(family="emerging", fr=51, wt="W51", fs=0.49)
    complete: set[tuple[str, str]] = set()
    incomplete = {("emerging", "W50")}
    sel = select_contrastive_for_family(
        "emerging",
        [c1, c2],
        per_family=5,
        complete_keys=complete,
        incomplete_keys=incomplete,
    )
    first = sel[0]
    assert first[0].work_token == "W50"
    assert first[1] == "label_incomplete"


def test_deterministic_ordering_repeatable() -> None:
    pool = [_cand(family="undercited", fr=i, wt=f"W{i}", fs=1.0 - i * 0.001) for i in range(1, 120)]
    complete: set[tuple[str, str]] = set()
    incomplete: set[tuple[str, str]] = set()
    a = select_contrastive_for_family(
        "undercited",
        pool,
        per_family=10,
        complete_keys=complete,
        incomplete_keys=incomplete,
    )
    b = select_contrastive_for_family(
        "undercited",
        pool,
        per_family=10,
        complete_keys=complete,
        incomplete_keys=incomplete,
    )
    assert [x[0].work_token for x in a] == [x[0].work_token for x in b]


def test_blank_label_columns_on_rendered_rows() -> None:
    run = {
        "ranking_run_id": "rank-x",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
    }
    c = _cand(family="bridge", fr=5, wt="W5", fs=0.5)
    rows = row_dict_to_csv_row(run=run, cluster_ver="cl", selected=[(c, "lower_rank_window")])
    assert rows[0]["relevance_label"] == ""
    assert rows[0]["novelty_label"] == ""
    assert rows[0]["bridge_like_label"] == ""
    assert rows[0]["reviewer_notes"] == ""


def test_csv_provenance_fields() -> None:
    run = {
        "ranking_run_id": "rank-x",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
    }
    c = _cand(family="emerging", fr=1, wt="W9", fs=0.9)
    rows = row_dict_to_csv_row(run=run, cluster_ver="cl-v", selected=[(c, "fallback_deterministic_fill")])
    r = rows[0]
    assert r["ranking_run_id"] == "rank-x"
    assert r["ranking_version"] == "rv"
    assert r["corpus_snapshot_version"] == "snap"
    assert r["embedding_version"] == "emb"
    assert r["cluster_version"] == "cl-v"


def test_paper_id_and_work_id_semantics() -> None:
    run = {
        "ranking_run_id": "r",
        "ranking_version": "rv",
        "corpus_snapshot_version": "s",
        "embedding_version": "e",
    }
    c = _cand(family="bridge", fr=1, wt="W4414199528", fs=0.7)
    rows = row_dict_to_csv_row(run=run, cluster_ver="", selected=[(c, "median_borderline")])
    assert rows[0]["paper_id"].startswith("https://openalex.org/")
    assert rows[0]["work_id"] == "W4414199528"
    assert rows[0]["work_id"].startswith("W") and rows[0]["work_id"][1:].isdigit()


def test_raw_row_to_candidate_extracts_work_token() -> None:
    row = {
        "family_rank": 3,
        "paper_id": "https://openalex.org/W1234567890",
        "final_score": 0.4,
        "semantic_score": None,
        "citation_velocity_score": 0.1,
        "topic_growth_score": 0.2,
        "bridge_score": 0.3,
        "diversity_penalty": 0.0,
        "bridge_eligible": True,
        "reason_short": "x",
        "title": "T",
        "year": 2019,
        "citation_count": 5,
        "source_slug": "slug",
        "topics": [],
    }
    c = raw_row_to_candidate("bridge", row)
    assert c is not None
    assert c.work_token == "W1234567890"


def test_sample_reasons_are_allowed() -> None:
    pool = [_cand(family="bridge", fr=i, wt=f"W{i}", fs=0.5 - i * 0.001) for i in range(1, 100)]
    sel = select_contrastive_for_family(
        "bridge",
        pool,
        per_family=20,
        complete_keys=set(),
        incomplete_keys=set(),
    )
    for _c, sr in sel:
        assert sr in ALLOWED_SAMPLE_REASONS


def test_fetch_sql_is_select_only() -> None:
    src = Path(__file__).resolve().parents[1] / "pipeline" / "ml_contrastive_review_worksheet.py"
    text = src.read_text(encoding="utf-8")
    q_start = text.find('q = """')
    q_end = text.find('"""', q_start + 5)
    q = text[q_start : q_end + 3]
    assert "INSERT" not in q.upper()
    assert "UPDATE" not in q.upper()
    assert "DELETE" not in q.upper()
    assert "SELECT" in q.upper()


def test_markdown_contains_verbatim_caveats() -> None:
    run = {"ranking_version": "a", "corpus_snapshot_version": "b", "embedding_version": "c"}
    md = markdown_report(
        ranking_run_id="rank-z",
        run=run,
        cluster_ver="cv",
        label_dataset_path=Path("docs/audit/ml-label-dataset-v1.json"),
        selected_by_family={"bridge": [], "emerging": [], "undercited": []},
        duplicate_notes=[],
    )
    for line in VERBATIM_MARKDOWN_CAVEATS:
        assert line in md
    assert "contrastive" in md.lower()
    assert "validation" in md.lower()
    assert "**40-80**" in md
    assert "â€" not in md


def test_missing_label_dataset_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    with pytest.raises(MLContrastiveReviewWorksheetError, match="label dataset not found"):
        build_contrastive_worksheet(
            database_url="postgresql://invalid",
            label_dataset_path=missing,
            ranking_run_id="rank-x",
            per_family=5,
        )


def test_invalid_ranking_run_raises_with_clear_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps({"rows": []}), encoding="utf-8")

    class _Conn:
        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def execute(self, sql: str, params: object | None = None) -> MagicMock:
            m = MagicMock()

            def fetchone() -> None:
                return None

            m.fetchone = fetchone
            if "FROM ranking_runs" in sql:
                m.fetchone = fetchone
            return m

    monkeypatch.setattr(
        "pipeline.ml_contrastive_review_worksheet.psycopg.connect",
        lambda *a, **k: _Conn(),
    )
    with pytest.raises(MLContrastiveReviewWorksheetError, match="ranking_run_id not found"):
        build_contrastive_worksheet(
            database_url="postgresql://mock",
            label_dataset_path=p,
            ranking_run_id="rank-missing",
            per_family=5,
        )


def test_csv_columns_include_family_and_sample_reason() -> None:
    assert "family" in CSV_COLUMNS
    assert "sample_reason" in CSV_COLUMNS
    assert "family_rank" in CSV_COLUMNS


def test_render_csv_roundtrip_header() -> None:
    run = {
        "ranking_run_id": "r",
        "ranking_version": "v",
        "corpus_snapshot_version": "s",
        "embedding_version": "e",
    }
    c = _cand(family="undercited", fr=10, wt="W10", fs=0.2)
    rows = row_dict_to_csv_row(run=run, cluster_ver="k", selected=[(c, "weak_family_signal")])
    text = render_csv(rows)
    header = text.splitlines()[0]
    assert "sample_reason" in header
    assert "work_id" in header
