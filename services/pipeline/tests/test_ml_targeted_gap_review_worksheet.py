"""Tests for ml-targeted-gap-review-worksheet (emerging gap; read-only; no training)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.ml_targeted_gap_review_worksheet import (
    GAP_SAMPLE_REASONS,
    VERBATIM_GAP_CAVEATS,
    MLTargetedGapReviewWorksheetError,
    EmergingRow,
    build_targeted_gap_worksheet,
    markdown_gap_report,
    render_gap_csv,
    select_emerging_gap_rows,
    _rows_to_csv_dicts,
)


def _er(
    *,
    fr: int,
    wt: str,
    fs: float,
    cv: float | None = 0.3,
    tg: float | None = 0.3,
    sem: float | None = 0.5,
    topics: list | None = None,
) -> EmergingRow:
    pid = f"https://openalex.org/{wt}"
    return EmergingRow(
        family_rank=fr,
        paper_id=pid,
        work_token=wt.upper(),
        final_score=fs,
        semantic_score=sem,
        citation_velocity_score=cv,
        topic_growth_score=tg,
        bridge_score=None,
        diversity_penalty=0.0,
        bridge_eligible=None,
        reason_short="rs",
        title="T",
        year=2024,
        citation_count=1,
        source_slug="s",
        topics_raw=topics if topics is not None else ["a", "b"],
    )


def test_select_excludes_complete_labeled_tokens() -> None:
    pool = [_er(fr=i, wt=f"W{i}", fs=0.5 - i * 0.01) for i in range(1, 21)]
    complete = {("emerging", "W5"), ("emerging", "W10")}
    sel = select_emerging_gap_rows(pool, complete_keys=complete, limit=12)
    tokens = {t[0].work_token for t in sel}
    assert "W5" not in tokens and "W10" not in tokens


def test_select_deterministic() -> None:
    pool = [_er(fr=i, wt=f"W{i:02d}", fs=0.5) for i in range(1, 30)]
    complete: set[tuple[str, str]] = set()
    a = select_emerging_gap_rows(pool, complete_keys=complete, limit=15)
    b = select_emerging_gap_rows(pool, complete_keys=complete, limit=15)
    assert [x[0].work_token for x in a] == [x[0].work_token for x in b]


def test_sample_reasons_allowed() -> None:
    pool = [_er(fr=i, wt=f"WX{i}", fs=0.4 + i * 0.001) for i in range(1, 80)]
    sel = select_emerging_gap_rows(pool, complete_keys=set(), limit=25)
    for _c, sr in sel:
        assert sr in GAP_SAMPLE_REASONS


def test_csv_rows_blank_labels_and_openalex_ids() -> None:
    run = {
        "ranking_run_id": "rank-x",
        "ranking_version": "v",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
    }
    c = _er(fr=5, wt="W4414199528", fs=0.2)
    rows = _rows_to_csv_dicts(run=run, cluster_ver="cl", target_gap="good_or_acceptable", selected=[(c, "low_topic_growth")])
    r = rows[0]
    assert r["relevance_label"] == "" and r["novelty_label"] == "" and r["bridge_like_label"] == "" and r["reviewer_notes"] == ""
    assert r["paper_id"].startswith("https://openalex.org/")
    assert r["work_id"] == "W4414199528"
    assert r["family"] == "emerging"


def test_markdown_caveats_and_ascii() -> None:
    run = {"ranking_version": "rv", "corpus_snapshot_version": "cs", "embedding_version": "ev"}
    c = _er(fr=1, wt="W1", fs=0.1)
    md = markdown_gap_report(
        ranking_run_id="rank-ee2ba6c816",
        target_gap="good_or_acceptable",
        run=run,
        cluster_ver="k",
        label_dataset_path=Path("docs/audit/ml-label-dataset-v2.json"),
        selected=[(c, "emerging_bottom_rank_tail")],
    )
    for line in VERBATIM_GAP_CAVEATS:
        assert line in md
    assert "â" not in md and "â€" not in md and "Ã" not in md


def test_fetch_family_sql_is_select_only() -> None:
    src = Path(__file__).resolve().parents[1] / "pipeline" / "ml_contrastive_review_worksheet.py"
    text = src.read_text(encoding="utf-8")
    q_start = text.find('q = """')
    q_end = text.find('"""', q_start + 5)
    q = text[q_start : q_end + 3]
    assert "INSERT" not in q.upper() and "UPDATE" not in q.upper() and "DELETE" not in q.upper()


def test_build_raises_missing_dataset(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    with pytest.raises(MLTargetedGapReviewWorksheetError, match="label dataset not found"):
        build_targeted_gap_worksheet(
            database_url="postgresql://invalid",
            label_dataset_path=missing,
            ranking_run_id="rank-x",
            family="emerging",
            target_gap="good_or_acceptable",
            limit=5,
        )


def test_build_raises_wrong_family() -> None:
    with pytest.raises(MLTargetedGapReviewWorksheetError, match="--family must be"):
        build_targeted_gap_worksheet(
            database_url="postgresql://x",
            label_dataset_path=Path("x.json"),
            ranking_run_id="r",
            family="bridge",
            target_gap="good_or_acceptable",
            limit=5,
        )


def test_build_raises_invalid_ranking_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
            return m

    monkeypatch.setattr(
        "pipeline.ml_targeted_gap_review_worksheet.psycopg.connect",
        lambda *a, **k: _Conn(),
    )
    with pytest.raises(MLTargetedGapReviewWorksheetError, match="ranking_run_id not found"):
        build_targeted_gap_worksheet(
            database_url="postgresql://mock",
            label_dataset_path=p,
            ranking_run_id="rank-missing",
            family="emerging",
            target_gap="surprising_or_useful",
            limit=5,
        )


def test_render_gap_csv_header() -> None:
    run = {"ranking_run_id": "r", "ranking_version": "v", "corpus_snapshot_version": "s", "embedding_version": "e"}
    c = _er(fr=2, wt="W2", fs=0.3)
    text = render_gap_csv(_rows_to_csv_dicts(run=run, cluster_ver="", target_gap="good_or_acceptable", selected=[(c, "weak_emerging_signal")]))
    h = text.splitlines()[0]
    assert "sample_reason" in h and "bridge_eligible" in h and "diversity_penalty" in h
