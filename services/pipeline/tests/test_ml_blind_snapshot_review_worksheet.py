"""Tests for ml-blind-snapshot-review-worksheet (deterministic, non-rank-driven)."""

from __future__ import annotations

import csv
import io
import json
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.ml_blind_snapshot_review_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    CSV_COLUMNS,
    VERBATIM_CAVEATS,
    WORKSHEET_VERSION,
    BlindCandidate,
    MLBlindSnapshotReviewWorksheetError,
    _row_to_csv_dict,
    build_blind_snapshot_review_worksheet,
    fully_labeled_work_tokens,
    render_csv,
    render_markdown,
    select_blind_sample,
)


def _cand(
    *,
    wid: int,
    cluster: str = "0",
    year: int | None = 2023,
    cites: int = 5,
    family_scores: dict[str, float] | None = None,
    family_ranks: dict[str, int] | None = None,
    abstract: str = "Abstract content here.",
    work_type: str = "article",
    source: str = "neurips",
) -> BlindCandidate:
    token = f"W{wid}"
    return BlindCandidate(
        internal_work_id=wid,
        paper_id=f"https://openalex.org/{token}",
        work_token=token,
        title=f"Title {wid}",
        year=year,
        citation_count=cites,
        source_slug=source,
        work_type=work_type,
        cluster_id=cluster,
        topics=("topic-a", "topic-b"),
        abstract=abstract,
        family_scores=dict(family_scores) if family_scores else {},
        family_ranks=dict(family_ranks) if family_ranks else {},
    )


def _make_pool(num_clusters: int = 12, per_cluster: int = 30) -> list[BlindCandidate]:
    pool: list[BlindCandidate] = []
    wid = 1
    for cidx in range(num_clusters):
        for k in range(per_cluster):
            year = 2018 + (k % 8)
            cites = (k * 7) % 250
            scores = {"emerging": 0.5 + (wid % 7) * 0.01}
            ranks = {"emerging": (wid * 3) % 1000}
            if k % 5 == 0:
                scores = {}
                ranks = {}
            pool.append(
                _cand(
                    wid=wid,
                    cluster=str(cidx),
                    year=year,
                    cites=cites,
                    family_scores=scores,
                    family_ranks=ranks,
                )
            )
            wid += 1
    return pool


def test_select_is_deterministic_for_same_seed() -> None:
    pool = _make_pool()
    a, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=20260430)
    b, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=20260430)
    assert [c.work_token for c, _r in a] == [c.work_token for c, _r in b]


def test_select_different_seed_changes_order() -> None:
    pool = _make_pool()
    a, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=1)
    b, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=2)
    assert [c.work_token for c, _r in a] != [c.work_token for c, _r in b]


def test_select_excludes_fully_labeled_tokens() -> None:
    pool = _make_pool()
    blocked = {pool[0].work_token, pool[1].work_token, pool[37].work_token}
    selected, debug = select_blind_sample(
        pool, fully_labeled_tokens=blocked, total_rows=60, seed=20260430
    )
    tokens = {c.work_token for c, _r in selected}
    assert blocked.isdisjoint(tokens)
    assert int(debug["fully_labeled_excluded_count"]) == len(blocked)


def test_select_uses_only_allowed_sample_reasons() -> None:
    pool = _make_pool()
    selected, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=42)
    reasons = {r for _c, r in selected}
    assert reasons.issubset(set(ALLOWED_SAMPLE_REASONS))


def test_select_hits_target_when_pool_is_large() -> None:
    pool = _make_pool()
    selected, debug = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=7)
    assert len(selected) == 60
    assert int(debug["achieved_rows"]) == 60


def test_select_under_pool_capacity_returns_all_eligible() -> None:
    pool = _make_pool(num_clusters=2, per_cluster=4)
    selected, debug = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=9)
    assert len(selected) == len(pool)
    assert int(debug["achieved_rows"]) == len(pool)


def test_select_preserves_cluster_coverage() -> None:
    pool = _make_pool()
    selected, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=3)
    by_cluster = Counter(c.cluster_id for c, _ in selected)
    assert len(by_cluster) == 12
    assert min(by_cluster.values()) >= 1


def test_select_uses_only_cluster_stratified_when_total_eq_clusters() -> None:
    pool = _make_pool()
    selected, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=12, seed=11)
    reasons = {r for _c, r in selected}
    assert reasons == {"cluster_stratified_seeded"}


def test_select_emits_expected_reason_counts_for_60_seed_20260430() -> None:
    pool = _make_pool()
    selected, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=20260430)
    by_reason = Counter(sr for _c, sr in selected)
    assert by_reason["cluster_stratified_seeded"] == 48
    assert by_reason["year_band_seeded"] == 4
    assert by_reason["citation_band_seeded"] == 4
    assert by_reason["weak_family_context_seeded"] == 4
    assert sum(by_reason.values()) == 60


def test_csv_row_blank_labels_and_openalex_ids() -> None:
    cand = _cand(wid=4414199528, family_scores={"bridge": 0.31}, family_ranks={"bridge": 12})
    row = _row_to_csv_dict(
        cand=cand,
        sample_reason="cluster_stratified_seeded",
        seed=42,
        corpus_snapshot_version="snap",
        embedding_version="emb",
        cluster_version="cl",
        ranking_run_id_context="rank-x",
    )
    assert row["relevance_label"] == ""
    assert row["novelty_label"] == ""
    assert row["bridge_like_label"] == ""
    assert row["reviewer_notes"] == ""
    assert row["paper_id"].startswith("https://openalex.org/W")
    assert row["openalex_work_id"] == "W4414199528"
    assert row["internal_work_id"] == "4414199528"
    assert row["worksheet_version"] == WORKSHEET_VERSION
    assert row["sample_seed"] == "42"


def test_csv_does_not_expose_internal_id_under_work_id_column() -> None:
    cand = _cand(wid=999, family_scores={}, family_ranks={})
    row = _row_to_csv_dict(
        cand=cand,
        sample_reason="cluster_stratified_seeded",
        seed=1,
        corpus_snapshot_version="snap",
        embedding_version="emb",
        cluster_version="cl",
        ranking_run_id_context="rank-x",
    )
    assert "work_id" not in CSV_COLUMNS
    assert "openalex_work_id" in CSV_COLUMNS
    assert "internal_work_id" in CSV_COLUMNS
    assert row["openalex_work_id"] == "W999"
    assert row["openalex_work_id"] != row["internal_work_id"]


def test_csv_header_columns_blank_label_columns_and_no_score_ordering() -> None:
    pool = _make_pool()
    selected, _ = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=60, seed=20260430)
    csv_rows = [
        _row_to_csv_dict(
            cand=c,
            sample_reason=sr,
            seed=20260430,
            corpus_snapshot_version="snap",
            embedding_version="emb",
            cluster_version="cl",
            ranking_run_id_context="rank-ee2ba6c816",
        )
        for c, sr in selected
    ]
    text = render_csv(csv_rows)
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    for col in ("relevance_label", "novelty_label", "bridge_like_label", "reviewer_notes"):
        assert col in fieldnames
    out_rows = list(reader)
    assert len(out_rows) == 60
    for r in out_rows:
        assert r["relevance_label"] == ""
        assert r["novelty_label"] == ""
        assert r["bridge_like_label"] == ""
        assert r["reviewer_notes"] == ""


def test_fully_labeled_work_tokens_only_when_all_three_filled() -> None:
    payload = {
        "rows": [
            {
                "work_id": "W1",
                "paper_id": "https://openalex.org/W1",
                "relevance_label": "good",
                "novelty_label": "useful",
                "bridge_like_label": "yes",
            },
            {
                "work_id": "W2",
                "paper_id": "https://openalex.org/W2",
                "relevance_label": "good",
                "novelty_label": "",
                "bridge_like_label": "",
            },
            {
                "work_id": "",
                "paper_id": "https://openalex.org/W3",
                "relevance_label": "good",
                "novelty_label": "useful",
                "bridge_like_label": "no",
            },
        ]
    }
    tokens = fully_labeled_work_tokens(payload)
    assert tokens == {"W1", "W3"}


def test_markdown_includes_verbatim_caveats_and_provenance() -> None:
    pool = _make_pool(num_clusters=3, per_cluster=10)
    selected, debug = select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=15, seed=5)
    csv_rows = [
        _row_to_csv_dict(
            cand=c,
            sample_reason=sr,
            seed=5,
            corpus_snapshot_version="snap",
            embedding_version="emb",
            cluster_version="cl",
            ranking_run_id_context="rank-x",
        )
        for c, sr in selected
    ]
    md = render_markdown(
        csv_rows=csv_rows,
        selected=selected,
        debug=debug,
        seed=5,
        corpus_snapshot_version="snap",
        embedding_version="emb",
        cluster_version="cl",
        ranking_run_id_context="rank-x",
        label_dataset_path=Path("docs/audit/ml-label-dataset-v3.json"),
        csv_output_path=Path("out.csv"),
        markdown_output_path=Path("out.md"),
        requested_rows=15,
    )
    for line in VERBATIM_CAVEATS:
        assert line in md
    assert "snap" in md
    assert "rank-x" in md
    assert "sample_seed" in md
    assert "Row counts by sample_reason" in md
    assert "Row counts by cluster_id" in md


def test_module_has_no_write_sql() -> None:
    src = (
        Path(__file__).resolve().parents[1]
        / "pipeline"
        / "ml_blind_snapshot_review_worksheet.py"
    )
    text = src.read_text(encoding="utf-8").upper()
    for tok in ("INSERT ", "UPDATE ", "DELETE ", "DROP "):
        assert tok not in text, f"Unexpected write SQL token in module: {tok!r}"


def test_build_raises_missing_label_dataset(tmp_path: Path) -> None:
    missing = tmp_path / "no.json"
    with pytest.raises(MLBlindSnapshotReviewWorksheetError, match="label dataset not found"):
        build_blind_snapshot_review_worksheet(
            database_url="postgresql://invalid",
            label_dataset_path=missing,
            corpus_snapshot_version="snap",
            embedding_version="emb",
            cluster_version="cl",
            ranking_run_id="rank-x",
            rows=10,
            seed=1,
            csv_output_path=tmp_path / "out.csv",
            markdown_output_path=tmp_path / "out.md",
        )


def test_build_raises_when_clustering_run_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps({"rows": []}), encoding="utf-8")

    class _Conn:
        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def execute(self, sql: str, params: object | None = None) -> MagicMock:
            m = MagicMock()
            m.fetchone = lambda: None
            m.fetchall = lambda: []
            return m

    monkeypatch.setattr(
        "pipeline.ml_blind_snapshot_review_worksheet.psycopg.connect",
        lambda *a, **k: _Conn(),
    )
    with pytest.raises(MLBlindSnapshotReviewWorksheetError, match="cluster_version not found"):
        build_blind_snapshot_review_worksheet(
            database_url="postgresql://mock",
            label_dataset_path=p,
            corpus_snapshot_version="snap",
            embedding_version="emb",
            cluster_version="missing-cl",
            ranking_run_id="rank-x",
            rows=10,
            seed=1,
            csv_output_path=tmp_path / "out.csv",
            markdown_output_path=tmp_path / "out.md",
        )


def test_invalid_rows_raises() -> None:
    pool = _make_pool(num_clusters=2, per_cluster=2)
    with pytest.raises(MLBlindSnapshotReviewWorksheetError, match="--rows must be between"):
        select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=0, seed=1)
    with pytest.raises(MLBlindSnapshotReviewWorksheetError, match="--rows must be between"):
        select_blind_sample(pool, fully_labeled_tokens=set(), total_rows=10_000, seed=1)
