"""Tests for reviewer-blind snapshot worksheet v2."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest

from pipeline.ml_blind_snapshot_review_worksheet import MLBlindSnapshotReviewWorksheetError
from pipeline.ml_blind_snapshot_review_worksheet_v2 import (
    HIDDEN_REVIEWER_CSV_FIELDS,
    V2_CAVEATS,
    V2_CSV_COLUMNS,
    WORKSHEET_VERSION_V2,
    build_blind_snapshot_review_worksheet_v2,
    render_v2_csv,
    render_v2_markdown,
    stable_row_id,
)


class _Result:
    def __init__(self, *, one: dict | None = None, many: list[dict] | None = None) -> None:
        self._one = one
        self._many = many or []

    def fetchone(self) -> dict | None:
        return self._one

    def fetchall(self) -> list[dict]:
        return list(self._many)


class _FakeConn:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: tuple | None = None) -> _Result:
        self.executed_sql.append(query)
        if "FROM clustering_runs" in query:
            return _Result(
                one={
                    "corpus_snapshot_version": "snap",
                    "embedding_version": "emb",
                    "status": "succeeded",
                }
            )
        if "FROM ranking_runs" in query:
            return _Result(
                one={
                    "ranking_run_id": "rank-x",
                    "ranking_version": "rv",
                    "corpus_snapshot_version": "snap",
                    "embedding_version": "emb",
                    "status": "succeeded",
                }
            )
        if "FROM works w" in query:
            return _Result(many=_raw_rows())
        if "FROM paper_scores ps" in query and "RANK() OVER" in query:
            return _Result(many=_rank_context_rows())
        if "FROM paper_scores ps" in query:
            return _Result(many=_score_feature_rows())
        return _Result()


def _raw_rows() -> list[dict]:
    rows: list[dict] = []
    for wid in range(1, 10):
        rows.append(
            {
                "internal_work_id": wid,
                "paper_id": f"https://openalex.org/W{wid}",
                "title": f"Title {wid}",
                "year": 2020 + (wid % 5),
                "citation_count": wid * 3,
                "source_slug": "ismir",
                "work_type": "article",
                "abstract": f"Abstract {wid}",
                "cluster_id": f"c{wid % 3}",
                "topics": json.dumps([f"topic-{wid}", "music"]),
            }
        )
    return rows


def _rank_context_rows() -> list[dict]:
    rows: list[dict] = []
    for wid in range(1, 10):
        rows.append(
            {
                "work_id": wid,
                "recommendation_family": "emerging",
                "final_score": 1.0 - wid * 0.01,
                "family_rank": wid,
            }
        )
    return rows


def _score_feature_rows() -> list[dict]:
    rows: list[dict] = []
    for wid in range(1, 10):
        rows.append(
            {
                "work_id": wid,
                "recommendation_family": "emerging",
                "final_score": 1.0 - wid * 0.01,
                "semantic_score": 0.1 * wid,
                "citation_velocity_score": 0.01 * wid,
                "topic_growth_score": 0.02 * wid,
                "diversity_penalty": 0.0,
                "bridge_score": 0.03 * wid,
            }
        )
    return rows


def _write_labels(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    p = tmp_path / "labels.json"
    p.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "work_id": "W2",
                        "paper_id": "https://openalex.org/W2",
                        "relevance_label": "good",
                        "novelty_label": "useful",
                        "bridge_like_label": "yes",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return p


def _build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, seed: int = 123) -> tuple[str, dict, str, dict, _FakeConn]:
    conn = _FakeConn()
    monkeypatch.setattr(
        "pipeline.ml_blind_snapshot_review_worksheet_v2.psycopg.connect",
        lambda *args, **kwargs: conn,
    )
    labels = _write_labels(tmp_path)
    csv_text, ctx, md, debug = build_blind_snapshot_review_worksheet_v2(
        database_url="postgresql://fake",
        label_dataset_path=labels,
        corpus_snapshot_version="snap",
        embedding_version="emb",
        cluster_version="cl",
        ranking_run_id="rank-x",
        rows=6,
        seed=seed,
        csv_output_path=tmp_path / "worksheet.csv",
        context_output_path=tmp_path / "context.json",
        markdown_output_path=tmp_path / "worksheet.md",
    )
    return csv_text, ctx, md, debug, conn


def _csv_rows(csv_text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(csv_text)))


def test_reviewer_csv_columns_exclude_score_rank_model_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, _ctx, _md, _debug, _conn = _build(monkeypatch, tmp_path)
    reader = csv.DictReader(io.StringIO(csv_text))
    assert tuple(reader.fieldnames or ()) == V2_CSV_COLUMNS
    for hidden in HIDDEN_REVIEWER_CSV_FIELDS:
        assert hidden not in (reader.fieldnames or [])
    assert all("score" not in col for col in reader.fieldnames or [])
    assert all("rank" not in col for col in reader.fieldnames or [])


def test_sidecar_contains_hidden_fields_and_internal_id_policy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, ctx, _md, _debug, _conn = _build(monkeypatch, tmp_path)
    row = ctx["rows"][0]
    assert "internal_work_id" in row
    assert row["ranking_run_id"] == "rank-x"
    assert row["ranking_context_family_scores_json"]
    assert row["ranking_context_family_ranks_json"]
    assert row["emerging_paper_scores"]["final_score"] is not None
    csv_row = _csv_rows(csv_text)[0]
    assert csv_row["work_id"].startswith("W")
    assert "internal_work_id" not in csv_row
    assert csv_row["work_id"] != str(row["internal_work_id"])


def test_row_id_links_csv_and_sidecar_one_to_one(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, ctx, _md, _debug, _conn = _build(monkeypatch, tmp_path, seed=777)
    csv_ids = {r["row_id"] for r in _csv_rows(csv_text)}
    ctx_ids = {r["row_id"] for r in ctx["rows"]}
    assert csv_ids == ctx_ids
    for row in _csv_rows(csv_text):
        assert row["row_id"] == stable_row_id(
            worksheet_version=WORKSHEET_VERSION_V2,
            sample_seed=777,
            paper_id=row["paper_id"],
        )


def test_deterministic_sampling_for_fixed_seed_fake_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first, first_ctx, _md1, _debug1, _conn1 = _build(monkeypatch, tmp_path / "a", seed=42)
    second, second_ctx, _md2, _debug2, _conn2 = _build(monkeypatch, tmp_path / "b", seed=42)
    assert first == second
    assert [r["row_id"] for r in first_ctx["rows"]] == [r["row_id"] for r in second_ctx["rows"]]


def test_fully_labeled_tokens_excluded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, ctx, _md, debug, _conn = _build(monkeypatch, tmp_path)
    ids = {r["openalex_work_id"] for r in _csv_rows(csv_text)}
    assert "W2" not in ids
    assert all(r["openalex_work_id"] != "W2" for r in ctx["rows"])
    assert debug["fully_labeled_excluded_count"] == 1


def test_label_columns_blank_in_generated_csv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, _ctx, _md, _debug, _conn = _build(monkeypatch, tmp_path)
    for row in _csv_rows(csv_text):
        assert row["relevance_label"] == ""
        assert row["novelty_label"] == ""
        assert row["bridge_like_label"] == ""
        assert row["reviewer_notes"] == ""


def test_markdown_mentions_caveats_and_offworksheet_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _csv_text, _ctx, md, _debug, _conn = _build(monkeypatch, tmp_path)
    for caveat in V2_CAVEATS:
        assert caveat in md
    assert "off-worksheet" in md
    assert "row_id formula" in md


def test_no_dml_sql(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _csv_text, _ctx, _md, _debug, conn = _build(monkeypatch, tmp_path)
    sql = "\n".join(conn.executed_sql).upper()
    for bad in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE "):
        assert bad not in sql


def test_render_v2_csv_never_adds_hidden_columns() -> None:
    text = render_v2_csv(
        [
            {
                "row_id": "r",
                "worksheet_version": WORKSHEET_VERSION_V2,
                "review_pool_variant": "ml_blind_snapshot_audit",
                "paper_id": "https://openalex.org/W1",
                "openalex_work_id": "W1",
                "work_id": "W1",
            }
        ]
    )
    assert "final_score" not in text
    assert "ranking_context" not in text


def test_render_markdown_documents_sidecar_schema(tmp_path: Path) -> None:
    md = render_v2_markdown(
        selected=[],
        debug={"achieved_rows": 0, "eligible_pool_size": 0, "fully_labeled_excluded_count": 0},
        seed=1,
        corpus_snapshot_version="snap",
        embedding_version="emb",
        cluster_version="cl",
        ranking_run_id="rank-x",
        label_dataset_path=tmp_path / "labels.json",
        label_dataset_sha256="abc",
        csv_output_path=tmp_path / "out.csv",
        context_output_path=tmp_path / "ctx.json",
        markdown_output_path=tmp_path / "out.md",
        requested_rows=0,
    )
    assert "Sidecar Schema" in md
    assert "internal_work_id" in md
