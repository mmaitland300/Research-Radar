"""Tests for reviewer-blind hard-negative / near-miss worksheet."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest

from pipeline.ml_hard_negative_review_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    CSV_COLUMNS,
    HIDDEN_REVIEWER_CSV_FIELDS,
    REVIEW_POOL_VARIANT,
    WORKSHEET_VERSION,
    any_labeled_work_tokens,
    build_hard_negative_review_worksheet,
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
    def __init__(self, *, raw_rows: list[dict] | None = None) -> None:
        self.executed_sql: list[str] = []
        self.raw_rows = raw_rows if raw_rows is not None else _raw_rows()

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: tuple | None = None) -> _Result:
        self.executed_sql.append(query)
        if "FROM clustering_runs" in query:
            return _Result(
                one={
                    "corpus_snapshot_version": "snapshot-test",
                    "embedding_version": "embedding-test",
                    "status": "succeeded",
                }
            )
        if "FROM ranking_runs" in query:
            return _Result(
                one={
                    "ranking_run_id": "rank-test",
                    "ranking_version": "rv",
                    "corpus_snapshot_version": "snapshot-test",
                    "embedding_version": "embedding-test",
                    "status": "succeeded",
                }
            )
        if "FROM works w" in query:
            return _Result(many=self.raw_rows)
        if "FROM paper_scores ps" in query and "RANK() OVER" in query:
            return _Result(many=_rank_context_rows(self.raw_rows))
        if "FROM paper_scores ps" in query:
            return _Result(many=_score_feature_rows(self.raw_rows))
        return _Result()


def _raw_rows() -> list[dict]:
    cases = [
        (1, "Urban sound classification for smart city monitoring", "Environmental sound detection and traffic noise.", "c001"),
        (2, "Already touched music recommendation", "Music recommendation baseline.", "c001"),
        (3, "Music therapy recommender for patient wellness", "Music therapy and health support system.", "c002"),
        (4, "Weak melody metadata for classroom listening", "Educational classroom listening with light music terms.", "c002"),
        (5, "Broad audio embeddings for machine diagnostics", "Audio representation for industrial machinery.", "c003"),
        (6, "Database optimization for media catalogs", "A generic catalog indexing paper.", "c003"),
    ]
    rows: list[dict] = []
    for wid, title, abstract, cluster in cases:
        rows.append(
            {
                "internal_work_id": wid,
                "paper_id": f"https://openalex.org/W{wid}",
                "title": title,
                "year": 2024,
                "citation_count": wid,
                "source_slug": "fixture",
                "work_type": "article",
                "abstract": abstract,
                "cluster_id": cluster,
                "topics": json.dumps(["Audio Metadata", "Education"] if wid in {3, 4} else ["Industrial Signals"]),
            }
        )
    return rows


def _rank_context_rows(raw_rows: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for idx, raw in enumerate(raw_rows, start=1):
        wid = int(raw["internal_work_id"])
        rows.append(
            {
                "work_id": wid,
                "recommendation_family": "emerging",
                "final_score": 0.05 + idx * 0.01,
                "family_rank": idx,
            }
        )
    return rows


def _score_feature_rows(raw_rows: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for idx, raw in enumerate(raw_rows, start=1):
        wid = int(raw["internal_work_id"])
        rows.append(
            {
                "work_id": wid,
                "recommendation_family": "emerging",
                "final_score": 0.05 + idx * 0.01,
                "semantic_score": 0.2 + idx * 0.01,
                "citation_velocity_score": 0.0,
                "topic_growth_score": 0.0,
                "diversity_penalty": 0.0,
                "bridge_score": 0.0,
            }
        )
    return rows


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    label_path = tmp_path / "ml-label-dataset-v5.json"
    label_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "paper_id": "https://openalex.org/W2",
                        "work_id": "W2",
                        "relevance_label": "good",
                        "novelty_label": "",
                        "bridge_like_label": "",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    conflict_path = tmp_path / "ml-label-conflict-policy.md"
    conflict_path.write_text("# conflict policy\n", encoding="utf-8")
    return label_path, conflict_path


def _build(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    rows: int = 4,
    seed: int = 123,
    raw_rows: list[dict] | None = None,
) -> tuple[str, dict, str, dict, _FakeConn]:
    conn = _FakeConn(raw_rows=raw_rows)
    monkeypatch.setattr("pipeline.ml_hard_negative_review_worksheet.psycopg.connect", lambda *args, **kwargs: conn)
    label_path, conflict_path = _write_inputs(tmp_path)
    csv_text, context, markdown, debug = build_hard_negative_review_worksheet(
        database_url="postgresql://fake",
        label_dataset_path=label_path,
        conflict_policy_path=conflict_path,
        corpus_snapshot_version="snapshot-test",
        embedding_version="embedding-test",
        cluster_version="cluster-version-test",
        ranking_run_id="rank-test",
        rows=rows,
        seed=seed,
        csv_output_path=tmp_path / "worksheet.csv",
        context_output_path=tmp_path / "context.json",
        markdown_output_path=tmp_path / "worksheet.md",
    )
    return csv_text, context, markdown, debug, conn


def _csv_rows(csv_text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(csv_text)))


def test_closed_sample_reason_vocabulary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, context, _markdown, _debug, _conn = _build(monkeypatch, tmp_path)
    csv_reasons = {row["sample_reason"] for row in _csv_rows(csv_text)}
    sidecar_reasons = {row["sample_reason"] for row in context["rows"]}
    assert csv_reasons
    assert csv_reasons <= set(ALLOWED_SAMPLE_REASONS)
    assert sidecar_reasons <= set(ALLOWED_SAMPLE_REASONS)


def test_csv_excludes_forbidden_fields_and_version_strings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, _context, _markdown, _debug, _conn = _build(monkeypatch, tmp_path)
    reader = csv.DictReader(io.StringIO(csv_text))
    assert tuple(reader.fieldnames or ()) == CSV_COLUMNS
    for hidden in HIDDEN_REVIEWER_CSV_FIELDS:
        assert hidden not in (reader.fieldnames or [])
    assert "snapshot-test" not in csv_text
    assert "embedding-test" not in csv_text
    assert "cluster-version-test" not in csv_text
    assert "final_score" not in csv_text
    assert "ranking_run_id" not in csv_text
    assert "internal_work_id" not in csv_text


def test_sidecar_contains_hidden_context_and_versions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, context, markdown, debug, _conn = _build(monkeypatch, tmp_path)
    csv_ids = {row["row_id"] for row in _csv_rows(csv_text)}
    sidecar_ids = {row["row_id"] for row in context["rows"]}
    assert csv_ids == sidecar_ids
    assert context["artifact_type"] == "ml_hard_negative_review_v1_context"
    assert context["provenance"]["corpus_snapshot_version"] == "snapshot-test"
    assert context["provenance"]["embedding_version"] == "embedding-test"
    assert context["provenance"]["cluster_version"] == "cluster-version-test"
    first = context["rows"][0]
    assert "internal_work_id" in first
    assert first["ranking_run_id"] == "rank-test"
    assert first["emerging_paper_scores"]["final_score"] is not None
    assert first["selection_auxiliary_scores"]
    assert "Future Ingest Note" in markdown
    assert debug["achieved_rows"] == len(context["rows"])


def test_any_labeled_work_tokens_excludes_partial_labels() -> None:
    payload = {
        "rows": [
            {"paper_id": "https://openalex.org/W1", "relevance_label": "", "novelty_label": "", "bridge_like_label": ""},
            {"paper_id": "https://openalex.org/W2", "relevance_label": "good", "novelty_label": "", "bridge_like_label": ""},
            {"work_id": "W3", "relevance_label": "", "novelty_label": "useful", "bridge_like_label": ""},
        ]
    }
    assert any_labeled_work_tokens(payload) == {"W2", "W3"}


def test_exclusion_removes_any_previously_labeled_work(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, context, _markdown, debug, _conn = _build(monkeypatch, tmp_path, rows=4)
    csv_work_ids = {row["work_id"] for row in _csv_rows(csv_text)}
    assert "W2" not in csv_work_ids
    assert all(row["openalex_work_id"] != "W2" for row in context["rows"])
    assert debug["any_labeled_excluded_count"] == 1


def test_deterministic_output_for_fixed_seed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_a, context_a, _md_a, _debug_a, _conn_a = _build(monkeypatch, tmp_path / "a", seed=42)
    csv_b, context_b, _md_b, _debug_b, _conn_b = _build(monkeypatch, tmp_path / "b", seed=42)
    assert csv_a == csv_b
    assert [row["row_id"] for row in context_a["rows"]] == [row["row_id"] for row in context_b["rows"]]
    for row in _csv_rows(csv_a):
        assert row["row_id"] == stable_row_id(
            worksheet_version=WORKSHEET_VERSION,
            sample_seed=42,
            paper_id=row["paper_id"],
        )


def test_shortfall_reporting_when_candidate_pool_too_small(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, context, markdown, debug, _conn = _build(monkeypatch, tmp_path, rows=3, raw_rows=_raw_rows()[:1])
    assert len(_csv_rows(csv_text)) == 1
    assert len(context["rows"]) == 1
    assert debug["shortfall_count"] == 2
    assert debug["pool_supported_requested_rows"] is False
    assert "Shortfall" in markdown


def test_label_columns_blank_in_generated_csv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_text, _context, _markdown, _debug, _conn = _build(monkeypatch, tmp_path)
    for row in _csv_rows(csv_text):
        assert row["relevance_label"] == ""
        assert row["novelty_label"] == ""
        assert row["bridge_like_label"] == ""
        assert row["reviewer_notes"] == ""
        assert row["worksheet_version"] == WORKSHEET_VERSION
        assert row["review_pool_variant"] == REVIEW_POOL_VARIANT


def test_no_db_writes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _csv_text, _context, _markdown, _debug, conn = _build(monkeypatch, tmp_path)
    sql = "\n".join(conn.executed_sql).upper()
    for bad in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE "):
        assert bad not in sql
