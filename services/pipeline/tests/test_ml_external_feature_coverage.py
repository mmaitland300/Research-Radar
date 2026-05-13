"""Tests for external near-miss feature coverage diagnostic."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.ml_external_feature_coverage import (
    REVIEW_POOL_VARIANT,
    build_external_feature_coverage_payload,
    render_markdown,
    select_external_rows,
)


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict]:
        return list(self._rows)


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.executed_sql: list[str] = []
        self.executed_params: list[tuple] = []

    def execute(self, query: str, params: tuple | None = None) -> _Result:
        self.executed_sql.append(query)
        self.executed_params.append(params or ())
        return _Result(self.rows)


def _external_row(index: int, *, abstract_preview: str = "preview text") -> dict:
    token = f"W{index}"
    return {
        "dataset_version": "ml-label-dataset-v7",
        "row_id": f"row-{index}",
        "paper_id": f"https://openalex.org/{token}",
        "work_id": token,
        "openalex_work_id": token,
        "title": f"External fixture title {index}",
        "year": "2025",
        "citation_count": str(index),
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "split": "audit_only",
        "family": None,
        "ranking_run_id": None,
        "sample_reason": "adjacent_audio_not_mir",
        "cluster_id": "ext",
        "topics": "Audio;Recommender",
        "abstract_preview": abstract_preview,
        "external_near_miss_context": {
            "row_id": f"row-{index}",
            "paper_id": f"https://openalex.org/{token}",
            "openalex_work_id": token,
            "sample_reason": "adjacent_audio_not_mir",
            "cluster_id": "ext",
            "source_metadata": {"source_display_name": "Fixture Journal"},
            "hidden_diagnostics": {"query_match_strategy": "adjacent_audio_not_mir"},
            "review_metadata": {
                "abstract_preview": "context preview " + ("x" * 230),
            },
        },
    }


def _write_label_dataset(tmp_path: Path) -> Path:
    payload = {
        "dataset_version": "ml-label-dataset-v7",
        "rows": [
            _external_row(1, abstract_preview=""),
            _external_row(2, abstract_preview="short"),
            _external_row(3, abstract_preview="dataset preview " + ("y" * 220)),
            {"row_id": "other", "review_pool_variant": "ml_blind_snapshot_audit"},
        ],
    }
    path = tmp_path / "ml-label-dataset-v7.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _write_sidecar(tmp_path: Path, *, mismatch: bool = False) -> Path:
    rows = [
        {"row_id": "row-1"},
        {"row_id": "row-2"},
        {"row_id": "row-extra" if mismatch else "row-3"},
    ]
    path = tmp_path / "ml_external_near_miss_review_v1_context.json"
    path.write_text(
        json.dumps(
            {
                "artifact_type": "ml_external_near_miss_review_v1_context",
                "provenance": {"corpus_snapshot_version": "snapshot-test"},
                "rows": rows,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _db_rows() -> list[dict]:
    return [
        {
            "internal_work_id": 101,
            "openalex_id": "https://openalex.org/W1",
            "title": "DB title one",
            "abstract": "DB abstract " + ("a" * 260),
            "inclusion_status": "included",
            "corpus_snapshot_version": "snapshot-test",
            "embedding_row_present": True,
            "representative_paper_scores_count": 1,
            "corpus_v2_embed_eligible": True,
        },
        {
            "internal_work_id": 102,
            "openalex_id": "W2",
            "title": "DB title two",
            "abstract": "",
            "inclusion_status": "included",
            "corpus_snapshot_version": "other-snapshot",
            "embedding_row_present": True,
            "representative_paper_scores_count": 0,
            "corpus_v2_embed_eligible": False,
        },
    ]


def test_select_external_rows_only() -> None:
    payload = {"rows": [_external_row(1), {"row_id": "x", "review_pool_variant": "other"}]}
    rows = select_external_rows(payload)
    assert len(rows) == 1
    assert rows[0]["review_pool_variant"] == REVIEW_POOL_VARIANT


def test_external_feature_coverage_aggregates_and_text_heuristic(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    sidecar_path = _write_sidecar(tmp_path)
    conn = _FakeConn(_db_rows())
    payload = build_external_feature_coverage_payload(
        conn,
        label_dataset_path=label_path,
        context_sidecar_path=sidecar_path,
        embedding_version="emb-test",
    )

    agg = payload["aggregates"]
    assert agg["external_row_count"] == 3
    assert agg["works_row_present_count"] == 2
    assert agg["embedding_row_present_count"] == 2
    assert agg["corpus_v2_embed_eligible_count"] == 1
    assert agg["representative_paper_scores_present_count"] == 1
    assert agg["sufficient_text_for_embedding_heuristic_count"] == 2
    assert payload["provenance"]["context_sidecar"]["row_id_sets_match"] is True
    assert payload["repo_accurate_nuance"]["embedding_row_present_definition"]

    row1 = next(row for row in payload["rows"] if row["work_token"] == "W1")
    assert row1["db_coverage"]["works_row_present"] is True
    assert row1["embedding_row_present"] is True
    assert row1["corpus_v2_embed_eligible"] is True
    assert row1["text_coverage"]["text_source"] == "db_title_plus_db_abstract"

    row2 = next(row for row in payload["rows"] if row["work_token"] == "W2")
    assert row2["embedding_row_present"] is True
    assert row2["corpus_v2_embed_eligible"] is False
    assert row2["sufficient_text_for_embedding_heuristic"] is False

    row3 = next(row for row in payload["rows"] if row["work_token"] == "W3")
    assert row3["db_coverage"]["works_row_present"] is False
    assert row3["text_coverage"]["text_source"] == "dataset_title_plus_dataset_abstract_preview"
    assert row3["sufficient_text_for_embedding_heuristic"] is True


def test_context_preview_used_when_dataset_preview_missing(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    conn = _FakeConn([])
    payload = build_external_feature_coverage_payload(
        conn,
        label_dataset_path=label_path,
        context_sidecar_path=None,
        embedding_version="emb-test",
    )
    row1 = next(row for row in payload["rows"] if row["work_token"] == "W1")
    assert row1["text_coverage"]["text_source"] == "dataset_title_plus_context_review_abstract_preview"
    assert row1["sufficient_text_for_embedding_heuristic"] is True


def test_sidecar_row_id_mismatch_is_reported_not_hidden(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    sidecar_path = _write_sidecar(tmp_path, mismatch=True)
    conn = _FakeConn([])
    payload = build_external_feature_coverage_payload(
        conn,
        label_dataset_path=label_path,
        context_sidecar_path=sidecar_path,
        embedding_version="emb-test",
    )
    sidecar = payload["provenance"]["context_sidecar"]
    assert sidecar["row_id_sets_match"] is False
    assert sidecar["missing_in_sidecar"] == ["row-3"]
    assert sidecar["extra_in_sidecar"] == ["row-extra"]


def test_markdown_contains_required_caveats_and_feature_notes(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    conn = _FakeConn(_db_rows())
    payload = build_external_feature_coverage_payload(
        conn,
        label_dataset_path=label_path,
        context_sidecar_path=None,
        embedding_version="emb-test",
    )
    md = render_markdown(payload)
    assert "Not validation" in md
    assert "Preview text is not a full abstract" in md
    assert "Embedding presence is not a production ranking signal" in md
    assert "corpus_v2_embed" in md
    assert "review_pool_variant" in md


def test_sql_is_read_only(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    conn = _FakeConn(_db_rows())
    build_external_feature_coverage_payload(
        conn,
        label_dataset_path=label_path,
        context_sidecar_path=None,
        embedding_version="emb-test",
    )
    sql = "\n".join(conn.executed_sql).upper()
    assert "SELECT" in sql
    for bad in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE "):
        assert bad not in sql
