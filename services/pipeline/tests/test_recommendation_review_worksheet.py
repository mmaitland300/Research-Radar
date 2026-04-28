"""Unit tests for recommendation review worksheet (no live Postgres)."""

import csv
import inspect
import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main
from pipeline.recommendation_review_worksheet import (
    WORKSHEET_COLUMNS,
    WorksheetError,
    build_worksheet_rows,
    cluster_version_from_config,
    format_bridge_eligible_for_csv,
    render_worksheet_csv,
    write_recommendation_review_worksheet,
)


def test_cluster_version_from_config_present() -> None:
    assert (
        cluster_version_from_config(
            {
                "clustering_artifact": {
                    "cluster_version": "kmeans-v0",
                }
            }
        )
        == "kmeans-v0"
    )


def test_cluster_version_from_config_absent() -> None:
    assert cluster_version_from_config({}) is None
    assert cluster_version_from_config({"clustering_artifact": {}}) is None
    assert cluster_version_from_config({"clustering_artifact": {"cluster_version": ""}}) is None


def test_format_bridge_eligible() -> None:
    assert format_bridge_eligible_for_csv(True) == "true"
    assert format_bridge_eligible_for_csv(False) == "false"
    assert format_bridge_eligible_for_csv(None) == ""


def test_bridge_signal_not_in_query() -> None:
    from pipeline import recommendation_review_worksheet as m

    src = inspect.getsource(m._fetch_scored_rows)
    assert "bridge_signal_json" not in src


def _run_row() -> dict:
    return {
        "ranking_run_id": "r1",
        "ranking_version": "v0",
        "corpus_snapshot_version": "snap-a",
        "embedding_version": "embed-a",
        "cluster_version": "k-v1",
        "family": "bridge",
        "rank": "1",
        "paper_id": "P1",
        "title": "T1",
        "year": "2020",
        "citation_count": "5",
        "source_slug": "src",
        "topics": "a;b",
        "final_score": "0.5",
        "reason_short": "r1",
        "semantic_score": "0.1",
        "citation_velocity_score": "0.2",
        "topic_growth_score": "0.3",
        "bridge_score": "0.4",
        "diversity_penalty": "0.0",
        "bridge_eligible": "true",
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def test_render_worksheet_deterministic() -> None:
    a = [dict(_run_row())]
    b = [dict(_run_row())]
    assert render_worksheet_csv(a) == render_worksheet_csv(b)
    t = render_worksheet_csv(a)
    t2 = render_worksheet_csv(a)
    assert t == t2


def test_reviewer_columns_blank() -> None:
    row = _run_row()
    assert row["relevance_label"] == ""
    assert row["bridge_like_label"] == ""


def _mock_conn_succeeded(
    rows: list[dict] | None = None,
    *,
    run_status: str = "succeeded",
    run_id: str = "run-xyz",
    config_json: dict | None = None,
) -> MagicMock:
    if rows is None:
        rows = [
            {
                "rank": 1,
                "paper_id": "W10",
                "title": "Same score",
                "year": 2021,
                "citation_count": 1,
                "source_slug": "s",
                "topics": '["A","B"]',
                "final_score": 0.5,
                "reason_short": "a",
                "semantic_score": 0.1,
                "citation_velocity_score": 0.2,
                "topic_growth_score": 0.3,
                "bridge_score": 0.4,
                "diversity_penalty": 0.0,
                "bridge_eligible": True,
            }
        ]
    res_run = MagicMock()
    cfg = (
        config_json
        if config_json is not None
        else {
            "clustering_artifact": {
                "cluster_version": "cv-99",
            }
        }
    )
    res_run.fetchone.return_value = {
        "ranking_run_id": run_id,
        "ranking_version": "v-label",
        "corpus_snapshot_version": "ssv-1",
        "embedding_version": "emb-1",
        "config_json": cfg,
        "status": run_status,
    }
    res_scores = MagicMock()
    res_scores.fetchall.return_value = rows
    conn = MagicMock()
    conn.execute.side_effect = [res_run, res_scores]
    return conn


def test_empty_ranking_run_id_raises() -> None:
    conn = MagicMock()
    with pytest.raises(WorksheetError, match="ranking-run-id"):
        build_worksheet_rows(conn, ranking_run_id="  ", family="bridge", limit=5)


def test_invalid_family_raises() -> None:
    conn = MagicMock()
    with pytest.raises(WorksheetError, match="Invalid"):
        build_worksheet_rows(conn, ranking_run_id="r1", family="lunar", limit=5)


def test_run_missing_raises() -> None:
    res = MagicMock()
    res.fetchone.return_value = None
    conn = MagicMock()
    conn.execute.return_value = res
    with pytest.raises(WorksheetError, match="not found") as ei:
        build_worksheet_rows(conn, ranking_run_id="nope", family="emerging", limit=5)
    assert ei.value.code == 2


def test_run_not_succeeded_raises() -> None:
    res_run = MagicMock()
    res_run.fetchone.return_value = {
        "ranking_run_id": "r1",
        "status": "failed",
        "config_json": {},
    }
    conn = MagicMock()
    conn.execute.return_value = res_run
    with pytest.raises(WorksheetError, match="not succeeded"):
        build_worksheet_rows(conn, ranking_run_id="r1", family="emerging", limit=1)


def test_cluster_version_blank_without_artifact() -> None:
    rows: list[dict] = [
        {
            "rank": 1,
            "paper_id": "A",
            "title": "t1",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 0.6,
            "reason_short": "q",
            "semantic_score": None,
            "citation_velocity_score": None,
            "topic_growth_score": None,
            "bridge_score": None,
            "diversity_penalty": None,
            "bridge_eligible": None,
        }
    ]
    conn = _mock_conn_succeeded(rows, run_id="rr2", config_json={})
    out = build_worksheet_rows(conn, ranking_run_id="rr2", family="emerging", limit=10)
    assert out[0]["cluster_version"] == ""


def test_provenance_repeated_and_cluster_version() -> None:
    rows: list[dict] = [
        {
            "rank": 1,
            "paper_id": "A",
            "title": "t1",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 0.6,
            "reason_short": "q",
            "semantic_score": None,
            "citation_velocity_score": None,
            "topic_growth_score": None,
            "bridge_score": None,
            "diversity_penalty": None,
            "bridge_eligible": None,
        }
    ]
    conn = _mock_conn_succeeded(rows, run_id="rr-pin")
    out = build_worksheet_rows(conn, ranking_run_id="rr-pin", family="emerging", limit=10)
    assert len(out) == 1
    r0 = out[0]
    assert r0["ranking_run_id"] == "rr-pin"
    assert r0["corpus_snapshot_version"] == "ssv-1"
    assert r0["cluster_version"] == "cv-99"
    assert r0["review_pool_variant"] == "full_family_top_k"
    for col in (
        "ranking_run_id",
        "ranking_version",
        "corpus_snapshot_version",
        "embedding_version",
        "cluster_version",
        "family",
        "rank",
    ):
        assert r0[col]


def test_ordering_same_score_work_id() -> None:
    rows = [
        {
            "rank": 1,
            "paper_id": "P-low",
            "title": "a",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 0.5,
            "reason_short": "a",
            "semantic_score": None,
            "citation_velocity_score": None,
            "topic_growth_score": None,
            "bridge_score": None,
            "diversity_penalty": None,
            "bridge_eligible": True,
        },
        {
            "rank": 2,
            "paper_id": "P-high",
            "title": "b",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 0.5,
            "reason_short": "b",
            "semantic_score": None,
            "citation_velocity_score": None,
            "topic_growth_score": None,
            "bridge_score": None,
            "diversity_penalty": None,
            "bridge_eligible": False,
        },
    ]
    conn = _mock_conn_succeeded(rows)
    out = build_worksheet_rows(conn, ranking_run_id="run-xyz", family="bridge", limit=10)
    assert out[0]["rank"] == "1" and out[0]["paper_id"] == "P-low"
    assert out[1]["rank"] == "2" and out[1]["paper_id"] == "P-high"
    # tie-break: DB orders work_id asc; our mock emulates that ordering for rank


def test_bridge_eligible_variants() -> None:
    rows = [
        {
            "rank": 1,
            "paper_id": "1",
            "title": "t",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 1.0,
            "reason_short": "x",
            "semantic_score": 0.0,
            "citation_velocity_score": 0.0,
            "topic_growth_score": 0.0,
            "bridge_score": 0.0,
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
        },
        {
            "rank": 2,
            "paper_id": "2",
            "title": "t2",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 0.9,
            "reason_short": "y",
            "semantic_score": 0.0,
            "citation_velocity_score": 0.0,
            "topic_growth_score": 0.0,
            "bridge_score": 0.0,
            "diversity_penalty": 0.0,
            "bridge_eligible": False,
        },
        {
            "rank": 3,
            "paper_id": "3",
            "title": "t3",
            "year": 2020,
            "citation_count": 0,
            "source_slug": None,
            "topics": "[]",
            "final_score": 0.8,
            "reason_short": "z",
            "semantic_score": 0.0,
            "citation_velocity_score": 0.0,
            "topic_growth_score": 0.0,
            "bridge_score": 0.0,
            "diversity_penalty": 0.0,
            "bridge_eligible": None,
        },
    ]
    conn = _mock_conn_succeeded(rows)
    out = build_worksheet_rows(conn, ranking_run_id="run-xyz", family="bridge", limit=10)
    assert out[0]["bridge_eligible"] == "true"
    assert out[1]["bridge_eligible"] == "false"
    assert out[2]["bridge_eligible"] == ""


def test_bridge_eligible_only_rejected_for_non_bridge_family() -> None:
    conn = _mock_conn_succeeded()
    with pytest.raises(WorksheetError, match="only valid with --family bridge"):
        build_worksheet_rows(
            conn,
            ranking_run_id="run-xyz",
            family="emerging",
            limit=10,
            bridge_eligible_only=True,
        )


def test_bridge_eligible_only_query_filter_and_variant_provenance() -> None:
    conn = _mock_conn_succeeded()
    out = build_worksheet_rows(
        conn,
        ranking_run_id="run-xyz",
        family="bridge",
        limit=10,
        bridge_eligible_only=True,
    )
    assert out[0]["review_pool_variant"] == "bridge_eligible_only"
    # second execute call is scored-row query
    q = conn.execute.call_args_list[1].args[0]
    assert "AND ps.bridge_eligible IS TRUE" in q


def test_default_bridge_does_not_add_eligible_only_filter() -> None:
    conn = _mock_conn_succeeded()
    out = build_worksheet_rows(conn, ranking_run_id="run-xyz", family="bridge", limit=10)
    assert out[0]["review_pool_variant"] == "full_family_top_k"
    q = conn.execute.call_args_list[1].args[0]
    assert "AND ps.bridge_eligible IS TRUE" not in q


def test_worksheet_header_columns() -> None:
    row = {c: _run_row().get(c, "") for c in WORKSHEET_COLUMNS}
    out = render_worksheet_csv([row])
    first = out.splitlines()[0]
    parsed = next(csv.reader(io.StringIO(first)))
    assert parsed == list(WORKSHEET_COLUMNS)
    for c in WORKSHEET_COLUMNS:
        assert c in out


@patch("pipeline.recommendation_review_worksheet.psycopg.connect")
def test_write_uses_path(mock_connect: MagicMock, tmp_path: Path) -> None:
    mconn = _mock_conn_succeeded()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mconn
    mock_cm.__exit__.return_value = None
    mock_connect.return_value = mock_cm
    outp = tmp_path / "w.csv"
    write_recommendation_review_worksheet(
        output_path=outp, database_url="postgres://x", ranking_run_id="run-xyz", family="emerging", limit=5
    )
    text = outp.read_text(encoding="utf-8")
    assert "run-xyz" in text
    assert "relevance_label" in text
    assert "review_pool_variant" in text
    assert outp.is_file()


@patch("pipeline.cli.write_recommendation_review_worksheet")
def test_cli_invokes_worksheet(
    mock_write: MagicMock,
) -> None:
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-worksheet",
            "--ranking-run-id",
            "RUN1",
            "--family",
            "bridge",
            "--limit",
            "20",
            "--output",
            "out.csv",
        ],
    ):
        cli_main.main()
    mock_write.assert_called_once()
    k = mock_write.call_args.kwargs
    assert k["ranking_run_id"] == "RUN1"
    assert k["family"] == "bridge"
    assert k["limit"] == 20
    assert k["bridge_eligible_only"] is False
    p = k["output_path"]
    assert isinstance(p, Path)
    assert p.name == "out.csv"


@patch("pipeline.cli.write_recommendation_review_worksheet")
def test_cli_bridge_eligible_only_passthrough(
    mock_write: MagicMock,
) -> None:
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-worksheet",
            "--ranking-run-id",
            "RUN1",
            "--family",
            "bridge",
            "--bridge-eligible-only",
            "--limit",
            "20",
            "--output",
            "out.csv",
        ],
    ):
        cli_main.main()
    k = mock_write.call_args.kwargs
    assert k["bridge_eligible_only"] is True


def test_cli_rejects_bridge_eligible_only_for_non_bridge() -> None:
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-worksheet",
            "--ranking-run-id",
            "RUN1",
            "--family",
            "emerging",
            "--bridge-eligible-only",
            "--limit",
            "20",
            "--output",
            "out.csv",
        ],
    ):
        with pytest.raises(SystemExit):
            cli_main.main()
