from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.cluster_inspection import (
    ClusterInspectionError,
    build_cluster_inspection_payload,
    render_cluster_inspection_markdown,
    run_cluster_inspection,
)


class _Result:
    def __init__(self, one=None, rows=None) -> None:
        self._one = one
        self._rows = rows or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, *, run_status: str = "succeeded", missing_cluster: bool = False) -> None:
        self.sql: list[str] = []
        self.run_status = run_status
        self.missing_cluster = missing_cluster

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False

    def execute(self, sql: str, params: tuple | None = None):
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        if compact.startswith("SELECT status, algorithm, counts_json, config_json FROM clustering_runs"):
            if params[2] != "embed-v1":
                return _Result(one=None)
            return _Result(
                one=(
                    self.run_status,
                    "kmeans-l2-v0",
                    {"cluster_count": 3, "clustered_works": 9},
                    {"cluster_count": 3},
                )
            )
        if compact.startswith("SELECT w.id, w.title, w.abstract, w.year, w.citation_count, w.source_slug, c.cluster_id"):
            c2 = None if self.missing_cluster else "c2"
            rows = [
                (
                    1,
                    "Graph Retrieval for Music",
                    "music retrieval graph embeddings",
                    2021,
                    10,
                    "core",
                    "c1",
                    True,
                    {"bucket_id": "b-a"},
                ),
                (
                    2,
                    "Temporal Models for MIR",
                    "temporal sequence retrieval",
                    2023,
                    40,
                    "core",
                    "c1",
                    True,
                    {"bucket_id": "b-b"},
                ),
                (
                    3,
                    "Contrastive Audio Representation",
                    "contrastive representation audio",
                    2022,
                    25,
                    "edge",
                    c2,
                    True,
                    {"bucket_id": "b-a"},
                ),
                (4, "Signal Processing Overview", "signal processing basics", 2018, 4, "edge", "c3", True, {}),
                (5, "Jazz Similarity Search", "jazz similarity retrieval", 2020, 18, "core", "c3", True, {}),
                (6, "Classical Theme Embeddings", "theme clustering embeddings", 2019, 7, "core", "c3", True, {}),
                (7, "Transformer Music Retrieval", "transformer retrieval study", 2024, 50, "core", "c1", True, {}),
                (8, "Low Resource Audio Tags", "audio tags classification", 2017, 1, "edge", "c1", True, {}),
                (9, "Corpus Expansion for MIR", "expansion corpus strategy", 2025, 12, "core", "c1", True, {}),
            ]
            return _Result(rows=rows)
        raise AssertionError(f"Unhandled SQL: {compact}")


def test_requires_explicit_snapshot_embedding_cluster_version() -> None:
    conn = _FakeConn()
    with pytest.raises(ClusterInspectionError, match="--corpus-snapshot-version"):
        build_cluster_inspection_payload(
            conn,
            corpus_snapshot_version="",
            embedding_version="embed-v1",
            cluster_version="cluster-v1",
        )
    with pytest.raises(ClusterInspectionError, match="--embedding-version"):
        build_cluster_inspection_payload(
            conn,
            corpus_snapshot_version="snap-v1",
            embedding_version="",
            cluster_version="cluster-v1",
        )
    with pytest.raises(ClusterInspectionError, match="--cluster-version"):
        build_cluster_inspection_payload(
            conn,
            corpus_snapshot_version="snap-v1",
            embedding_version="embed-v1",
            cluster_version="",
        )


def test_fails_if_clustering_run_missing_or_not_succeeded() -> None:
    conn_missing = _FakeConn()
    with pytest.raises(ClusterInspectionError, match="No clustering_runs row matches"):
        build_cluster_inspection_payload(
            conn_missing,
            corpus_snapshot_version="snap-v1",
            embedding_version="embed-v2",
            cluster_version="cluster-v1",
        )

    conn_failed = _FakeConn(run_status="failed")
    with pytest.raises(ClusterInspectionError, match="must be succeeded"):
        build_cluster_inspection_payload(
            conn_failed,
            corpus_snapshot_version="snap-v1",
            embedding_version="embed-v1",
            cluster_version="cluster-v1",
        )


def test_fails_if_missing_cluster_assignments() -> None:
    conn = _FakeConn(missing_cluster=True)
    with pytest.raises(ClusterInspectionError, match="missing cluster assignments"):
        build_cluster_inspection_payload(
            conn,
            corpus_snapshot_version="snap-v1",
            embedding_version="embed-v1",
            cluster_version="cluster-v1",
        )


def test_computes_cluster_size_and_imbalance_diagnostics() -> None:
    conn = _FakeConn()
    payload = build_cluster_inspection_payload(
        conn,
        corpus_snapshot_version="snap-v1",
        embedding_version="embed-v1",
        cluster_version="cluster-v1",
    )
    diag = payload["corpus_diagnostics"]
    assert diag["total_works"] == 9
    assert diag["cluster_count"] == 3
    assert diag["min_cluster_size"] == 1
    assert diag["max_cluster_size"] == 5
    assert diag["median_cluster_size"] == 3
    assert diag["imbalance_ratio"] == 5.0
    assert diag["tiny_cluster_count"] == 2
    assert diag["dominant_cluster_share"] == pytest.approx(5 / 9, rel=1e-6)


def test_representative_titles_are_deterministic() -> None:
    conn = _FakeConn()
    payload = build_cluster_inspection_payload(
        conn,
        corpus_snapshot_version="snap-v1",
        embedding_version="embed-v1",
        cluster_version="cluster-v1",
    )
    c1 = next(c for c in payload["cluster_summaries"] if c["cluster_id"] == "c1")
    assert c1["representative_titles"][:3] == [
        "Transformer Music Retrieval",
        "Temporal Models for MIR",
        "Corpus Expansion for MIR",
    ]


def test_includes_bucket_and_source_mix_when_available() -> None:
    conn = _FakeConn()
    payload = build_cluster_inspection_payload(
        conn,
        corpus_snapshot_version="snap-v1",
        embedding_version="embed-v1",
        cluster_version="cluster-v1",
    )
    c1 = next(c for c in payload["cluster_summaries"] if c["cluster_id"] == "c1")
    assert c1["source_mix"]["core"] >= 1
    assert c1["bucket_mix"]["b-a"] >= 1
    assert c1["bucket_mix"]["b-b"] >= 1


def test_markdown_includes_caveat_without_bridge_validation_claim() -> None:
    conn = _FakeConn()
    payload = build_cluster_inspection_payload(
        conn,
        corpus_snapshot_version="snap-v1",
        embedding_version="embed-v1",
        cluster_version="cluster-v1",
    )
    md = render_cluster_inspection_markdown(payload).lower()
    assert "not ranking validation" in md
    assert "not bridge validation" in md


def test_run_cluster_inspection_makes_no_ranking_or_bridge_writes(tmp_path: Path, monkeypatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr("pipeline.cluster_inspection.psycopg.connect", lambda *args, **kwargs: conn)
    output = tmp_path / "cluster_inspection.json"
    markdown_output = tmp_path / "cluster_inspection.md"
    run_cluster_inspection(
        corpus_snapshot_version="snap-v1",
        embedding_version="embed-v1",
        cluster_version="cluster-v1",
        output_path=output,
        markdown_output_path=markdown_output,
        database_url="postgresql://example",
    )
    assert output.exists()
    assert markdown_output.exists()
    blob = "\n".join(conn.sql).lower()
    assert "insert into ranking_runs" not in blob
    assert "insert into paper_scores" not in blob
    assert "insert into embeddings" not in blob
    assert "update works set" not in blob
    parsed = json.loads(output.read_text(encoding="utf-8"))
    assert parsed["provenance"]["cluster_version"] == "cluster-v1"
