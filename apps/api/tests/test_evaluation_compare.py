from unittest.mock import MagicMock
from dataclasses import replace

from fastapi.testclient import TestClient

from app import main
from app.evaluation_repo import (
    EvalCitationProxy,
    EvalComparePayload,
    EvalListArm,
    EvalPaperRow,
    EvalRecencyProxy,
    EvalTopicMixProxy,
    EvalTopicOverlap,
    _arm_stats,
    _pool_cte_sql,
)


client = TestClient(main.app)


def _fake_payload() -> EvalComparePayload:
    paper = EvalPaperRow(
        paper_id="https://openalex.org/W1",
        title="Example",
        year=2022,
        citation_count=3,
        source_slug="tismir",
        topics=["mir"],
        final_score=0.9,
    )
    arm_ranked = EvalListArm(
        arm_label="ranked_family",
        arm_description="ranked",
        ordering_description="final_score DESC",
        items=[paper],
        recency=EvalRecencyProxy(2022.0, 2022, 2022, 1.0),
        citations=EvalCitationProxy(3.0, 3.0, 3, 3),
        topics=EvalTopicMixProxy(1, ["mir"]),
    )
    arm_cit = EvalListArm(
        arm_label="citation_baseline",
        arm_description="cit",
        ordering_description="cites",
        items=[paper],
        recency=EvalRecencyProxy(2022.0, 2022, 2022, 1.0),
        citations=EvalCitationProxy(3.0, 3.0, 3, 3),
        topics=EvalTopicMixProxy(1, ["mir"]),
    )
    arm_date = EvalListArm(
        arm_label="date_baseline",
        arm_description="date",
        ordering_description="date",
        items=[paper],
        recency=EvalRecencyProxy(2022.0, 2022, 2022, 1.0),
        citations=EvalCitationProxy(3.0, 3.0, 3, 3),
        topics=EvalTopicMixProxy(1, ["mir"]),
    )
    return EvalComparePayload(
        ranking_run_id="run-1",
        ranking_version="v0-test",
        corpus_snapshot_version="snap-a",
        embedding_version="none-v0",
        family="emerging",
        pool_definition="test pool",
        pool_size=10,
        low_cite_min_year=None,
        low_cite_max_citations=None,
        candidate_pool_doc_revision=None,
        ranked=arm_ranked,
        citation_baseline=arm_cit,
        date_baseline=arm_date,
        topic_overlap=EvalTopicOverlap(1.0, 1.0, 1.0),
    )


def test_arm_stats_builds_ordering_description() -> None:
    paper = EvalPaperRow(
        paper_id="https://openalex.org/W1",
        title="Example",
        year=2022,
        citation_count=3,
        source_slug="tismir",
        topics=["mir"],
        final_score=0.9,
    )
    arm = _arm_stats(
        [paper],
        arm_label="ranked_family",
        arm_desc="ranked",
        ordering_desc="final_score DESC",
    )
    assert arm.ordering_description == "final_score DESC"
    assert arm.recency.mean_year == 2022.0


def test_pool_sql_undercited_uses_low_cite_contract_filters() -> None:
    sql, params = _pool_cte_sql(
        family="undercited",
        corpus_snapshot_version="snap",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
    )
    assert "w.inclusion_status = 'included'" in sql
    assert "w.corpus_snapshot_version = %s" in sql
    assert "w.is_core_corpus = TRUE" in sql
    assert "w.year >= %s" in sql
    assert "w.citation_count <= %s" in sql
    assert "length(trim(COALESCE(w.title, ''))) > 0" in sql
    assert "length(trim(COALESCE(w.abstract, ''))) > 0" in sql
    assert params == ("snap", 2019, 30)


def test_pool_sql_non_undercited_uses_all_included_snapshot_works_only() -> None:
    sql, params = _pool_cte_sql(
        family="emerging",
        corpus_snapshot_version="snap",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
    )
    assert "w.inclusion_status = 'included'" in sql
    assert "w.corpus_snapshot_version = %s" in sql
    assert "w.is_core_corpus = TRUE" not in sql
    assert "w.year >= %s" not in sql
    assert "w.citation_count <= %s" not in sql
    assert params == ("snap",)


def test_evaluation_compare_smoke(monkeypatch) -> None:
    monkeypatch.setattr(main, "load_evaluation_compare", MagicMock(return_value=_fake_payload()))
    response = client.get("/api/v1/evaluation/compare?family=emerging&limit=5")

    assert response.status_code == 200
    body = response.json()
    assert body["ranking_run_id"] == "run-1"
    assert body["family"] == "emerging"
    assert body["pool_size"] == 10
    assert body["disclaimer"]["headline"]
    assert len(body["disclaimer"]["bullets"]) >= 2
    assert body["ranked"]["arm_label"] == "ranked_family"
    assert body["citation_baseline"]["items"][0]["paper_id"] == "https://openalex.org/W1"
    assert body["topic_overlap"]["jaccard_ranked_vs_citation_baseline"] == 1.0
    assert "topic_overlap_note" in body
    assert "not human relevance judgments" in body["disclaimer"]["headline"].lower()
    assert "same candidate pool" in " ".join(body["disclaimer"]["bullets"]).lower()


def test_evaluation_compare_undercited_pool_contract_fields(monkeypatch) -> None:
    payload = replace(
        _fake_payload(),
        family="undercited",
        pool_definition="Low-cite candidate pool (revision v0): included core works.",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
        candidate_pool_doc_revision="v0",
    )
    monkeypatch.setattr(main, "load_evaluation_compare", MagicMock(return_value=payload))

    response = client.get("/api/v1/evaluation/compare?family=undercited&limit=5")
    assert response.status_code == 200
    body = response.json()
    assert body["family"] == "undercited"
    assert body["low_cite_min_year"] == 2019
    assert body["low_cite_max_citations"] == 30
    assert body["candidate_pool_doc_revision"] == "v0"
    assert "low-cite candidate pool" in body["pool_definition"].lower()


def test_evaluation_compare_emerging_does_not_emit_low_cite_gate_fields(monkeypatch) -> None:
    payload = replace(
        _fake_payload(),
        family="emerging",
        pool_definition="All included works in this corpus snapshot.",
        low_cite_min_year=None,
        low_cite_max_citations=None,
        candidate_pool_doc_revision=None,
    )
    monkeypatch.setattr(main, "load_evaluation_compare", MagicMock(return_value=payload))

    response = client.get("/api/v1/evaluation/compare?family=emerging&limit=5")
    assert response.status_code == 200
    body = response.json()
    assert body["family"] == "emerging"
    assert body["low_cite_min_year"] is None
    assert body["low_cite_max_citations"] is None
    assert body["candidate_pool_doc_revision"] is None


def test_evaluation_compare_not_found(monkeypatch) -> None:
    monkeypatch.setattr(main, "load_evaluation_compare", MagicMock(return_value=None))
    response = client.get("/api/v1/evaluation/compare?family=bridge")

    assert response.status_code == 404


def test_evaluation_compare_invalid_family() -> None:
    response = client.get("/api/v1/evaluation/compare?family=notafamily")

    assert response.status_code == 422


def test_readiness_ok(monkeypatch) -> None:
    class FakeConn:
        def execute(self, *_a, **_k):
            return self

        def fetchone(self):
            return (1,)

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(main.psycopg, "connect", lambda *_a, **_k: FakeConn())
    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["database"] == "connected"


def test_readiness_db_down(monkeypatch) -> None:
    def boom(*_a, **_k):
        raise RuntimeError("no db")

    monkeypatch.setattr(main.psycopg, "connect", boom)
    response = client.get("/readyz")

    assert response.status_code == 503
