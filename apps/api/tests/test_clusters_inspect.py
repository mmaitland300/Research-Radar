from fastapi.testclient import TestClient

from app.clusters_repo import ClusterGroupRow, ClusterInspectionPayload, ClusterSamplePaper
from app.main import app


def test_get_cluster_inspection_smoke(monkeypatch) -> None:
    payload = ClusterInspectionPayload(
        cluster_version="cluster-v0",
        embedding_version="embed-v1",
        corpus_snapshot_version="source-snapshot-1",
        algorithm="kmeans-l2-v0",
        status="succeeded",
        config_json={
            "clustering_metric": "squared_l2_euclidean",
            "note": "kmeans uses L2; similar papers use cosine.",
        },
        groups=[
            ClusterGroupRow(
                cluster_id="c000",
                work_count=2,
                sample_papers=[
                    ClusterSamplePaper(paper_id="https://openalex.org/W1", title="Alpha"),
                    ClusterSamplePaper(paper_id="https://openalex.org/W2", title="Beta"),
                ],
            )
        ],
    )
    monkeypatch.setattr("app.main.load_cluster_inspection", lambda **kwargs: payload)
    client = TestClient(app)
    response = client.get("/api/v1/clusters/cluster-v0/inspect?sample_per_cluster=3")
    assert response.status_code == 200
    body = response.json()
    assert body["cluster_version"] == "cluster-v0"
    assert body["embedding_version"] == "embed-v1"
    assert body["clustering_metric"] == "squared_l2_euclidean"
    assert body["groups"][0]["cluster_id"] == "c000"
    assert body["groups"][0]["work_count"] == 2
    assert body["groups"][0]["sample_papers"][0]["title"] == "Alpha"


def test_get_cluster_inspection_404(monkeypatch) -> None:
    monkeypatch.setattr("app.main.load_cluster_inspection", lambda **kwargs: None)
    client = TestClient(app)
    response = client.get("/api/v1/clusters/missing/inspect")
    assert response.status_code == 404


def test_get_cluster_inspection_503(monkeypatch) -> None:
    def _boom(**kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr("app.main.load_cluster_inspection", _boom)
    client = TestClient(app)
    response = client.get("/api/v1/clusters/x/inspect")
    assert response.status_code == 503
