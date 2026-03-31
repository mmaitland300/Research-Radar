from pipeline.clustering import ClusteringInput
from pipeline.clustering_persistence import ClusterInputSummary
from pipeline.clustering_run import execute_clustering_run


class _FakeConn:
    def __init__(self) -> None:
        self.commit_calls = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def commit(self) -> None:
        self.commit_calls += 1


def test_execute_clustering_run_uses_identity_and_writes_counts(monkeypatch) -> None:
    conn1 = _FakeConn()
    conn2 = _FakeConn()
    calls: list[tuple[str, object]] = []
    connections = iter([conn1, conn2])

    monkeypatch.setattr(
        "pipeline.clustering_run.psycopg.connect",
        lambda *args, **kwargs: next(connections),
    )

    def _list_inputs(conn, *, embedding_version, corpus_snapshot_version):
        return ClusterInputSummary(
            corpus_snapshot_version="source-snapshot-1",
            embedding_version=embedding_version,
            total_input_works=3,
            rows=[
                ClusteringInput(work_id=1, vector=(0.0, 0.0)),
                ClusteringInput(work_id=2, vector=(0.1, 0.0)),
                ClusteringInput(work_id=3, vector=(5.0, 5.0)),
            ],
        )

    monkeypatch.setattr("pipeline.clustering_run.list_clustering_inputs", _list_inputs)
    monkeypatch.setattr(
        "pipeline.clustering_run.insert_clustering_run_started",
        lambda conn, run: calls.append(("started", run)),
    )
    monkeypatch.setattr(
        "pipeline.clustering_run.replace_cluster_assignments",
        lambda conn, cluster_version, assignments: calls.append(
            ("assignments", (cluster_version, list(assignments)))
        ),
    )
    monkeypatch.setattr(
        "pipeline.clustering_run.update_clustering_run_final",
        lambda conn, cluster_version, status, counts, error_message: calls.append(
            ("final", (cluster_version, status, counts, error_message))
        ),
    )

    run = execute_clustering_run(
        database_url="postgresql://example",
        cluster_version="cluster-v0",
        embedding_version="embed-v1",
        cluster_count=2,
        max_iterations=5,
    )

    assert run.cluster_version == "cluster-v0"
    assert run.embedding_version == "embed-v1"
    assert run.corpus_snapshot_version == "source-snapshot-1"
    assert run.status == "succeeded"
    assert run.counts.total_input_works == 3
    assert run.counts.clustered_works == 3
    assert run.counts.cluster_count == 2
    assert conn1.commit_calls == 1
    assert conn2.commit_calls == 1
    assert calls[0][0] == "started"
    assert calls[1][0] == "assignments"
    assert calls[2][0] == "final"
    assigned_cluster_version, assigned_rows = calls[1][1]
    assert assigned_cluster_version == "cluster-v0"
    assert len(assigned_rows) == 3

