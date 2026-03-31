from unittest.mock import MagicMock

from pipeline.clustering import ClusterAssignment
from pipeline.clustering_persistence import list_clustering_inputs, replace_cluster_assignments


def test_list_clustering_inputs_uses_explicit_snapshot_and_embedding_version() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [(1, [0.1, 0.2]), (2, [0.3, 0.4])]
    summary = list_clustering_inputs(
        conn,
        embedding_version="v1-embed",
        corpus_snapshot_version="source-snapshot-xyz",
    )
    assert summary.corpus_snapshot_version == "source-snapshot-xyz"
    assert summary.embedding_version == "v1-embed"
    assert summary.total_input_works == 2
    assert [row.work_id for row in summary.rows] == [1, 2]
    params = conn.execute.call_args[0][1]
    assert params == ("v1-embed", "source-snapshot-xyz")


def test_replace_cluster_assignments_delete_then_insert_idempotency_rule() -> None:
    conn = MagicMock()
    replace_cluster_assignments(
        conn,
        cluster_version="cluster-v0",
        assignments=[
            ClusterAssignment(work_id=1, cluster_id="c000"),
            ClusterAssignment(work_id=2, cluster_id="c001"),
        ],
    )
    calls = conn.execute.call_args_list
    assert "DELETE FROM clusters" in calls[0][0][0]
    assert calls[0][0][1] == ("cluster-v0",)
    assert "INSERT INTO clusters" in calls[1][0][0]
    assert calls[1][0][1] == (1, "c000", "cluster-v0")
    assert calls[2][0][1] == (2, "c001", "cluster-v0")

