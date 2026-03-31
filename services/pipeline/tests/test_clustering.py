from pipeline.clustering import ClusteringInput, cluster_inputs_kmeans


def test_cluster_inputs_kmeans_assigns_all_inputs_deterministically() -> None:
    inputs = [
        ClusteringInput(work_id=10, vector=(0.0, 0.0)),
        ClusteringInput(work_id=11, vector=(0.1, 0.0)),
        ClusteringInput(work_id=20, vector=(10.0, 10.0)),
        ClusteringInput(work_id=21, vector=(10.2, 10.1)),
    ]

    first = cluster_inputs_kmeans(inputs, cluster_count=2, max_iterations=10)
    second = cluster_inputs_kmeans(inputs, cluster_count=2, max_iterations=10)

    assert [(a.work_id, a.cluster_id) for a in first] == [
        (10, "c000"),
        (11, "c000"),
        (20, "c001"),
        (21, "c001"),
    ]
    assert [(a.work_id, a.cluster_id) for a in first] == [
        (a.work_id, a.cluster_id) for a in second
    ]

