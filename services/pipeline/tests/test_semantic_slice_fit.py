from pipeline.clustering import ClusteringInput
from pipeline.semantic_slice_fit import compute_semantic_slice_fit_by_work


def test_compute_semantic_slice_fit_identical_vectors_high_score() -> None:
    v = (1.0, 0.0, 0.0)
    rows = [
        ClusteringInput(work_id=1, vector=v),
        ClusteringInput(work_id=2, vector=v),
    ]
    out = compute_semantic_slice_fit_by_work(rows)
    assert out[1] == out[2]
    assert 0.99 <= out[1] <= 1.0


def test_compute_semantic_slice_fit_opposite_vectors_mid_score() -> None:
    rows = [
        ClusteringInput(work_id=1, vector=(1.0, 0.0, 0.0)),
        ClusteringInput(work_id=2, vector=(-1.0, 0.0, 0.0)),
    ]
    out = compute_semantic_slice_fit_by_work(rows)
    assert abs(out[1] - 0.5) < 1e-9
    assert abs(out[2] - 0.5) < 1e-9
