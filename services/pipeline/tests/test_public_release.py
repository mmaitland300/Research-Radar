from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import pipeline.public_release as public_release
from pipeline.public_release import PublicReleasePromotionError, promote_public_release
from pipeline.public_release_persistence import (
    PublicReleasePromotionRow,
    RankingRunForPromotion,
    ScoreCoverage,
)


SNAPSHOT = "snapshot-public-v1"
EMBEDDING = "embedding-public-v1"
RUN_ID = "ranking-public-v1"
NOW = datetime(2026, 8, 23, tzinfo=UTC)


def _valid_config(*, clustering_artifact=None) -> dict[str, object]:
    return {
        "families_written": ["emerging", "bridge", "undercited"],
        "selection_scope": {
            "type": "included_works",
            "corpus_snapshot_version": SNAPSHOT,
        },
        "clustering_artifact": clustering_artifact,
    }


def _valid_counts() -> dict[str, object]:
    return {
        "total_candidate_works": 5,
        "total_rows_written": 12,
        "rows_by_family": {"emerging": 5, "bridge": 5, "undercited": 2},
        "rows_null_semantic": 0,
        "rows_null_bridge": 0,
    }


def _valid_run(**overrides: object) -> RankingRunForPromotion:
    values: dict[str, object] = {
        "ranking_run_id": RUN_ID,
        "ranking_version": "ranking-v1",
        "corpus_snapshot_version": SNAPSHOT,
        "embedding_version": EMBEDDING,
        "status": "succeeded",
        "finished_at": NOW,
        "config_json": _valid_config(),
        "counts_json": _valid_counts(),
        "error_message": None,
    }
    values.update(overrides)
    return RankingRunForPromotion(**values)  # type: ignore[arg-type]


def _promotion(
    *, ranking_run_id: str = RUN_ID, promotion_id: int = 17
) -> PublicReleasePromotionRow:
    return PublicReleasePromotionRow(
        promotion_id=promotion_id,
        ranking_run_id=ranking_run_id,
        promoted_at=NOW,
        promoted_by="pipeline-cli",
        note=None,
    )


def _install_valid_gate(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run: RankingRunForPromotion | None = None,
    active: PublicReleasePromotionRow | None = None,
    membership_count: int = 5,
    missing_embeddings: int = 0,
    coverage: ScoreCoverage | None = None,
) -> SimpleNamespace:
    conn = MagicMock(name="connection")
    @contextmanager
    def transaction_context(_database_url: str):
        yield conn

    transaction = MagicMock(side_effect=transaction_context)
    fetch_run = MagicMock(return_value=run or _valid_run())
    count_memberships = MagicMock(return_value=membership_count)
    count_embeddings = MagicMock(return_value=missing_embeddings)
    fetch_coverage = MagicMock(
        return_value=coverage or ScoreCoverage(5, 5, 2, 12, 0)
    )
    fetch_active = MagicMock(return_value=active)
    appended = _promotion(promotion_id=18)
    append = MagicMock(return_value=appended)
    require_cluster = MagicMock()
    missing_cluster = MagicMock(return_value=0)
    outside_cluster = MagicMock(return_value=0)

    monkeypatch.setattr(public_release, "serialized_public_release_transaction", transaction)
    monkeypatch.setattr(public_release, "fetch_ranking_run_for_promotion", fetch_run)
    monkeypatch.setattr(public_release, "count_included_memberships", count_memberships)
    monkeypatch.setattr(
        public_release, "count_missing_embedding_candidates", count_embeddings
    )
    monkeypatch.setattr(public_release, "fetch_score_coverage", fetch_coverage)
    monkeypatch.setattr(public_release, "fetch_active_public_release_promotion", fetch_active)
    monkeypatch.setattr(public_release, "append_public_release_promotion", append)
    monkeypatch.setattr(public_release, "require_successful_clustering_run", require_cluster)
    monkeypatch.setattr(
        public_release, "count_included_missing_cluster_assignment", missing_cluster
    )
    monkeypatch.setattr(
        public_release, "count_cluster_assignments_outside_membership", outside_cluster
    )
    return SimpleNamespace(
        conn=conn,
        transaction=transaction,
        fetch_run=fetch_run,
        count_memberships=count_memberships,
        count_embeddings=count_embeddings,
        fetch_coverage=fetch_coverage,
        fetch_active=fetch_active,
        append=append,
        require_cluster=require_cluster,
        missing_cluster=missing_cluster,
        outside_cluster=outside_cluster,
        appended=appended,
    )


def test_promotes_valid_exact_run_under_transaction_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _install_valid_gate(monkeypatch)

    result = promote_public_release(
        ranking_run_id=f"  {RUN_ID}  ", database_url="postgresql://test/db"
    )

    assert result.status == "promoted"
    assert result.changed is True
    assert result.promotion_id == 18
    assert result.membership_count == 5
    assert result.rows_by_family == {"emerging": 5, "bridge": 5, "undercited": 2}
    gate.transaction.assert_called_once_with("postgresql://test/db")
    gate.fetch_run.assert_called_once_with(gate.conn, ranking_run_id=RUN_ID)
    gate.append.assert_called_once_with(
        gate.conn,
        ranking_run_id=RUN_ID,
        promoted_by="pipeline-cli",
        note=None,
    )


def test_dry_run_performs_full_validation_without_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _install_valid_gate(monkeypatch)

    result = promote_public_release(ranking_run_id=RUN_ID, dry_run=True)

    assert result.status == "validated"
    assert result.changed is False
    assert result.dry_run is True
    assert result.promotion_id is None
    gate.fetch_coverage.assert_called_once()
    gate.append.assert_not_called()


def test_already_active_run_is_revalidated_and_does_not_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = _promotion()
    gate = _install_valid_gate(monkeypatch, active=active)

    result = promote_public_release(ranking_run_id=RUN_ID)

    assert result.status == "already-active"
    assert result.changed is False
    assert result.promotion_id == active.promotion_id
    gate.count_memberships.assert_called_once()
    gate.count_embeddings.assert_called_once()
    gate.fetch_coverage.assert_called_once()
    gate.append.assert_not_called()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"status": "running"}, "is not succeeded"),
        ({"finished_at": None}, "no finished_at"),
        ({"error_message": "failed materialization"}, "recorded error"),
        ({"embedding_version": "none-v0"}, "non-placeholder embedding_version"),
    ],
)
def test_rejects_nonserveable_run_state_before_append(
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, object],
    message: str,
) -> None:
    gate = _install_valid_gate(monkeypatch, run=_valid_run(**overrides))

    with pytest.raises(PublicReleasePromotionError, match=message):
        promote_public_release(ranking_run_id=RUN_ID)

    gate.append.assert_not_called()


def test_rejects_empty_membership_or_embedding_gaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _install_valid_gate(monkeypatch, membership_count=0)
    with pytest.raises(PublicReleasePromotionError, match="no included memberships"):
        promote_public_release(ranking_run_id=RUN_ID)
    gate.append.assert_not_called()

    gate = _install_valid_gate(monkeypatch, missing_embeddings=1)
    with pytest.raises(PublicReleasePromotionError, match="missing 1 embeddings"):
        promote_public_release(ranking_run_id=RUN_ID)
    gate.append.assert_not_called()


@pytest.mark.parametrize(
    ("coverage", "message"),
    [
        (ScoreCoverage(4, 5, 2, 11, 0), "emerging score coverage is incomplete"),
        (ScoreCoverage(5, 4, 2, 11, 0), "bridge score coverage is incomplete"),
        (ScoreCoverage(5, 5, 0, 10, 0), "undercited score coverage must be a nonempty"),
        (ScoreCoverage(5, 5, 2, 13, 1), "score rows outside snapshot membership"),
    ],
)
def test_rejects_incomplete_or_out_of_membership_scores(
    monkeypatch: pytest.MonkeyPatch,
    coverage: ScoreCoverage,
    message: str,
) -> None:
    gate = _install_valid_gate(monkeypatch, coverage=coverage)

    with pytest.raises(PublicReleasePromotionError, match=message):
        promote_public_release(ranking_run_id=RUN_ID)

    gate.append.assert_not_called()


def test_rejects_stale_config_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _valid_config()
    config["selection_scope"] = {
        "type": "included_works",
        "corpus_snapshot_version": "different-snapshot",
    }
    gate = _install_valid_gate(monkeypatch, run=_valid_run(config_json=config))
    with pytest.raises(PublicReleasePromotionError, match="does not match the ranking run"):
        promote_public_release(ranking_run_id=RUN_ID)
    gate.append.assert_not_called()

    counts = _valid_counts()
    counts["total_rows_written"] = 11
    gate = _install_valid_gate(monkeypatch, run=_valid_run(counts_json=counts))
    with pytest.raises(PublicReleasePromotionError, match="total_rows_written"):
        promote_public_release(ranking_run_id=RUN_ID)
    gate.append.assert_not_called()


def test_validates_optional_clustering_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cluster_artifact = {
        "cluster_version": "clusters-v1",
        "corpus_snapshot_version": SNAPSHOT,
        "embedding_version": EMBEDDING,
    }
    run = _valid_run(config_json=_valid_config(clustering_artifact=cluster_artifact))
    gate = _install_valid_gate(monkeypatch, run=run)

    result = promote_public_release(ranking_run_id=RUN_ID, dry_run=True)

    assert result.cluster_version == "clusters-v1"
    gate.require_cluster.assert_called_once_with(
        gate.conn,
        cluster_version="clusters-v1",
        corpus_snapshot_version=SNAPSHOT,
        embedding_version=EMBEDDING,
    )
    gate.missing_cluster.assert_called_once()
    gate.outside_cluster.assert_called_once()


def test_rejects_clustering_assignment_gaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cluster_artifact = {
        "cluster_version": "clusters-v1",
        "corpus_snapshot_version": SNAPSHOT,
        "embedding_version": EMBEDDING,
    }
    run = _valid_run(config_json=_valid_config(clustering_artifact=cluster_artifact))
    gate = _install_valid_gate(monkeypatch, run=run)
    gate.missing_cluster.return_value = 1

    with pytest.raises(PublicReleasePromotionError, match="missing 1 snapshot assignments"):
        promote_public_release(ranking_run_id=RUN_ID)

    gate.append.assert_not_called()


def test_blank_or_unknown_exact_run_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(PublicReleasePromotionError, match="must not be blank"):
        promote_public_release(ranking_run_id="   ")

    gate = _install_valid_gate(monkeypatch)
    gate.fetch_run.return_value = None
    with pytest.raises(PublicReleasePromotionError, match="ranking run not found"):
        promote_public_release(ranking_run_id="missing-run")
    gate.append.assert_not_called()
