"""Tests for isolated shadow write path guards."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.shadow_write_path_guards import (
    PHASE2_PROOF_ROOT,
    ShadowWritePathGuardError,
    assert_forbidden_write_target_counts,
    assert_write_path_allowed,
    resolve_pilot_directory,
    validate_pilot_run_id,
)


@pytest.mark.parametrize(
    "pilot_run_id",
    [
        "rank-83787b91ef-20260528T193000Z",
        "pilot_001",
        "pilot.001",
        "PILOT-001",
    ],
)
def test_validate_pilot_run_id_accepts_safe_ids(pilot_run_id: str) -> None:
    assert validate_pilot_run_id(pilot_run_id) == pilot_run_id


@pytest.mark.parametrize(
    "pilot_run_id",
    [
        "",
        "../pilot",
        "pilot/child",
        "pilot\\child",
        "/absolute",
        "C:\\absolute",
        "pilot..child",
        "pilot child",
    ],
)
def test_validate_pilot_run_id_rejects_traversal_separators_absolute_and_empty_ids(
    pilot_run_id: str,
) -> None:
    with pytest.raises(ShadowWritePathGuardError):
        validate_pilot_run_id(pilot_run_id)


def test_resolve_pilot_directory_is_direct_child_under_phase2_root(tmp_path: Path) -> None:
    pilot_dir = resolve_pilot_directory(tmp_path, "pilot-001")

    assert pilot_dir == (tmp_path / PHASE2_PROOF_ROOT / "pilot-001").resolve()
    assert pilot_dir.parent == (tmp_path / PHASE2_PROOF_ROOT).resolve()


def test_resolve_pilot_directory_rejects_root_alias(tmp_path: Path) -> None:
    with pytest.raises(ShadowWritePathGuardError, match="direct child"):
        resolve_pilot_directory(tmp_path, ".")


def test_assert_write_path_allowed_rejects_phase2_root_and_outside_paths(tmp_path: Path) -> None:
    proof_root = (tmp_path / PHASE2_PROOF_ROOT).resolve()
    allowed = proof_root / "pilot-001" / "manifest.json"

    assert assert_write_path_allowed(allowed, tmp_path) == allowed.resolve()
    with pytest.raises(ShadowWritePathGuardError):
        assert_write_path_allowed(proof_root, tmp_path)
    with pytest.raises(ShadowWritePathGuardError):
        assert_write_path_allowed(tmp_path / "docs/audit/not-shadow-runs/file.json", tmp_path)


def test_assert_forbidden_write_target_counts_allows_only_isolated_artifacts_positive() -> None:
    assert_forbidden_write_target_counts(
        {
            "isolated_audit_shadow_artifacts": 4,
            "isolated_audit_shadow_tables": 0,
            "ranking_runs": 0,
        }
    )

    with pytest.raises(ShadowWritePathGuardError, match="forbidden write targets"):
        assert_forbidden_write_target_counts(
            {
                "isolated_audit_shadow_artifacts": 4,
                "isolated_audit_shadow_tables": 1,
                "ranking_runs": 0,
            }
        )
