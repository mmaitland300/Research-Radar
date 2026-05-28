"""Tests for reviewing the Phase 2 isolated audit write pilot from the bundle."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_online_shadow_phase2_write_pilot_review import (
    MLShadowScorerOnlineShadowPhase2WritePilotReviewError,
    build_phase2_write_pilot_review_slice,
    review_ml_shadow_scorer_online_shadow_phase2_write_pilot,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "bundle": "docs/audit/bundles/phase2-v1/bundle.json",
    "bundle_md": "docs/audit/bundles/phase2-v1/bundle.md",
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "prior_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    _restore_post_pilot_unreviewed_bundle(root)
    return root


def _bundle(root: Path) -> Path:
    return root / FIXTURE_RELS["bundle"]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _restore_post_pilot_unreviewed_bundle(root: Path) -> None:
    bundle = _load(_bundle(root))
    bundle["metadata"]["bundle_revision"] = 2
    bundle["review"] = {
        "phase2_write_pilot_reviewed": False,
        "phase2_write_pilot_accepted": None,
    }
    bundle["recommended_next_stage"] = "review_online_shadow_phase2_isolated_audit_write_pilot_v1"
    _write_json(_bundle(root), bundle)


def _shadow_runs_files(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*") if path.is_file()}


def test_happy_path_accepts_passing_bundle_execution(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    result = review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
        bundle_path=_bundle(root),
        repo_root=root,
        update_bundle=False,
        generated_at="2026-05-28T23:30:00Z",
    )

    assert result["phase2_write_pilot_accepted"] is True
    assert result["review"]["review_decision"]["decision"] == "accepted"
    assert all(result["review"]["review_decision"]["checks"].values())
    assert result["recommended_next_stage"] == "begin_production_readiness_authorization_v1"


def test_not_accepted_when_one_review_check_fails(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_bundle(root))
    bundle["execution"]["pilot_runtime_summary"]["status"] = "runtime_exception"
    _write_json(_bundle(root), bundle)

    result = review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
        bundle_path=_bundle(root),
        repo_root=root,
        update_bundle=False,
        generated_at="2026-05-28T23:30:00Z",
    )

    assert result["phase2_write_pilot_accepted"] is False
    assert result["review"]["review_decision"]["decision"] == "not_accepted"
    assert result["review"]["review_decision"]["failed_review_checks"] == ["pilot_runtime_succeeded"]
    assert result["recommended_next_stage"] == "remediate_online_shadow_phase2_write_pilot_v1"


def test_reviewer_and_review_notes_recorded_verbatim(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_bundle(root))
    review_slice = build_phase2_write_pilot_review_slice(
        bundle,
        reviewer="Reviewer Name",
        review_notes="notes with punctuation: keep verbatim",
        generated_at="2026-05-28T23:30:00Z",
    )

    decision = review_slice["review_decision"]
    assert decision["reviewer"] == "Reviewer Name"
    assert decision["review_notes"] == "notes with punctuation: keep verbatim"
    assert decision["reviewed_at"] == "2026-05-28T23:30:00Z"


def test_no_update_bundle_does_not_mutate_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _bundle(root).read_text(encoding="utf-8")

    result = review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
        bundle_path=_bundle(root),
        repo_root=root,
        update_bundle=False,
        generated_at="2026-05-28T23:30:00Z",
    )

    assert result["bundle_updated"] is False
    assert _bundle(root).read_text(encoding="utf-8") == before


def test_update_bundle_writes_revision_3_json_and_markdown(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    result = review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
        bundle_path=_bundle(root),
        repo_root=root,
        update_bundle=True,
        generated_at="2026-05-28T23:30:00Z",
    )
    persisted = _load(_bundle(root))

    assert result["bundle_updated"] is True
    assert persisted["metadata"]["bundle_revision"] == 3
    assert persisted["review"]["phase2_write_pilot_reviewed"] is True
    assert persisted["review"]["phase2_write_pilot_accepted"] is True
    assert persisted["recommended_next_stage"] == "begin_production_readiness_authorization_v1"
    assert "Pilot Review" in _bundle(root).with_name("bundle.md").read_text(encoding="utf-8")


def test_rejects_bundle_that_is_not_post_pilot_unreviewed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_bundle(root))
    bundle["execution"]["phase2_write_pilot_executed"] = False
    _write_json(_bundle(root), bundle)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2WritePilotReviewError, match="phase2_write_pilot_executed"):
        review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
            bundle_path=_bundle(root),
            repo_root=root,
            update_bundle=False,
        )


def test_no_runtime_calls_shadow_runs_writes_or_db_imports(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_files(root)

    review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
        bundle_path=_bundle(root),
        repo_root=root,
        update_bundle=False,
        generated_at="2026-05-28T23:30:00Z",
    )

    assert _shadow_runs_files(root) == before
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_phase2_write_pilot_review.py"
    ).read_text(encoding="utf-8")
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source


def test_cli_smoke_writes_reviewed_bundle_and_no_update_mode_preserves_file(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    no_update_before = _bundle(root).read_text(encoding="utf-8")
    no_update_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase2-write-pilot-review",
        "--bundle",
        str(_bundle(root)),
        "--reviewer",
        "CLI Reviewer",
        "--review-notes",
        "cli notes",
        "--repo-root",
        str(root),
        "--no-update-bundle",
    ]
    no_update = subprocess.run(no_update_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert no_update.stdout.splitlines() == [
        "accepted",
        "True",
        "begin_production_readiness_authorization_v1",
    ]
    assert _bundle(root).read_text(encoding="utf-8") == no_update_before

    update_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase2-write-pilot-review",
        "--bundle",
        str(_bundle(root)),
        "--reviewer",
        "CLI Reviewer",
        "--review-notes",
        "cli notes",
        "--repo-root",
        str(root),
    ]
    update = subprocess.run(update_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    persisted = _load(_bundle(root))

    assert update.stdout.splitlines() == [
        "accepted",
        "True",
        "begin_production_readiness_authorization_v1",
    ]
    assert persisted["metadata"]["bundle_revision"] == 3
    assert persisted["review"]["review_decision"]["reviewer"] == "CLI Reviewer"
    assert persisted["review"]["review_decision"]["review_notes"] == "cli notes"
