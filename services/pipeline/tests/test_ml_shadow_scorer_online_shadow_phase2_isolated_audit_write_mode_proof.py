"""Tests for ml-shadow-scorer-v1 Phase 2 isolated audit write-mode proof."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof as proof_module
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof import (
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError,
    build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof_payload,
    write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "phase2_plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "learned": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
    "audit": "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
    "phase1_run": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-run-v1.json",
}

COPY_RELS = sorted(
    {
        *FIXTURE_RELS.values(),
        "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-request-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json",
    }
)


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in COPY_RELS:
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build(
    root: Path,
    *,
    phase2_plan_path: Path | None = None,
    grant_path: Path | None = None,
    review_path: Path | None = None,
    learned_path: Path | None = None,
    audit_path: Path | None = None,
    pilot_run_id: str = "proof-test",
    cleanup_after_proof: bool = True,
) -> dict[str, Any]:
    return build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof_payload(
        phase2_write_mode_plan_path=phase2_plan_path or _fixture(root, "phase2_plan"),
        authorization_grant_path=grant_path or _fixture(root, "grant"),
        phase1_no_write_pilot_review_path=review_path or _fixture(root, "review"),
        learned_probability_artifact_path=learned_path or _fixture(root, "learned"),
        second_surface_generalization_audit_path=audit_path or _fixture(root, "audit"),
        phase1_no_write_pilot_run_path=_fixture(root, "phase1_run"),
        pilot_run_id=pilot_run_id,
        repo_root=root,
        generated_at="2026-05-28T19:30:00Z",
        cleanup_after_proof=cleanup_after_proof,
    )


def _write_proof(root: Path, *, pilot_run_id: str, cleanup_after_proof: bool) -> dict[str, Any]:
    return write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof(
        phase2_write_mode_plan_path=_fixture(root, "phase2_plan"),
        authorization_grant_path=_fixture(root, "grant"),
        phase1_no_write_pilot_review_path=_fixture(root, "review"),
        learned_probability_artifact_path=_fixture(root, "learned"),
        second_surface_generalization_audit_path=_fixture(root, "audit"),
        phase1_no_write_pilot_run_path=_fixture(root, "phase1_run"),
        output_path=root / "docs/audit/proof.json",
        markdown_output_path=root / "docs/audit/proof.md",
        pilot_run_id=pilot_run_id,
        repo_root=root,
        cleanup_after_proof=cleanup_after_proof,
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_happy_path_writes_files_and_records_hashes(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _write_proof(root, pilot_run_id="happy-proof", cleanup_after_proof=False)
    proof_json = _load(root / "docs/audit/proof.json")
    run_dir = Path(proof_json["pilot_run_directory"]["resolved_path"])

    assert payload["phase2_write_mode_proof_passed"] is True
    assert proof_json["phase2_write_mode_proof_passed"] is True
    assert proof_json["missing_phase2_write_mode_isolation_proof"] is False
    assert proof_json["isolated_file_writes"]["file_count"] == 4
    assert proof_json["write_count_verification"]["write_counts_by_isolated_target"]["isolated_audit_shadow_artifacts"] == 4
    forbidden = {
        key: value
        for key, value in proof_json["write_count_verification"]["write_counts_by_isolated_target"].items()
        if key != "isolated_audit_shadow_artifacts"
    }
    assert all(value == 0 for value in forbidden.values())
    assert run_dir.exists()
    for record in proof_json["isolated_file_writes"]["files_written"]:
        written = run_dir / record["relative_path"]
        assert written.exists()
        assert record["sha256"] == _sha256(written)
        if record["relative_path"] == "shadow_rows.jsonl":
            assert record["row_count"] == 528


@pytest.mark.parametrize("bad_id", ["", "../x", "x/y", "x\\y", "/abs", "C:\\abs"])
def test_rejects_invalid_pilot_run_id(tmp_path: Path, bad_id: str) -> None:
    root = _copy_fixture_repo(tmp_path)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, match="pilot_run_id"):
        _build(root, pilot_run_id=bad_id)


def test_rejects_resolved_path_not_strictly_under_phase2_root(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, match="direct child"):
        _build(root, pilot_run_id=".")


def test_rejects_plan_not_ready_or_wrong_next_stage(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    plan = copy.deepcopy(_load(_fixture(root, "phase2_plan")))
    plan["recommended_next_stage"] = "wrong_next_stage"
    plan_path = _write_json(root / "docs/audit/plan-not-ready.json", plan)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, match="recommended_next_stage"):
        _build(root, phase2_plan_path=plan_path)


def test_rejects_join_count_not_528(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    learned = copy.deepcopy(_load(_fixture(root, "learned")))
    learned["candidate_work_scores"] = learned["candidate_work_scores"][:-1]
    learned_path = _write_json(_fixture(root, "learned"), learned)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, match="528"):
        _build(root, learned_path=learned_path)


def test_rejects_runtime_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    original_runtime = proof_module.run_ml_shadow_scorer_v1_online_shadow_runtime

    def fail_when_enabled(candidate_rows: list[dict[str, Any]]) -> dict[str, Any]:
        if os.environ.get(proof_module.FEATURE_FLAG) == "true":
            return {
                "status": "runtime_exception",
                "reason": "forced test failure",
                "runtime_feature_flag": proof_module.FEATURE_FLAG,
                "runtime_feature_flag_value": "true",
                "runtime_enabled": True,
                "shadow_rows": [],
                "shadow_row_count": 0,
                "writes_performed": False,
                "write_count": 0,
                "labels_used_for_scoring": False,
                "production_default_changed": False,
                "user_visible_ranking_changed": False,
            }
        return original_runtime(candidate_rows)

    monkeypatch.setattr(proof_module, "run_ml_shadow_scorer_v1_online_shadow_runtime", fail_when_enabled)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, match="pilot runtime"):
        _build(root)


def test_rejects_forbidden_write_count_and_cleans_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)

    def bad_write_counts(*, file_count: int = 0) -> dict[str, int]:
        counts = {target: 0 for target in proof_module.WRITE_COUNT_TARGETS}
        counts["isolated_audit_shadow_artifacts"] = file_count
        counts["paper_scores"] = 1
        return counts

    monkeypatch.setattr(proof_module, "_write_count_template", bad_write_counts)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, match="forbidden_write_counts_zero"):
        _build(root, pilot_run_id="bad-write-counts", cleanup_after_proof=True)
    assert not (root / proof_module.PRIMARY_TARGET_ROOT / "bad-write-counts").exists()
    assert (root / proof_module.PRIMARY_TARGET_ROOT).exists()


def test_cleanup_after_proof_removes_only_pilot_dir(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _build(root, pilot_run_id="cleanup-proof", cleanup_after_proof=True)
    proof_root = root / proof_module.PRIMARY_TARGET_ROOT
    run_dir = proof_root / "cleanup-proof"

    assert payload["rollback_and_cleanup"]["cleanup_completed"] is True
    assert payload["rollback_and_cleanup"]["directory_absent_after_cleanup"] is True
    assert proof_root.exists()
    assert not run_dir.exists()


def test_no_cleanup_leaves_files_for_inspection_with_hashes(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _build(root, pilot_run_id="inspect-proof", cleanup_after_proof=False)
    run_dir = root / proof_module.PRIMARY_TARGET_ROOT / "inspect-proof"

    assert payload["rollback_and_cleanup"]["cleanup_completed"] is False
    assert payload["rollback_and_cleanup"]["directory_absent_after_cleanup"] is False
    assert run_dir.exists()
    assert {record["relative_path"] for record in payload["isolated_file_writes"]["files_written"]} == {
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    }
    for record in payload["isolated_file_writes"]["files_written"]:
        assert record["sha256"] == _sha256(run_dir / record["relative_path"])


def test_environment_variable_restored_after_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    monkeypatch.setenv(proof_module.FEATURE_FLAG, "sentinel")

    payload = _build(root, pilot_run_id="env-proof", cleanup_after_proof=True)

    assert os.environ[proof_module.FEATURE_FLAG] == "sentinel"
    assert payload["disable_drill"]["environment_restored"] is True


def test_cli_smoke_writes_committed_proof_without_requiring_shadow_runs_in_repo(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json"
    out_md = root / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-mode-proof",
        "--phase2-write-mode-plan",
        str(_fixture(root, "phase2_plan")),
        "--authorization-grant",
        str(_fixture(root, "grant")),
        "--phase1-no-write-pilot-review",
        str(_fixture(root, "review")),
        "--learned-probability-artifact",
        str(_fixture(root, "learned")),
        "--second-surface-generalization-audit",
        str(_fixture(root, "audit")),
        "--phase1-no-write-pilot-run",
        str(_fixture(root, "phase1_run")),
        "--pilot-run-id",
        "cli-proof",
        "--repo-root",
        str(root),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = _load(out_json)

    assert payload["metadata"]["pilot_run_id"] == "cli-proof"
    assert payload["phase2_write_mode_proof_passed"] is True
    assert payload["rollback_and_cleanup"]["directory_absent_after_cleanup"] is True
    assert not (root / proof_module.PRIMARY_TARGET_ROOT / "cli-proof").exists()
    assert result.stdout.splitlines() == [
        "True",
        "True",
        "request_phase2_isolated_audit_write_authorization_v1",
    ]
    assert "Online Shadow Phase 2 Isolated Audit Write-Mode Proof" in out_md.read_text(encoding="utf-8")


def test_no_database_url_no_forbidden_imports_and_uses_hash_helper() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "sqlalchemy", "ranking_persistence", "paper_scores_repo"):
        assert forbidden not in import_lines
    assert "recorded_sha256_matches_text_artifact" in module_source

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-mode-proof"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
