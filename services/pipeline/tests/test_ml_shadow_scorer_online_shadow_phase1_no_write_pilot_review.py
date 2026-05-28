"""Tests for ml-shadow-scorer-v1 Phase 1 no-write online shadow pilot review."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review import (
    MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError,
    build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review_payload,
    write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FORMULA_ID,
    RANKING_RUN_ID,
    SCORER_ID,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

RUN_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-run-v1.json"
PLAN_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1.json"
GRANT_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json"
RUNTIME_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build(
    *,
    run_path: Path = RUN_PATH,
    plan_path: Path = PLAN_PATH,
    grant_path: Path = GRANT_PATH,
    runtime_path: Path = RUNTIME_PATH,
    generated_at: str = "2026-05-27T00:00:00Z",
) -> dict:
    return build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review_payload(
        phase1_no_write_pilot_run_path=run_path,
        phase1_no_write_pilot_plan_path=plan_path,
        authorization_grant_path=grant_path,
        online_shadow_runtime_path=runtime_path,
        repo_root=REPO_ROOT,
        generated_at=generated_at,
    )


def test_happy_path_writes_review_acceptance_semantics() -> None:
    payload = _build()

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review"
    assert payload["phase1_no_write_pilot_review_executed"] is True
    assert payload["phase1_no_write_pilot_result_accepted"] is True
    assert payload["review_decision"]["decision"] == "accepted"
    assert payload["phase1_no_write_pilot_executed"] is True
    assert payload["phase1_no_write_pilot_passed"] is True
    assert payload["online_shadow_execution_authorized"] is True
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["runtime_execution_authorized"] is True
    assert payload["shadow_scoring_allowed"] is True
    assert payload["writes_performed"] is False
    assert payload["phase2_writes_authorized"] is False
    assert payload["production_default_allowed"] is False
    assert payload["api_web_changes_allowed"] is False
    assert payload["user_visible_ranking_changed"] is False
    assert payload["missing_production_readiness_authorization"] is True
    assert payload["recommended_next_stage"] == "draft_online_shadow_phase2_isolated_audit_write_mode_plan_v1"
    assert payload["pilot_result_summary"]["runtime_status"] == "succeeded_test_only"
    assert payload["pilot_result_summary"]["runtime_row_count"] == 528
    assert payload["no_write_review"]["shadow_rows_persisted"] is False
    assert payload["no_write_review"]["shadow_rows_omitted_from_artifact"] is True
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_phase1_no_write_pilot_review"] is False
    assert blockers["missing_phase2_write_mode_isolation_proof"] is True
    assert blockers["phase2_writes_authorized"] is False


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda run: run.update({"phase1_no_write_pilot_executed": False}), "phase1_no_write_pilot_executed"),
        (lambda run: run.update({"phase1_no_write_pilot_passed": False}), "phase1_no_write_pilot_passed"),
        (lambda run: run["pass_fail_evaluation"].update({"passed": False}), "pass_fail_evaluation.passed"),
        (lambda run: run["pass_fail_evaluation"].update({"failed_checks": ["x"]}), "failed_checks"),
        (lambda run: run["pilot_runtime_result"].update({"status": "skipped_runtime_disabled"}), "pilot_runtime_result.status"),
        (lambda run: run.update({"disable_drill_passed": False}), "disable_drill_passed"),
        (lambda run: run.update({"writes_performed": True}), "writes_performed"),
        (lambda run: run["pilot_runtime_result"].update({"shadow_rows_persisted": True}), "shadow_rows_persisted"),
        (
            lambda run: run["pilot_runtime_result"].update({"shadow_rows_omitted_from_artifact": False}),
            "shadow_rows_omitted_from_artifact",
        ),
        (lambda run: run["metadata"].update({"formula_id": "other"}), "formula_id"),
        (lambda run: run["metadata"].update({"scorer_id": "other"}), "scorer_id"),
    ],
)
def test_rejects_invalid_run_evidence(tmp_path: Path, mutate, match: str) -> None:
    run = copy.deepcopy(_load(RUN_PATH))
    mutate(run)
    run_path = _write_json(tmp_path, "run.json", run)
    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match=match):
        _build(run_path=run_path)


def test_rejects_nonzero_write_counts(tmp_path: Path) -> None:
    run = copy.deepcopy(_load(RUN_PATH))
    run["no_write_verification"]["write_counts_by_isolated_target"]["ranking_runs"] = 1
    run_path = _write_json(tmp_path, "run.json", run)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="write counts"):
        _build(run_path=run_path)


def test_rejects_missing_observability_key_from_plan_contract(tmp_path: Path) -> None:
    run = copy.deepcopy(_load(RUN_PATH))
    del run["observability"]["latency"]
    run_path = _write_json(tmp_path, "run.json", run)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="latency"):
        _build(run_path=run_path)


def test_rejects_plan_mutated_to_executed(tmp_path: Path) -> None:
    plan = copy.deepcopy(_load(PLAN_PATH))
    plan["phase1_no_write_pilot_executed"] = True
    plan["shadow_and_production_blockers"]["phase1_no_write_pilot_executed"] = True
    plan_path = _write_json(tmp_path, "plan.json", plan)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="phase1_no_write_pilot_executed"):
        _build(plan_path=plan_path)


def test_rejects_grant_no_longer_granted(tmp_path: Path) -> None:
    grant = copy.deepcopy(_load(GRANT_PATH))
    grant["grant_decision"]["decision"] = "revoked"
    grant_path = _write_json(tmp_path, "grant.json", grant)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="grant_decision.decision"):
        _build(grant_path=grant_path)


def test_rejects_review_by_and_expiry_by_exact_string_equality(tmp_path: Path) -> None:
    grant = copy.deepcopy(_load(GRANT_PATH))
    grant["grant_decision"]["review_by"] = "2026-08-27T00:00:00Z"
    grant["grant_decision"]["expiry_date"] = "2026-08-27T00:00:00Z"
    grant_path = _write_json(tmp_path, "grant.json", grant)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="review_by"):
        _build(grant_path=grant_path)


def test_rejects_tampered_input_chain(tmp_path: Path) -> None:
    run = copy.deepcopy(_load(RUN_PATH))
    run["metadata"]["inputs"][0]["sha256"] = "0" * 64
    run_path = _write_json(tmp_path, "run.json", run)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="sha256 mismatch"):
        _build(run_path=run_path)


def test_rejects_runtime_not_disabled_by_default(tmp_path: Path) -> None:
    runtime = copy.deepcopy(_load(RUNTIME_PATH))
    runtime["runtime_disabled_by_default"] = False
    runtime_path = _write_json(tmp_path, "runtime.json", runtime)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="runtime_disabled_by_default"):
        _build(runtime_path=runtime_path)


def test_identity_review_contains_approved_identity() -> None:
    payload = _build()
    identity = payload["identity_review"]["identity_fields"]

    assert identity["ranking_run_id"] == RANKING_RUN_ID
    assert identity["family"] == FAMILY
    assert identity["corpus_snapshot_version"] == CORPUS_SNAPSHOT_VERSION
    assert identity["embedding_version"] == EMBEDDING_VERSION
    assert identity["candidate_pool_work_set_sha256"] == CANDIDATE_POOL_WORK_SET_SHA256
    assert identity["formula_id"] == FORMULA_ID
    assert identity["scorer_id"] == SCORER_ID


def test_non_accepted_path_writes_review_artifact_when_validation_succeeds(tmp_path: Path) -> None:
    run = copy.deepcopy(_load(RUN_PATH))
    run["rollback_summary"]["production_ranking_unchanged"] = False
    run_path = _write_json(tmp_path, "run.json", run)
    out_json = tmp_path / "review.json"
    out_md = tmp_path / "review.md"

    payload = write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review(
        phase1_no_write_pilot_run_path=run_path,
        phase1_no_write_pilot_plan_path=PLAN_PATH,
        authorization_grant_path=GRANT_PATH,
        online_shadow_runtime_path=RUNTIME_PATH,
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=REPO_ROOT,
    )

    assert payload["phase1_no_write_pilot_result_accepted"] is False
    assert payload["review_decision"]["decision"] == "not_accepted"
    assert payload["recommended_next_stage"] == "remediate_online_shadow_phase1_pilot_v1"
    assert out_json.exists()
    assert out_md.exists()


def test_writer_does_not_emit_artifacts_on_validation_error(tmp_path: Path) -> None:
    run = copy.deepcopy(_load(RUN_PATH))
    run["phase1_no_write_pilot_executed"] = False
    run_path = _write_json(tmp_path, "run.json", run)
    out_json = tmp_path / "review.json"
    out_md = tmp_path / "review.md"

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError, match="phase1_no_write_pilot_executed"):
        write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review(
            phase1_no_write_pilot_run_path=run_path,
            phase1_no_write_pilot_plan_path=PLAN_PATH,
            authorization_grant_path=GRANT_PATH,
            online_shadow_runtime_path=RUNTIME_PATH,
            output_path=out_json,
            markdown_output_path=out_md,
            repo_root=REPO_ROOT,
        )

    assert not out_json.exists()
    assert not out_md.exists()


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "review.json"
    out_md = tmp_path / "review.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase1-no-write-pilot-review",
        "--phase1-no-write-pilot-run",
        str(RUN_PATH),
        "--phase1-no-write-pilot-plan",
        str(PLAN_PATH),
        "--authorization-grant",
        str(GRANT_PATH),
        "--online-shadow-runtime",
        str(RUNTIME_PATH),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review"
    assert payload["phase1_no_write_pilot_result_accepted"] is True
    assert payload["phase2_writes_authorized"] is False
    assert payload["recommended_next_stage"] == "draft_online_shadow_phase2_isolated_audit_write_mode_plan_v1"
    assert result.stdout.splitlines() == [
        "True",
        "True",
        "draft_online_shadow_phase2_isolated_audit_write_mode_plan_v1",
    ]
    assert "Online Shadow Phase 1 No-Write Pilot Review" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_no_runtime_rerun_and_cli_has_no_database_url() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
    assert "compute_shadow_score_rows" not in module_source

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase1-no-write-pilot-review"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
