"""Tests for second candidate-source expansion plan for shadow generalization."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_shadow_scorer_second_candidate_source_expansion_plan import (
    DISALLOWED_CANDIDATE_SHA,
    DISALLOWED_RANKING_RUN_ID,
    MLShadowScorerSecondCandidateSourceExpansionPlanError,
    PLAN_VERSION,
    build_ml_shadow_scorer_second_candidate_source_expansion_plan_payload,
    markdown_from_ml_shadow_scorer_second_candidate_source_expansion_plan,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _second_surface_payload(
    *,
    status: str = "blocked_no_candidate_source_meets_minimum",
    recommended_next_stage: str = "create_or_expand_second_fresh_candidate_source_for_shadow_generalization_v1",
    selected_second_surface: dict | None = None,
    ready: bool = False,
    best_confirmatory: int = 43,
    best_ranking_run_id: str = "rank-3904fec89d",
    best_sha: str = "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926",
) -> dict:
    sources = [
        {
            "ranking_run_id": best_ranking_run_id,
            "family": "emerging",
            "candidate_pool_work_count": 59,
            "candidate_pool_work_set_sha256": best_sha,
            "confirmatory_metric_eligible_work_count": best_confirmatory,
            "old_217_overlap_count": 15,
            "rank_9f4b2a2084_overlap_count": 16,
            "combined_prior_surface_overlap_count": 16,
            "distinct_from_first_validated_surface": True,
            "disallowed_reasons": [],
        },
        {
            "ranking_run_id": DISALLOWED_RANKING_RUN_ID,
            "family": "emerging",
            "candidate_pool_work_count": 358,
            "candidate_pool_work_set_sha256": DISALLOWED_CANDIDATE_SHA,
            "confirmatory_metric_eligible_work_count": 0,
            "distinct_from_first_validated_surface": False,
            "disallowed_reasons": [
                "ranking_run_id_matches_first_validated_surface",
                "candidate_sha_matches_first_validated_surface",
            ],
        },
    ]
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
        },
        "discovery_summary": {
            "status": status,
            "candidate_sources_considered_count": len(sources),
        },
        "selected_second_surface": selected_second_surface,
        "candidate_sources_considered": sources,
        "readiness_for_generalization_audit": {
            "ready_for_generalization_audit_execution": ready,
            "status": status,
        },
        "recommended_next_stage": recommended_next_stage,
    }


def _audit_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
    }


def _online_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "runtime_implementation_authorized": False,
    }


def _fresh_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "label_policy": {
            "minimum_confirmatory_label_thresholds": {
                "minimum_candidate_work_count": 100,
                "minimum_confirmatory_labeled_work_count": 100,
                "minimum_confirmatory_positive_work_count": 50,
                "minimum_confirmatory_negative_work_count": 20,
                "minimum_distinct_negative_work_count": 20,
                "minimum_confirmatory_label_coverage_rate": 0.60,
            }
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    second_surface: dict | None = None,
    audit_plan: dict | None = None,
    online_policy: dict | None = None,
    fresh_policy: dict | None = None,
) -> dict[str, Path]:
    return {
        "generalization_second_surface_path": _write_json(
            tmp_path, "second-surface.json", second_surface or _second_surface_payload()
        ),
        "generalization_audit_plan_path": _write_json(tmp_path, "audit-plan.json", audit_plan or _audit_plan_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "online-policy.json", online_policy or _online_policy_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "fresh-policy.json", fresh_policy or _fresh_policy_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_second_candidate_source_expansion_plan_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def test_happy_path_from_blocked_second_surface_fixture(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_candidate_source_expansion_plan"
    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["second_candidate_source_expansion_plan_defined"] is True
    assert payload["current_blocker_summary"]["sources_considered_count"] == 2
    assert payload["current_blocker_summary"]["best_distinct_ranking_run_id"] == "rank-3904fec89d"
    assert payload["current_blocker_summary"]["best_confirmatory_eligible_work_count"] == 43
    assert payload["current_blocker_summary"]["candidate_gap"] == 57
    assert payload["recommended_next_stage"] == (
        "implement_or_run_second_fresh_candidate_source_build_for_shadow_generalization_v1"
    )


def test_rejects_if_status_already_ready(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondCandidateSourceExpansionPlanError, match="blocked"):
        _build(
            tmp_path,
            second_surface=_second_surface_payload(
                status="selected_ready_for_generalization_audit",
                selected_second_surface={"ranking_run_id": "rank-ready"},
                ready=True,
            ),
        )


def test_rejects_wrong_recommended_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondCandidateSourceExpansionPlanError, match="recommended_next_stage"):
        _build(tmp_path, second_surface=_second_surface_payload(recommended_next_stage="audit_ml_shadow_scorer_v1"))


def test_rejects_best_confirmatory_at_or_above_minimum(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondCandidateSourceExpansionPlanError, match="already meets"):
        _build(tmp_path, second_surface=_second_surface_payload(best_confirmatory=100))


def test_rejects_best_source_reusing_first_surface_rank_or_sha(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondCandidateSourceExpansionPlanError, match="rank-9f4b2a2084"):
        _build(tmp_path, second_surface=_second_surface_payload(best_ranking_run_id=DISALLOWED_RANKING_RUN_ID))

    with pytest.raises(MLShadowScorerSecondCandidateSourceExpansionPlanError, match="rank-9f4b2a2084"):
        _build(tmp_path, second_surface=_second_surface_payload(best_sha=DISALLOWED_CANDIDATE_SHA))


def test_requires_threshold_lowering_forbidden_and_blockers_updated(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    forbidden = "\n".join(payload["forbidden_expansion_strategies"]).lower()
    blockers = payload["shadow_and_production_blockers"]

    assert "lowering thresholds" in forbidden
    assert "rank-3904fec89d" in forbidden
    assert payload["runtime_implementation_authorized"] is False
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["production_default_allowed"] is False
    assert blockers["missing_second_fresh_candidate_source_expansion_plan_v1"] is False
    assert blockers["missing_second_fresh_candidate_source"] is True


def test_markdown_contains_current_blocker_and_not_runtime(tmp_path: Path) -> None:
    markdown = markdown_from_ml_shadow_scorer_second_candidate_source_expansion_plan(_build(tmp_path))

    assert "Best confirmatory-eligible works: 43 / 100" in markdown
    assert "Candidate gap: 57" in markdown
    assert "not ML retuning" in markdown
    assert "runtime" in markdown.lower()


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "plan.json"
    out_md = tmp_path / "plan.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-candidate-source-expansion-plan",
        "--generalization-second-surface",
        str(paths["generalization_second_surface_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--online-shadow-policy",
        str(paths["online_shadow_policy_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["current_blocker_summary"]["candidate_gap"] == 57
    assert "Allowed Strategies" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_candidate_source_expansion_plan.py"
    ).read_text(encoding="utf-8").lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("psycopg", "postgres", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = read_cli_parser_source(PACKAGE_ROOT)
    start = cli_source.index('"ml-shadow-scorer-second-candidate-source-expansion-plan"')
    end = cli_source.index("ml_fresh_product_candidate_ranking_source_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
