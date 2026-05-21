"""Tests for isolated ml-shadow-scorer-v1 audit output artifacts."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_shadow_scorer_v1 import (
    AUDIT_OUTPUT_ARTIFACT_VERSION,
    MLShadowScorerV1Error,
    build_ml_shadow_scorer_v1_audit_output_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _work_set_sha256(work_ids: list[str]) -> str:
    return hashlib.sha256("".join(f"{work_id}\n" for work_id in sorted(work_ids)).encode("utf-8")).hexdigest()


FIXTURE_SHA = _work_set_sha256(["W1", "W2", "W3"])


def _readiness_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_execution_readiness_gates",
            "gates_version": "ml-shadow-scorer-v1-execution-readiness-gates",
            "candidate_pool_work_set_sha256": FIXTURE_SHA,
        },
        "shadow_scorer_execution_readiness_passed": True,
        "shadow_audit_execution_allowed": True,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": "implement_ml_shadow_scorer_v1_audit_output_artifact",
        "overall_outcomes": {
            "shadow_scorer_execution_readiness_passed": True,
            "shadow_audit_execution_allowed": True,
            "shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "recommended_next_stage": "implement_ml_shadow_scorer_v1_audit_output_artifact",
        },
        "required_shadow_output_isolation_contract": {
            "isolated_audit_shadow_outputs_only": True,
            "no_production_ranking_table_or_config_writes": True,
            "reversible_disableable": True,
            "required_fields": [
                "run_id",
                "scorer_version",
                "formula_id",
                "input_hashes",
                "candidate_pool_work_set_sha256",
                "coverage",
            ],
            "audit_only_until_later_artifact_permits_more": True,
        },
        "required_observability_contract": {
            "requirements": [
                "component coverage counts",
                "missing learned probability count",
                "score distribution for final_score",
                "score distribution for audit_embedding_probability_work",
                "score distribution for hybrid shadow score",
                "top-k overlap with heuristic final_score",
                "rank displacement summary",
                "family-level counts",
                "shadow output completeness",
                "error counters if implemented online",
                "latency counters if implemented online",
            ]
        },
        "shadow_and_production_blockers": {
            "missing_ml_shadow_scorer_v1_audit_output_artifact": True,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
    }


def _implementation_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_implementation",
            "implementation_version": "ml-shadow-scorer-v1-implementation",
            "candidate_pool_work_set_sha256": FIXTURE_SHA,
        },
        "implementation_status": {
            "implemented": True,
            "disabled_by_default": True,
            "implementation_matches_spec": True,
            "implementation_matches_validation_replay": True,
            "missing_ml_shadow_scorer_v1_implementation": False,
            "candidate_pool_size": 3,
            "learned_probability_coverage_count": 3,
            "missing_learned_probability_count": 0,
        },
        "audit_replay_summary": {
            "candidate_pool_work_set_sha256": FIXTURE_SHA,
            "replay_tolerance": 1e-12,
        },
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_spec",
            "spec_version": "ml-shadow-scorer-v1-spec",
            "candidate_pool_work_set_sha256": FIXTURE_SHA,
        },
        "spec_ready_for_implementation": True,
        "recommended_next_stage": "implement_ml_shadow_scorer_v1_disabled_by_default",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "scoring_formula": {
            "formula_id": "hybrid_rank_mean_50_50",
            "scoring_formula_literal": "score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
            "components": [
                {"name": "final_score_rank_pct", "source": "rank_pct(final_score)", "weight": 0.5},
                {
                    "name": "audit_embedding_probability_rank_pct",
                    "source": "rank_pct(audit_embedding_probability_work)",
                    "weight": 0.5,
                },
            ],
        },
    }


def _validation_payload() -> dict:
    rows = [
        {
            "canonical_openalex_work_id": "W1",
            "title": "First",
            "year": 2024,
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "final_score": 3.0,
            "audit_embedding_probability_work": 0.1,
            "final_score_rank_pct": 1.0,
            "audit_embedding_probability_rank_pct": 0.0,
            "heuristic_rank": 1,
            "confirmatory_metric_eligible": True,
            "label_any_positive": False,
            "arm_scores": {"hybrid_rank_mean_50_50": 0.5},
        },
        {
            "canonical_openalex_work_id": "W2",
            "title": "Second",
            "year": 2025,
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "final_score": 2.0,
            "audit_embedding_probability_work": 0.3,
            "final_score_rank_pct": 0.5,
            "audit_embedding_probability_rank_pct": 1.0,
            "heuristic_rank": 2,
            "confirmatory_metric_eligible": True,
            "label_any_positive": True,
            "arm_scores": {"hybrid_rank_mean_50_50": 0.75},
        },
        {
            "canonical_openalex_work_id": "W3",
            "title": "Third",
            "year": 2023,
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "final_score": 1.0,
            "audit_embedding_probability_work": 0.2,
            "final_score_rank_pct": 0.0,
            "audit_embedding_probability_rank_pct": 0.5,
            "heuristic_rank": 3,
            "confirmatory_metric_eligible": False,
            "label_any_positive": None,
            "arm_scores": {"hybrid_rank_mean_50_50": 0.25},
        },
    ]
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_on_fresh_surface",
            "validation_version": "ml-hybrid-validation-on-fresh-surface-v1",
            "candidate_pool_work_set_sha256": FIXTURE_SHA,
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
            "embedding_version": "fresh-hybrid-text-embedding-v1",
        },
        "candidate_eval_coverage": {
            "candidate_pool_work_set_sha256": FIXTURE_SHA,
        },
        "candidate_work_scores": rows,
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    readiness: dict | None = None,
    implementation: dict | None = None,
    spec: dict | None = None,
    validation: dict | None = None,
) -> dict[str, Path]:
    return {
        "shadow_scorer_execution_readiness_gates_path": _write_json(
            tmp_path, "readiness.json", readiness or _readiness_payload()
        ),
        "shadow_scorer_implementation_path": _write_json(
            tmp_path, "implementation.json", implementation or _implementation_payload()
        ),
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "hybrid_validation_on_fresh_surface_path": _write_json(
            tmp_path, "validation.json", validation or _validation_payload()
        ),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_v1_audit_output_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def test_happy_path_small_fixture_known_ordering_and_replay_match(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["execution_summary"]["status"] == "succeeded"
    assert payload["execution_summary"]["output_row_count"] == 3
    assert payload["execution_verification"]["output_matches_validation_replay"] is True
    assert [row["canonical_openalex_work_id"] for row in payload["shadow_output_rows"]] == ["W2", "W1", "W3"]
    assert [row["shadow_rank"] for row in payload["shadow_output_rows"]] == [1, 2, 3]


def test_rejects_readiness_not_passed(tmp_path: Path) -> None:
    readiness = _readiness_payload()
    readiness["shadow_scorer_execution_readiness_passed"] = False
    readiness["overall_outcomes"]["shadow_scorer_execution_readiness_passed"] = False

    with pytest.raises(MLShadowScorerV1Error, match="shadow_scorer_execution_readiness_passed"):
        _build(tmp_path, readiness=readiness)


def test_rejects_duplicated_readiness_fields_when_disagree(tmp_path: Path) -> None:
    readiness = _readiness_payload()
    readiness["overall_outcomes"]["shadow_audit_execution_allowed"] = False

    with pytest.raises(MLShadowScorerV1Error, match="disagrees"):
        _build(tmp_path, readiness=readiness)


def test_rejects_when_audit_output_artifact_already_created(tmp_path: Path) -> None:
    readiness = _readiness_payload()
    readiness["shadow_and_production_blockers"]["missing_ml_shadow_scorer_v1_audit_output_artifact"] = False

    with pytest.raises(MLShadowScorerV1Error, match="missing_ml_shadow_scorer_v1_audit_output_artifact"):
        _build(tmp_path, readiness=readiness)


def test_rejects_incomplete_candidate_rows(tmp_path: Path) -> None:
    validation = _validation_payload()
    del validation["candidate_work_scores"][0]["final_score"]

    with pytest.raises(MLShadowScorerV1Error, match="final_score"):
        _build(tmp_path, validation=validation)


def test_rejects_sha_mismatch_across_inputs(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["metadata"]["candidate_pool_work_set_sha256"] = "bad"

    with pytest.raises(MLShadowScorerV1Error, match="candidate_pool_work_set_sha256 mismatch"):
        _build(tmp_path, spec=spec)


def test_output_rows_match_validation_hybrid_arm_within_tolerance(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["execution_verification"]["max_abs_score_delta"] <= 1e-12
    assert payload["execution_verification"]["max_abs_rank_pct_delta"] <= 1e-12
    assert payload["execution_verification"]["mismatched_work_count"] == 0


def test_shadow_and_production_flags_remain_false(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["shadow_execution_enabled"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False


def test_every_output_row_marks_label_not_used_for_scoring(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert all(row["label_any_positive_not_used_for_scoring"] is True for row in payload["shadow_output_rows"])


def test_label_fields_never_affect_score(tmp_path: Path) -> None:
    base = _build(tmp_path)
    validation = _validation_payload()
    validation["candidate_work_scores"][0]["label_any_positive"] = True
    validation["candidate_work_scores"][1]["label_any_positive"] = False
    changed = _build(tmp_path, validation=validation)

    assert [
        row["ml_shadow_scorer_v1_score"] for row in base["shadow_output_rows"]
    ] == [row["ml_shadow_scorer_v1_score"] for row in changed["shadow_output_rows"]]


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "audit-output.json"
    out_md = tmp_path / "audit-output.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-v1-audit-output",
        "--shadow-scorer-execution-readiness-gates",
        str(paths["shadow_scorer_execution_readiness_gates_path"]),
        "--shadow-scorer-implementation",
        str(paths["shadow_scorer_implementation_path"]),
        "--shadow-scorer-spec",
        str(paths["shadow_scorer_spec_path"]),
        "--hybrid-validation-on-fresh-surface",
        str(paths["hybrid_validation_on_fresh_surface_path"]),
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
    assert data["metadata"]["artifact_version"] == AUDIT_OUTPUT_ARTIFACT_VERSION
    assert data["execution_verification"]["output_matches_validation_replay"] is True
    assert "ML Shadow Scorer v1 Audit Output" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_v1.py").read_text(encoding="utf-8").lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("psycopg", "postgres", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8").lower()
    command_index = cli_source.index("ml-shadow-scorer-v1-audit-output")
    next_command_index = cli_source.index("ml-fresh-eval-labeling-plan-hybrid", command_index)
    command_block = cli_source[command_index:next_command_index]
    assert "--database-url" not in command_block
