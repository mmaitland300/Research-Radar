"""Tests for disabled-by-default ml-shadow-scorer-v1 implementation audit."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_shadow_scorer_v1 as shadow_module
from pipeline.ml_shadow_scorer_v1 import (
    IMPLEMENTATION_VERSION,
    MLShadowScorerV1Error,
    build_ml_shadow_scorer_v1_audit_payload,
    compute_rank_percentiles,
    compute_shadow_score_rows,
    validate_shadow_scorer_inputs,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


VALIDATION_WORK_IDS = ["W1", "W2", "W3"]
FRESH_SHA = shadow_module._work_set_sha256(VALIDATION_WORK_IDS)


@pytest.fixture(autouse=True)
def _small_replay_constants(monkeypatch: pytest.MonkeyPatch) -> None:
    original_validate = shadow_module._validate_validation

    def validate_small_fixture(payload: dict, *, expected_count: int | None = len(VALIDATION_WORK_IDS)):
        return original_validate(payload, expected_count=expected_count)

    monkeypatch.setattr(shadow_module, "EXPECTED_REPLAY_ROW_COUNT", len(VALIDATION_WORK_IDS))
    monkeypatch.setattr(shadow_module, "EXPECTED_CANDIDATE_POOL_SHA", FRESH_SHA)
    monkeypatch.setattr(shadow_module, "_validate_validation", validate_small_fixture)


def _validation_rows() -> list[dict]:
    return [
        {
            "canonical_openalex_work_id": "W1",
            "title": "Fixture one",
            "final_score": 3.0,
            "audit_embedding_probability_work": 1.0,
            "heuristic_rank": 1,
            "final_score_rank_pct": 1.0,
            "audit_embedding_probability_rank_pct": 0.0,
            "arm_scores": {"hybrid_rank_mean_50_50": 0.5},
        },
        {
            "canonical_openalex_work_id": "W2",
            "title": "Fixture two",
            "final_score": 2.0,
            "audit_embedding_probability_work": 3.0,
            "heuristic_rank": 2,
            "final_score_rank_pct": 0.5,
            "audit_embedding_probability_rank_pct": 1.0,
            "arm_scores": {"hybrid_rank_mean_50_50": 0.75},
        },
        {
            "canonical_openalex_work_id": "W3",
            "title": "Fixture three",
            "final_score": 1.0,
            "audit_embedding_probability_work": 2.0,
            "heuristic_rank": 3,
            "final_score_rank_pct": 0.0,
            "audit_embedding_probability_rank_pct": 0.5,
            "arm_scores": {"hybrid_rank_mean_50_50": 0.25},
        },
    ]


def _validation_payload() -> dict:
    scope = {
        "ranking_run_id": "rank-9f4b2a2084",
        "family": "emerging",
        "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        "embedding_version": "fresh-hybrid-text-embedding-v1",
    }
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_on_fresh_surface",
            "validation_version": "ml-hybrid-validation-on-fresh-surface-v1",
            "candidate_pool_work_set_sha256": FRESH_SHA,
            **scope,
        },
        "validation_scope": scope,
        "candidate_work_scores": _validation_rows(),
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_spec",
            "spec_version": "ml-shadow-scorer-v1-spec",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "spec_ready_for_implementation": True,
        "recommended_next_stage": "implement_ml_shadow_scorer_v1_disabled_by_default",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "cross_artifact_provenance_checks": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
            "embedding_version": "fresh-hybrid-text-embedding-v1",
        },
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


def _surface_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
        },
        "candidate_source": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        },
        "candidate_pool": {
            "candidate_work_count": 358,
            "candidate_work_set_sha256": FRESH_SHA,
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    spec: dict | None = None,
    validation: dict | None = None,
    surface: dict | None = None,
) -> dict[str, Path]:
    return {
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "hybrid_validation_on_fresh_surface_path": _write_json(
            tmp_path, "validation.json", validation or _validation_payload()
        ),
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_v1_audit_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def test_rank_percentile_happy_path() -> None:
    assert compute_rank_percentiles([10.0, 5.0, 1.0]) == [1.0, 0.5, 0.0]


def test_rank_percentile_ties_use_average_rank() -> None:
    assert compute_rank_percentiles([10.0, 10.0, 5.0]) == [0.75, 0.75, 0.0]


def test_rank_percentile_singleton_returns_one() -> None:
    assert compute_rank_percentiles([42.0]) == [1.0]


def test_score_formula_exactly_50_50_rank_fusion() -> None:
    rows = [
        {"canonical_openalex_work_id": "W1", "final_score": 3.0, "audit_embedding_probability_work": 1.0},
        {"canonical_openalex_work_id": "W2", "final_score": 2.0, "audit_embedding_probability_work": 3.0},
        {"canonical_openalex_work_id": "W3", "final_score": 1.0, "audit_embedding_probability_work": 2.0},
    ]

    scored = {row["canonical_openalex_work_id"]: row for row in compute_shadow_score_rows(rows)}

    assert scored["W1"]["ml_shadow_scorer_v1_score"] == 0.5
    assert scored["W2"]["ml_shadow_scorer_v1_score"] == 0.75
    assert scored["W3"]["ml_shadow_scorer_v1_score"] == 0.25


def test_rejects_missing_final_score() -> None:
    with pytest.raises(MLShadowScorerV1Error, match="final_score"):
        validate_shadow_scorer_inputs(
            [{"canonical_openalex_work_id": "W1", "audit_embedding_probability_work": 0.2}]
        )


def test_rejects_missing_audit_embedding_probability_work() -> None:
    with pytest.raises(MLShadowScorerV1Error, match="audit_embedding_probability_work"):
        validate_shadow_scorer_inputs([{"canonical_openalex_work_id": "W1", "final_score": 0.1}])


def test_ignores_label_fields_when_present() -> None:
    base = [
        {"canonical_openalex_work_id": "W1", "final_score": 3.0, "audit_embedding_probability_work": 1.0},
        {"canonical_openalex_work_id": "W2", "final_score": 2.0, "audit_embedding_probability_work": 3.0},
    ]
    labeled = copy.deepcopy(base)
    labeled[0]["label_any_positive"] = False
    labeled[0]["good_or_acceptable"] = False
    labeled[1]["label_any_positive"] = True
    labeled[1]["good_or_acceptable"] = True

    assert compute_shadow_score_rows(base) == compute_shadow_score_rows(labeled)


def test_rejects_spec_if_shadow_or_prod_flags_are_true(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["shadow_scoring_allowed"] = True

    with pytest.raises(MLShadowScorerV1Error, match="shadow_scoring_allowed"):
        _build(tmp_path, spec=spec)


def test_rejects_spec_formula_mismatch(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["scoring_formula"]["components"][0]["weight"] = 0.75

    with pytest.raises(MLShadowScorerV1Error, match="weights"):
        _build(tmp_path, spec=spec)


def test_cross_artifact_provenance_mismatch_fails(tmp_path: Path) -> None:
    validation = _validation_payload()
    validation["metadata"]["family"] = "undercited"
    validation["validation_scope"]["family"] = "undercited"

    with pytest.raises(MLShadowScorerV1Error, match="cross-artifact provenance"):
        _build(tmp_path, validation=validation)


def test_audit_replay_fixture_matches_validation_arm_score_within_tolerance(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["implementation_status"]["implementation_matches_spec"] is True
    assert payload["implementation_status"]["implementation_matches_validation_replay"] is True
    assert payload["audit_replay_summary"]["mismatched_work_count"] == 0
    assert payload["audit_replay_summary"]["max_abs_score_delta"] <= 1e-12
    assert payload["audit_replay_summary"]["max_abs_rank_pct_delta"] <= 1e-12


def test_mismatch_in_validation_arm_score_marks_replay_false(tmp_path: Path) -> None:
    validation = _validation_payload()
    validation["candidate_work_scores"][0]["arm_scores"]["hybrid_rank_mean_50_50"] += 0.01

    payload = _build(tmp_path, validation=validation)

    assert payload["implementation_status"]["implementation_matches_spec"] is False
    assert payload["implementation_status"]["implementation_matches_validation_replay"] is False
    assert payload["audit_replay_summary"]["mismatched_work_count"] == 1


def test_output_keeps_shadow_and_production_blocked(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["shadow_execution_enabled"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False
    assert payload["shadow_and_production_blockers"]["missing_ml_shadow_scorer_v1_implementation"] is False


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "implementation.json"
    out_md = tmp_path / "implementation.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-v1-audit",
        "--shadow-scorer-spec",
        str(paths["shadow_scorer_spec_path"]),
        "--hybrid-validation-on-fresh-surface",
        str(paths["hybrid_validation_on_fresh_surface_path"]),
        "--fresh-eval-surface",
        str(paths["fresh_eval_surface_path"]),
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
    assert data["metadata"]["implementation_version"] == IMPLEMENTATION_VERSION
    assert data["implementation_status"]["implementation_matches_spec"] is True
    assert "ML Shadow Scorer v1 Implementation Audit" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_v1.py").read_text(encoding="utf-8").lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    assert "psycopg" not in import_lines
    assert "sklearn" not in import_lines
    assert "openai" not in import_lines
    assert "openalex" not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-v1-audit"')
    end = cli_source.index("ml_fresh_eval_labeling_plan_hybrid_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
