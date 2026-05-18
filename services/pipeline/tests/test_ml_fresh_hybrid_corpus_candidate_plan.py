"""Tests for fresh hybrid corpus candidate plan v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_fresh_hybrid_corpus_candidate_plan as plan_mod
from pipeline.ml_fresh_hybrid_corpus_candidate_plan import (
    MLFreshHybridCorpusCandidatePlanError,
    PLAN_VERSION,
    _work_set_sha256,
    build_ml_fresh_hybrid_corpus_candidate_plan_payload,
    markdown_from_ml_fresh_hybrid_corpus_candidate_plan,
)
from pipeline.ml_label_dataset import sha256_file


def _candidate(work_id: str, *, bucket_id: str = "ismir_proceedings_or_mir_conference") -> dict:
    return {
        "openalex_id": f"https://openalex.org/{work_id}",
        "title": f"Candidate {work_id}",
        "year": 2026,
        "citation_count": 7,
        "source_display_name": "Fixture Venue",
        "bucket_id": bucket_id,
        "inclusion_reason": "bucket_allow_signal",
        "matched_terms": ["music"],
    }


def _corpus_plan(
    *,
    new_count: int = 105,
    old_count: int = 3,
    underpowered_count: int = 2,
    include_negative_bucket: bool = True,
) -> dict:
    rows: list[dict] = []
    rows.extend(_candidate(f"W90{i:02d}") for i in range(1, old_count + 1))
    rows.extend(_candidate(f"W2{i:02d}") for i in range(0, underpowered_count))
    for idx in range(1, new_count + 1):
        bucket = "audio_ml_signal_processing" if include_negative_bucket and idx <= 10 else "music_recommender_systems"
        rows.append(_candidate(f"W{idx}", bucket_id=bucket))
    rows.append(_candidate("W1", bucket_id="music_recommender_systems"))
    return {
        "contact_mode": "cli",
        "contact_provided": True,
        "auth_mode": "no_key",
        "api_key_provided": False,
        "selected_total": len(rows),
        "selected_candidates": rows,
        "dedup_statistics": {"unique_openalex_ids_kept": len(rows) - 1},
        "bucket_summaries": [],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    source_build: dict | None = None,
    expansion_plan: dict | None = None,
    policy: dict | None = None,
) -> dict[str, Path]:
    old_ids = [f"W90{i:02d}" for i in range(1, 6)]
    old_sha = _work_set_sha256(old_ids)
    monkeypatch.setattr(plan_mod, "OLD_EVAL_WORK_SET_SHA256", old_sha)
    scoring_path = _write_json(
        tmp_path,
        "old-scoring.json",
        {"candidate_pool_rows": [{"canonical_openalex_work_id": work_id} for work_id in old_ids]},
    )
    assignment_path = _write_json(
        tmp_path,
        "assignment.json",
        {"work_assignments": [{"canonical_openalex_work_id": work_id, "assignment": "eval"} for work_id in old_ids]},
    )
    policy_payload = policy or {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": old_sha,
            "inputs": [
                {"name": "production_candidate_scoring", "path": scoring_path.name, "sha256": sha256_file(scoring_path)},
                {"name": "holdout_assignment", "path": assignment_path.name, "sha256": sha256_file(assignment_path)},
            ],
        },
        "label_policy": {
            "minimum_confirmatory_label_thresholds": {
                "minimum_candidate_work_count": 100,
                "minimum_confirmatory_labeled_work_count": 100,
                "minimum_confirmatory_label_coverage_rate": 0.60,
                "minimum_confirmatory_positive_work_count": 50,
                "minimum_confirmatory_negative_work_count": 20,
                "minimum_distinct_negative_work_count": 20,
            }
        },
        "frozen_hybrid_arms": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
        },
    }
    source_build_payload = source_build or {
        "metadata": {
            "artifact_type": "ml_fresh_product_candidate_source_build",
            "build_version": "ml-fresh-product-candidate-source-build-v1",
        },
        "build_result": {
            "status": "blocked_needs_corpus_or_candidate_expansion",
            "recommended_next_stage": "blocked_expand_corpus_or_candidate_generation",
            "confirmatory_eligible_work_count": 44,
        },
        "candidate_source": {
            "candidate_rows": [
                {"canonical_openalex_work_id": f"W2{i:02d}", "underpowered_source_overlap": True}
                for i in range(0, 44)
            ]
        },
    }
    expansion_payload = expansion_plan or {
        "metadata": {
            "artifact_type": "ml_fresh_candidate_source_expansion_plan",
            "plan_version": "ml-fresh-candidate-source-expansion-plan-v1",
        },
        "current_blocker_summary": {
            "best_source_ranking_run_id": "rank-3904fec89d",
            "candidate_gap": 56,
        },
    }
    conflict_path = tmp_path / "conflict-policy.md"
    conflict_path.write_text("# Conflict Policy\n\nNo silent merge.\n", encoding="utf-8")
    return {
        "fresh_product_candidate_source_build_path": _write_json(tmp_path, "source-build.json", source_build_payload),
        "fresh_candidate_source_expansion_plan_path": _write_json(tmp_path, "expansion-plan.json", expansion_payload),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy_payload),
        "label_dataset_path": _write_json(tmp_path, "labels.json", {"dataset_version": "ml-label-dataset-v8", "rows": []}),
        "conflict_policy_path": conflict_path,
    }


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corpus_plan: dict | None = None, **kwargs: object) -> dict:
    return build_ml_fresh_hybrid_corpus_candidate_plan_payload(
        **_paths(tmp_path, monkeypatch),
        corpus_candidate_plan=corpus_plan or _corpus_plan(),
        target_min=160,
        target_max=500,
        repo_root=tmp_path,
        generated_at="2026-05-18T00:00:00Z",
        **kwargs,
    )


def test_fixture_plan_excludes_old_217_from_estimated_eligible_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_hybrid_corpus_candidate_plan"
    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["candidate_selection"]["estimated_overlap_with_old_217"] == 3
    assert payload["candidate_selection"]["estimated_confirmatory_eligible_after_old_217_exclusion"] == 107


def test_dedupes_canonical_openalex_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    ids = [row["canonical_openalex_work_id"] for row in payload["candidate_selection"]["selected_candidates"]]

    assert len(ids) == len(set(ids))
    assert ids.count("W1") == 1


def test_candidate_threshold_plausibly_met_only_when_estimated_eligible_at_least_100(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    passing = _build(tmp_path, monkeypatch, corpus_plan=_corpus_plan(new_count=98, old_count=3, underpowered_count=2))
    failing = _build(tmp_path, monkeypatch, corpus_plan=_corpus_plan(new_count=97, old_count=3, underpowered_count=2))

    assert passing["candidate_selection"]["estimated_confirmatory_eligible_after_old_217_exclusion"] == 100
    assert passing["candidate_selection"]["candidate_threshold_plausibly_met"] is True
    assert failing["candidate_selection"]["estimated_confirmatory_eligible_after_old_217_exclusion"] == 99
    assert failing["candidate_selection"]["candidate_threshold_plausibly_met"] is False


def test_negative_or_borderline_bucket_present_or_shortfall_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    present = _build(tmp_path, monkeypatch, corpus_plan=_corpus_plan(include_negative_bucket=True))
    shortfall = _build(tmp_path, monkeypatch, corpus_plan=_corpus_plan(include_negative_bucket=False))

    assert present["bucket_summary"]["negative_or_borderline_candidate"]["present"] is True
    assert present["bucket_summary"]["negative_or_borderline_candidate"]["selected_count"] > 0
    assert shortfall["bucket_summary"]["negative_or_borderline_candidate"]["present"] is False
    assert shortfall["bucket_summary"]["shortfall_report"]["shortfall_type"] == "negative_or_borderline_candidate_bucket_absent"


def test_rejects_source_build_with_wrong_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bad_source = {
        "metadata": {
            "artifact_type": "ml_fresh_product_candidate_source_build",
            "build_version": "ml-fresh-product-candidate-source-build-v1",
        },
        "build_result": {
            "status": "source_built_artifact_only",
            "recommended_next_stage": "extend_materializer_to_accept_candidate_source_build_artifact",
            "confirmatory_eligible_work_count": 100,
        },
    }
    with pytest.raises(MLFreshHybridCorpusCandidatePlanError, match="status"):
        build_ml_fresh_hybrid_corpus_candidate_plan_payload(
            **_paths(tmp_path, monkeypatch, source_build=bad_source),
            corpus_candidate_plan=_corpus_plan(),
            repo_root=tmp_path,
        )


def test_shadow_and_production_are_blocked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False
    assert payload["shadow_and_production_blockers"]["confirmatory_validation_complete"] is False


def test_markdown_reports_plan_and_not_production(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    md = markdown_from_ml_fresh_hybrid_corpus_candidate_plan(_build(tmp_path, monkeypatch))

    assert "Candidate threshold plausibly met" in md
    assert "Negative / Borderline Intent" in md
    assert "Not Validation / Not Shadow / Not Production" in md


def test_cli_writes_json_and_markdown_with_mocked_corpus_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path, monkeypatch)
    out_json = tmp_path / "hybrid-candidate-plan.json"
    out_md = tmp_path / "hybrid-candidate-plan.md"
    monkeypatch.setattr(plan_mod, "run_corpus_v2_candidate_plan", lambda **kwargs: _corpus_plan())

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-hybrid-corpus-candidate-plan",
        "--fresh-product-candidate-source-build",
        str(paths["fresh_product_candidate_source_build_path"]),
        "--fresh-candidate-source-expansion-plan",
        str(paths["fresh_candidate_source_expansion_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--target-min",
        "160",
        "--target-max",
        "500",
        "--mailto",
        "reviewer@example.test",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["candidate_selection"]["candidate_threshold_plausibly_met"] is True
    assert "Bucket Composition" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_hybrid_corpus_candidate_plan.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-fresh-hybrid-corpus-candidate-plan"')
    end = cli_source.index("ml_source_split_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
