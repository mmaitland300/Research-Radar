"""Tests for ML production readiness gate plan artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_production_readiness_plan import (
    CAVEATS,
    PLAN_VERSION,
    REQUIRED_GATE_IDS,
    MLProductionReadinessPlanError,
    build_ml_production_readiness_plan_payload,
    render_markdown,
)


def _metric(ba: float, auc: float) -> dict:
    return {
        "balanced_accuracy": ba,
        "roc_auc": auc,
        "macro_f1": 0.62,
        "confusion": {"tn": 2, "fp": 1, "fn": 1, "tp": 2},
    }


def _transfer_payload(*, good_strong: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_text_transfer_readiness",
            "readiness_version": "ml-text-transfer-readiness-v1",
            "thresholds": {
                "strong_in_pool_balanced_accuracy_threshold": 0.7,
                "weak_transfer_balanced_accuracy_threshold": 0.55,
                "sparse_class_min_count_threshold": 20,
            },
        },
        "class_balance_by_review_pool_variant": {
            "total_explicit_labeled_row_count": 8,
            "by_target": {
                "good_or_acceptable": {
                    "ml_external_near_miss_audit": {"true": 2, "false": 2, "null": 0, "total": 4},
                    "ml_blind_snapshot_audit": {"true": 3, "false": 1, "null": 0, "total": 4},
                    "ml_hard_negative_audit": {"true": 1, "false": 0, "null": 0, "total": 1},
                },
                "surprising_or_useful": {
                    "ml_external_near_miss_audit": {"true": 3, "false": 1, "null": 0, "total": 4},
                    "ml_blind_snapshot_audit": {"true": 3, "false": 1, "null": 0, "total": 4},
                    "ml_hard_negative_audit": {"true": 1, "false": 0, "null": 0, "total": 1},
                },
            },
        },
        "heuristic_readiness_flags": {
            "good_or_acceptable": {
                "in_pool_signal_strong": {"value": good_strong, "evidence": [{"metric_name": "roc_auc"}]},
                "external_blind_transfer_weak": {"value": False, "evidence": []},
                "transfer_inconsistent": {"value": False, "evidence": []},
                "needs_more_labels": {"value": True, "evidence": [{"metric_name": "min_class_count"}]},
                "production_ready": {"value": False, "evidence": [{"metric_name": "production_ready"}]},
            },
            "surprising_or_useful": {
                "in_pool_signal_strong": {"value": True, "evidence": [{"metric_name": "roc_auc"}]},
                "external_blind_transfer_weak": {"value": True, "evidence": [{"metric_name": "roc_auc"}]},
                "transfer_inconsistent": {"value": True, "evidence": [{"metric_name": "transfer_inconsistent"}]},
                "needs_more_labels": {"value": True, "evidence": [{"metric_name": "min_class_count"}]},
                "production_ready": {"value": False, "evidence": [{"metric_name": "production_ready"}]},
            },
        },
        "cross_pool_synthesis": {
            "good_or_acceptable": {
                "source_transfer": {
                    "external_near_miss_to_blind_snapshot": {"models": {"embedding_logistic": _metric(0.68, 0.76)}},
                    "blind_snapshot_to_external_near_miss": {"models": {"embedding_logistic": _metric(0.56, 0.77)}},
                }
            },
            "surprising_or_useful": {
                "source_transfer": {
                    "external_near_miss_to_blind_snapshot": {"models": {"embedding_logistic": _metric(0.49, 0.43)}},
                    "blind_snapshot_to_external_near_miss": {"models": {"embedding_logistic": _metric(0.51, 0.40)}},
                }
            },
        },
        "recommendations": [
            {
                "priority": 1,
                "action": "Clarify surprising rubric and collect balanced labels.",
                "rationale": "surprising transfer is weak.",
                "depends_on_artifact": "ml-text-baseline-cross-pool-v1",
            },
            {
                "priority": 2,
                "action": "Expand good labels for offline ranker research.",
                "rationale": "good is primary but sparse.",
                "depends_on_artifact": "ml-label-dataset-v7",
            },
        ],
        "production_recommender_missing_gates": ["multi-reviewer or adjudication policy", "shadow/flagged experiment plan"],
    }


def _label_row(index: int, *, explicit: bool = True, split: str = "audit_only") -> dict:
    return {
        "row_id": f"row-{index}",
        "split": split,
        "review_pool_variant": "ml_external_near_miss_audit",
        "paper_id": f"W{index}",
        "relevance_label": "good" if explicit else "",
        "novelty_label": "yes" if explicit else "",
        "bridge_like_label": "no" if explicit else "",
        "reviewer_notes": "notes",
        "good_or_acceptable": True,
        "surprising_or_useful": True,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v7",
        "rows": [
            _label_row(1),
            _label_row(2, explicit=False),
            _label_row(3, split="candidate"),
            _label_row(4),
        ],
    }


def _cross_pool_payload() -> dict:
    return {"metadata": {"artifact_type": "ml_text_baseline_cross_pool", "baseline_version": "ml-text-baseline-cross-pool-v1"}}


def _text_corpus_payload() -> dict:
    return {"metadata": {"artifact_type": "ml_labeled_text_corpus", "corpus_version": "ml-labeled-text-corpus-v2"}}


def _embeddings_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v1",
        },
        "rows": [{"embedding": "not inspected"}],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _write_policy(tmp_path: Path) -> Path:
    path = tmp_path / "ml-label-conflict-policy.md"
    path.write_text("# Conflict policy\n\nNo silent merge.\n", encoding="utf-8")
    return path


def test_plan_current_like_fixture_is_research_only_with_all_gates(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    payload = build_ml_production_readiness_plan_payload(
        transfer_readiness_path=_write_json(tmp_path, "readiness.json", _transfer_payload()),
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        conflict_policy_path=policy,
        cross_pool_path=_write_json(tmp_path, "cross.json", _cross_pool_payload()),
        text_corpus_v2_path=_write_json(tmp_path, "corpus.json", _text_corpus_payload()),
        embeddings_v1_path=_write_json(tmp_path, "embeddings.json", _embeddings_payload()),
        generated_at="2026-05-15T00:00:00Z",
    )

    metadata = payload["metadata"]
    assert metadata["plan_version"] == PLAN_VERSION
    assert metadata["overall_status"] == "research_only"
    assert metadata["evaluation_rules"]["strong_in_pool_balanced_accuracy_threshold"] == 0.7
    assert [item["name"] for item in metadata["inputs"]] == [
        "transfer_readiness",
        "label_dataset",
        "conflict_policy",
        "cross_pool",
        "text_corpus_v2",
        "embeddings_v1",
    ]
    policy_input = next(item for item in metadata["inputs"] if item["name"] == "conflict_policy")
    assert policy_input["sha256"] == hashlib.sha256(policy.read_bytes()).hexdigest()

    assert payload["targets"]["good_or_acceptable"]["status"] == "primary_candidate"
    assert payload["targets"]["good_or_acceptable"]["allowed_next_stage"] == "offline_ranker_research_only"
    assert payload["targets"]["good_or_acceptable"]["production_eligible"] is False
    assert payload["targets"]["surprising_or_useful"]["status"] == "deferred"
    assert payload["targets"]["surprising_or_useful"]["allowed_next_stage"] == "rubric_and_labeling_only"

    assert [gate["gate_id"] for gate in payload["gates"]] == list(REQUIRED_GATE_IDS)
    gate_by_id = {gate["gate_id"]: gate for gate in payload["gates"]}
    assert gate_by_id["G5_split_policy_artifact"]["status"] == "not_started"
    assert gate_by_id["G5_split_policy_artifact"]["blocking"]["offline_ranker_experiment"] is True
    assert gate_by_id["G1_target_selection"]["status"] == "partial"
    assert gate_by_id["G1_target_selection"]["blocking"]["offline_ranker_experiment"] is False
    assert gate_by_id["G10_production_rollout"]["blocking"]["production_default"] is True

    assert any(gap["priority"] == "P1" and gap["target"] == "surprising_or_useful" for gap in payload["label_gaps"])
    assert any(gap["priority"] == "P2" and gap["target"] == "good_or_acceptable" for gap in payload["label_gaps"])
    assert any(gap["priority"] == "P3" and gap["pool"] == "ml_hard_negative_audit" for gap in payload["label_gaps"])
    assert any("Silent deduplication" in item["condition"] for item in payload["no_go_conditions"])
    assert any(
        item["evidence"]["source_artifact"] == "ml-label-conflict-policy.md" for item in payload["no_go_conditions"]
    )
    assert [item["name"] for item in payload["next_artifacts"]] == [
        "ml-transfer-gap-review-worksheet",
        "ml-label-split-policy-v1",
        "ml-offline-ranker-experiment-v1",
        "ml-shadow-scorer-v1",
        "production flag change",
    ]
    assert "human approval gate" in payload["production_recommender_missing_gates"]
    for caveat in CAVEATS:
        assert caveat in metadata["caveats"]


def test_overall_status_blocked_when_primary_signal_missing(tmp_path: Path) -> None:
    payload = build_ml_production_readiness_plan_payload(
        transfer_readiness_path=_write_json(tmp_path, "readiness.json", _transfer_payload(good_strong=False)),
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        conflict_policy_path=_write_policy(tmp_path),
        generated_at="2026-05-15T00:00:00Z",
    )
    assert payload["metadata"]["overall_status"] == "blocked"
    assert "does not have a strong in-pool signal" in payload["metadata"]["overall_status_rationale"]


def test_fallback_class_balance_uses_row_has_explicit_label(tmp_path: Path) -> None:
    readiness = _transfer_payload()
    readiness.pop("class_balance_by_review_pool_variant")
    payload = build_ml_production_readiness_plan_payload(
        transfer_readiness_path=_write_json(tmp_path, "readiness.json", readiness),
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        conflict_policy_path=_write_policy(tmp_path),
        generated_at="2026-05-15T00:00:00Z",
    )
    gap = next(gap for gap in payload["label_gaps"] if gap["priority"] == "P2")
    assert gap["current_pos"] == 2
    assert gap["current_neg"] == 0


def test_version_validation_and_missing_policy_fail(tmp_path: Path) -> None:
    bad_readiness = _transfer_payload()
    bad_readiness["metadata"]["readiness_version"] = "wrong"
    with pytest.raises(MLProductionReadinessPlanError, match="readiness_version"):
        build_ml_production_readiness_plan_payload(
            transfer_readiness_path=_write_json(tmp_path, "bad-readiness.json", bad_readiness),
            label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
            conflict_policy_path=_write_policy(tmp_path),
        )

    bad_labels = _label_payload()
    bad_labels["dataset_version"] = "wrong"
    with pytest.raises(MLProductionReadinessPlanError, match="dataset_version"):
        build_ml_production_readiness_plan_payload(
            transfer_readiness_path=_write_json(tmp_path, "readiness.json", _transfer_payload()),
            label_dataset_path=_write_json(tmp_path, "bad-labels.json", bad_labels),
            conflict_policy_path=_write_policy(tmp_path),
        )

    bad_cross = _cross_pool_payload()
    bad_cross["metadata"]["baseline_version"] = "wrong"
    with pytest.raises(MLProductionReadinessPlanError, match="baseline_version"):
        build_ml_production_readiness_plan_payload(
            transfer_readiness_path=_write_json(tmp_path, "readiness2.json", _transfer_payload()),
            label_dataset_path=_write_json(tmp_path, "labels2.json", _label_payload()),
            conflict_policy_path=_write_policy(tmp_path),
            cross_pool_path=_write_json(tmp_path, "bad-cross.json", bad_cross),
        )

    with pytest.raises(MLProductionReadinessPlanError, match="conflict policy"):
        build_ml_production_readiness_plan_payload(
            transfer_readiness_path=_write_json(tmp_path, "readiness3.json", _transfer_payload()),
            label_dataset_path=_write_json(tmp_path, "labels3.json", _label_payload()),
            conflict_policy_path=tmp_path / "missing.md",
        )


def test_markdown_contains_gate_table_and_not_production_warning(tmp_path: Path) -> None:
    payload = build_ml_production_readiness_plan_payload(
        transfer_readiness_path=_write_json(tmp_path, "readiness.json", _transfer_payload()),
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        conflict_policy_path=_write_policy(tmp_path),
        generated_at="2026-05-15T00:00:00Z",
    )
    md = render_markdown(payload)
    assert "## Gate Checklist" in md
    assert "G1_target_selection" in md
    assert "Not Validation / Not Production Recommender Test" in md
    assert "No production ranking, shadow scoring, or default change is authorized" in md


def test_no_database_sklearn_network_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_production_readiness_plan.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source
    assert "embedding_provider" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-production-readiness-plan"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
