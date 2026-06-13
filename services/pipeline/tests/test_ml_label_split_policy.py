"""Tests for the ML label split policy spec artifact."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_label_split_policy import (
    MLLabelSplitPolicyError,
    POLICY_VERSION,
    build_ml_label_split_policy_payload,
    canonical_openalex_work_id,
    markdown_from_ml_label_split_policy,
)


def _row(
    row_id: str,
    work: str | None,
    *,
    variant: str = "ml_external_near_miss_audit",
    family: str | None = None,
    relevance: str = "good",
    novelty: str = "useful",
    good: bool | None = True,
    surprising: bool | None = True,
) -> dict:
    return {
        "dataset_version": "ml-label-dataset-v8",
        "row_id": row_id,
        "split": "audit_only",
        "review_pool_variant": variant,
        "family": family,
        "paper_id": work or "",
        "work_id": work or "",
        "openalex_work_id": work or "",
        "relevance_label": relevance,
        "novelty_label": novelty,
        "bridge_like_label": "no",
        "reviewer_notes": "notes",
        "good_or_acceptable": good,
        "surprising_or_useful": surprising,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v8",
        "rows": [
            _row("r1", "https://openalex.org/W1", variant="ml_blind_snapshot_audit", good=True, surprising=True),
            _row("r2", "w1", variant="ml_external_near_miss_audit", good=False, surprising=False),
            _row("r3", "W2", variant="ml_transfer_gap_audit", good=True, surprising=False),
            _row("r4", None, variant="ml_transfer_gap_audit", good=True, surprising=None),
        ],
        "metadata": {
            "duplicate_paper_id_report": {"duplicate_paper_id_count": 1},
            "conflicting_label_report": {"conflicting_label_count": 2},
            "derived_target_conflict_report": {"derived_target_conflict_count": 3},
        },
    }


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
            "plan_schema_version": 1,
        }
    }


def _transfer_readiness_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_text_transfer_readiness",
            "readiness_version": "ml-text-transfer-readiness-v8",
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _write_policy(tmp_path: Path) -> Path:
    path = tmp_path / "ml-label-conflict-policy.md"
    path.write_text("# Conflict Policy\n\nNo silent merge.\n", encoding="utf-8")
    return path


def _build(tmp_path: Path) -> dict:
    return build_ml_label_split_policy_payload(
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        conflict_policy_path=_write_policy(tmp_path),
        production_readiness_plan_path=_write_json(tmp_path, "plan.json", _production_plan_payload()),
        transfer_readiness_path=_write_json(tmp_path, "transfer.json", _transfer_readiness_payload()),
        repo_root=tmp_path,
        generated_at="2026-05-15T00:00:00Z",
    )


def test_payload_records_inputs_targets_assertions_and_inventory(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path)
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    payload = build_ml_label_split_policy_payload(
        label_dataset_path=label_path,
        conflict_policy_path=policy_path,
        production_readiness_plan_path=_write_json(tmp_path, "plan.json", _production_plan_payload()),
        transfer_readiness_path=_write_json(tmp_path, "transfer.json", _transfer_readiness_payload()),
        repo_root=tmp_path,
        generated_at="2026-05-15T00:00:00Z",
    )

    assert payload["metadata"]["policy_version"] == POLICY_VERSION
    assert payload["metadata"]["label_dataset_version"] == "ml-label-dataset-v8"
    assert payload["metadata"]["conflict_policy_sha256"] == hashlib.sha256(policy_path.read_bytes()).hexdigest()
    assert [item["name"] for item in payload["metadata"]["inputs"]] == [
        "label_dataset",
        "conflict_policy",
        "production_readiness_plan",
        "transfer_readiness",
    ]

    assert payload["allowed_targets_for_v1_split"] == ["good_or_acceptable"]
    assert payload["forbidden_targets"] == ["surprising_or_useful"]
    assert payload["target_policy"]["surprising_or_useful"]["status"] == "excluded_from_v1_split"
    assert payload["target_policy"]["surprising_or_useful"]["production_eligible"] is False
    assert payload["target_policy"]["good_or_acceptable"]["status"] == "eligible_for_offline_ranker_research"

    assertions = payload["policy_assertions"]
    assert assertions["surprising_or_useful_allowed_for_v1_split"] is False
    assert assertions["requires_grouped_split_by_work"] is True
    assert assertions["permits_row_level_random_split"] is False
    assert assertions["permits_silent_conflict_resolution"] is False
    assert assertions["production_default_change_allowed"] is False

    inventory = payload["dataset_inventory"]
    assert inventory["total_observation_rows"] == 4
    assert inventory["explicit_labeled_rows"] == 4
    assert inventory["unique_canonical_work_count"] == 2
    assert inventory["duplicate_work_group_count"] == 1
    assert inventory["duplicate_work_observation_pressure_count"] == 1
    assert inventory["rows_missing_canonical_work_id"] == 1
    assert inventory["targets"]["good_or_acceptable"]["overall"] == {"true": 3, "false": 1, "null": 0, "total": 4}
    assert inventory["targets"]["surprising_or_useful"]["overall"] == {
        "true": 1,
        "false": 2,
        "null": 1,
        "total": 4,
    }
    assert inventory["duplicate_conflict_rollups"] == {
        "duplicate_paper_id_count": 1,
        "raw_conflicting_label_count": 2,
        "derived_target_conflict_count": 3,
    }


def test_canonical_work_grouping_normalizes_urls_and_tokens() -> None:
    assert canonical_openalex_work_id({"work_id": " https://openalex.org/w123 "}) == "W123"
    assert canonical_openalex_work_id({"openalex_work_id": "w456"}) == "W456"
    assert canonical_openalex_work_id({"paper_id": "x"}) is None


def test_version_validation_requires_v8_label_dataset_and_v1_plan(tmp_path: Path) -> None:
    labels = _label_payload()
    labels["dataset_version"] = "ml-label-dataset-v7"
    with pytest.raises(MLLabelSplitPolicyError, match="ml-label-dataset-v8"):
        build_ml_label_split_policy_payload(
            label_dataset_path=_write_json(tmp_path, "bad-labels.json", labels),
            conflict_policy_path=_write_policy(tmp_path),
            production_readiness_plan_path=_write_json(tmp_path, "plan.json", _production_plan_payload()),
        )

    bad_plan = _production_plan_payload()
    bad_plan["metadata"]["plan_version"] = "wrong"
    with pytest.raises(MLLabelSplitPolicyError, match="plan_version"):
        build_ml_label_split_policy_payload(
            label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
            conflict_policy_path=_write_policy(tmp_path),
            production_readiness_plan_path=_write_json(tmp_path, "bad-plan.json", bad_plan),
        )

    bad_transfer = _transfer_readiness_payload()
    bad_transfer["metadata"]["artifact_type"] = "wrong"
    with pytest.raises(MLLabelSplitPolicyError, match="transfer-readiness"):
        build_ml_label_split_policy_payload(
            label_dataset_path=_write_json(tmp_path, "labels2.json", _label_payload()),
            conflict_policy_path=_write_policy(tmp_path),
            production_readiness_plan_path=_write_json(tmp_path, "plan2.json", _production_plan_payload()),
            transfer_readiness_path=_write_json(tmp_path, "bad-transfer.json", bad_transfer),
        )


def test_markdown_includes_not_production_caveat(tmp_path: Path) -> None:
    md = markdown_from_ml_label_split_policy(_build(tmp_path))
    assert "Target Policy" in md
    assert "`surprising_or_useful`" in md
    assert "No production ranking, API, web, or default behavior change is implied" in md
    assert "No fold assignment yet." in md


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "policy.json"
    out_md = tmp_path / "policy.md"
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-label-split-policy",
        "--label-dataset",
        str(_write_json(tmp_path, "labels.json", _label_payload())),
        "--conflict-policy",
        str(_write_policy(tmp_path)),
        "--production-readiness-plan",
        str(_write_json(tmp_path, "plan.json", _production_plan_payload())),
        "--transfer-readiness",
        str(_write_json(tmp_path, "transfer.json", _transfer_readiness_payload())),
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
    assert data["metadata"]["artifact_type"] == "ml_label_split_policy"
    assert data["allowed_targets_for_v1_split"] == ["good_or_acceptable"]
    assert "ML Label Split Policy" in out_md.read_text(encoding="utf-8")


def test_no_database_sklearn_openalex_client_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_label_split_policy.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "openai" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-label-split-policy"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
