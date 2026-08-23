from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main
from pipeline.public_release import (
    PublicReleasePromotionError,
    PublicReleasePromotionResult,
)


def _result() -> PublicReleasePromotionResult:
    return PublicReleasePromotionResult(
        status="validated",
        changed=False,
        dry_run=True,
        promotion_id=None,
        promoted_at=None,
        ranking_run_id="run-1",
        ranking_version="ranking-v1",
        corpus_snapshot_version="snapshot-v1",
        embedding_version="embedding-v1",
        membership_count=8,
        rows_by_family={"emerging": 8, "bridge": 8, "undercited": 3},
        cluster_version=None,
        active_ranking_run_id_before=None,
    )


def test_parser_exposes_only_exact_run_database_and_dry_run_flags() -> None:
    parser = cli_main.build_parser()
    args = parser.parse_args(
        [
            "public-release-promote",
            "--ranking-run-id",
            "run-1",
            "--database-url",
            "postgresql://test/db",
            "--dry-run",
        ]
    )

    assert args.ranking_run_id == "run-1"
    assert args.database_url == "postgresql://test/db"
    assert args.dry_run is True
    option_strings = {
        option
        for action in parser._subparsers._group_actions[0].choices[
            "public-release-promote"
        ]._actions
        for option in action.option_strings
    }
    assert "--latest" not in option_strings
    assert "--force" not in option_strings


@patch("pipeline.cli_app.release_dispatch.promote_public_release", return_value=_result())
def test_cli_promotes_exact_run_and_prints_json(
    promote: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch.object(
        cli_main.sys,
        "argv",
        [
            "pipeline.cli",
            "public-release-promote",
            "--ranking-run-id",
            "run-1",
            "--database-url",
            "postgresql://test/db",
            "--dry-run",
        ],
    ):
        cli_main.main()

    promote.assert_called_once_with(
        ranking_run_id="run-1",
        database_url="postgresql://test/db",
        dry_run=True,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "validated"
    assert payload["ranking_run_id"] == "run-1"
    assert payload["rows_by_family"]["undercited"] == 3


@patch(
    "pipeline.cli_app.release_dispatch.promote_public_release",
    side_effect=PublicReleasePromotionError("snapshot has no included memberships"),
)
def test_cli_reports_gate_failure_with_validation_exit_code(
    _promote: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch.object(
        cli_main.sys,
        "argv",
        ["pipeline.cli", "public-release-promote", "--ranking-run-id", "run-1"],
    ):
        with pytest.raises(SystemExit) as caught:
            cli_main.main()

    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "public-release-promote: snapshot has no included memberships\n"
    )
