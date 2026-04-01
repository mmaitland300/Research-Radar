"""CLI tests for embedding-coverage and repair-works-text operational commands."""

from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main


@patch("pipeline.cli.count_missing_embedding_candidates", return_value=3)
@patch("pipeline.cli.count_included_works_for_snapshot", return_value=40)
@patch("pipeline.cli.psycopg.connect")
def test_embedding_coverage_cli_prints_stderr_summary(
    mock_connect: MagicMock,
    _mock_total: MagicMock,
    _mock_missing: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn = MagicMock()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = conn
    mock_cm.__exit__.return_value = None
    mock_connect.return_value = mock_cm

    with patch.object(
        cli_main.sys,
        "argv",
        [
            "pipeline.cli",
            "embedding-coverage",
            "--embedding-version",
            "v1-test",
            "--corpus-snapshot-version",
            "snap-abc",
        ],
    ):
        cli_main.main()

    err = capsys.readouterr().err
    assert "corpus_snapshot_version=snap-abc" in err
    assert "embedding_version=v1-test" in err
    assert "included_works=40" in err
    assert "with_embedding=37" in err
    assert "missing_embedding=3" in err


@patch("pipeline.cli.count_missing_embedding_candidates", return_value=2)
@patch("pipeline.cli.count_included_works_for_snapshot", return_value=10)
@patch("pipeline.cli.psycopg.connect")
def test_embedding_coverage_cli_fail_on_gaps_exits_one(
    mock_connect: MagicMock,
    _mock_total: MagicMock,
    _mock_missing: MagicMock,
) -> None:
    conn = MagicMock()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = conn
    mock_cm.__exit__.return_value = None
    mock_connect.return_value = mock_cm

    with patch.object(
        cli_main.sys,
        "argv",
        [
            "pipeline.cli",
            "embedding-coverage",
            "--embedding-version",
            "v1-test",
            "--corpus-snapshot-version",
            "snap-abc",
            "--fail-on-gaps",
        ],
    ):
        with pytest.raises(SystemExit) as exc_info:
            cli_main.main()
    assert exc_info.value.code == 1


@patch("pipeline.cli.run_work_text_repair_cli", return_value=("snap-repair", 12, 3))
def test_repair_works_text_cli_invokes_runner(
    _mock_runner: MagicMock, capsys: pytest.CaptureFixture[str]
) -> None:
    with patch.object(
        cli_main.sys,
        "argv",
        ["pipeline.cli", "repair-works-text", "--corpus-snapshot-version", "snap-repair", "--dry-run"],
    ):
        cli_main.main()
    err = capsys.readouterr().err
    assert "repair-works-text (dry-run)" in err
    assert "rows_changed=3" in err
    assert "scanned=12" in err
