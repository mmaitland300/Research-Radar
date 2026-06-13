"""CLI tests for bootstrap planning commands."""

from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
from pipeline.policy import CorpusPolicy
from pipeline.source_resolution import SourceResolutionOutcome


def test_bootstrap_plan_resolve_openalex_uses_source_resolution(
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    policy = CorpusPolicy()
    outcomes = tuple(
        SourceResolutionOutcome(
            source_slug=source.slug,
            openalex_source_id=source.openalex_source_id or f"https://openalex.org/S-{source.slug}",
            matched_display_name=source.display_name,
            search_query=source.display_name,
        )
        for source in policy.source_policies
    )

    with (
        patch("pipeline.cli_app.core_dispatch.resolve_all_sources", return_value=outcomes) as mock_resolve,
        patch.object(
            cli_main.sys,
            "argv",
            [
                "pipeline.cli",
                "bootstrap-plan",
                "--resolve-openalex",
                "--output",
                str(tmp_path),
                "--mailto",
                "review@example.com",
            ],
        ),
    ):
        cli_main.main()

    mock_resolve.assert_called_once()
    assert mock_resolve.call_args.kwargs["mailto"] == "review@example.com"

    stdout = capsys.readouterr().out.strip().splitlines()
    assert len(stdout) == 2
    snapshot_dir = tmp_path / stdout[0]
    assert (snapshot_dir / "source-snapshot.json").is_file()
    assert (snapshot_dir / "source-resolution-plan.json").is_file()
    assert (snapshot_dir / "source-resolution-results.json").is_file()
    assert (snapshot_dir / "bootstrap-work-plan.json").is_file()
