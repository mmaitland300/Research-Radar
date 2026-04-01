from unittest.mock import MagicMock, patch

import pytest

from pipeline.ranking_run import warn_embedding_gaps_if_any


@patch("pipeline.ranking_run.count_missing_embedding_candidates", return_value=5)
def test_warn_embedding_gaps_prints_to_stderr_when_missing(
    _mock_count: MagicMock, capsys: pytest.CaptureFixture[str]
) -> None:
    warn_embedding_gaps_if_any(
        MagicMock(),
        corpus_snapshot_version="source-snap",
        embedding_version="v1-emb",
    )
    err = capsys.readouterr().err
    assert "ranking-run: warning:" in err
    assert "5" in err
    assert "v1-emb" in err
    assert "embed-works" in err


@patch("pipeline.ranking_run.count_missing_embedding_candidates", return_value=0)
def test_warn_embedding_gaps_silent_when_complete(_mock_count: MagicMock, capsys: pytest.CaptureFixture[str]) -> None:
    warn_embedding_gaps_if_any(
        MagicMock(),
        corpus_snapshot_version="source-snap",
        embedding_version="v1-emb",
    )
    assert capsys.readouterr().err == ""
