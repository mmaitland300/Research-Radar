from unittest.mock import MagicMock, patch

from pipeline.work_text_repair import repair_works_text_in_place, run_work_text_repair_cli


def test_repair_works_text_in_place_dry_run_counts_without_update() -> None:
    conn = MagicMock()
    select_r = MagicMock()
    select_r.fetchall.return_value = [(1, "The 2023 â Music Track", None)]
    conn.execute.return_value = select_r

    scanned, changed = repair_works_text_in_place(
        conn, corpus_snapshot_version="source-snap-test", dry_run=True
    )

    assert scanned == 1
    assert changed == 1
    assert conn.execute.call_count == 1
    sql_first = conn.execute.call_args_list[0][0][0]
    assert "SELECT" in sql_first


def test_repair_works_text_in_place_commits_update_when_not_dry_run() -> None:
    conn = MagicMock()
    select_r = MagicMock()
    select_r.fetchall.return_value = [(42, "CrossâModal paper", None)]
    update_r = MagicMock()
    conn.execute.side_effect = [select_r, update_r]

    scanned, changed = repair_works_text_in_place(
        conn, corpus_snapshot_version="source-snap-test", dry_run=False
    )

    assert scanned == 1
    assert changed == 1
    assert conn.execute.call_count == 2
    second_sql = conn.execute.call_args_list[1][0][0]
    assert "UPDATE works" in second_sql


def test_repair_works_text_in_place_skips_clean_rows() -> None:
    conn = MagicMock()
    select_r = MagicMock()
    select_r.fetchall.return_value = [(1, "Clean title", "Clean abstract body.")]
    conn.execute.return_value = select_r

    scanned, changed = repair_works_text_in_place(
        conn, corpus_snapshot_version="source-snap-test", dry_run=False
    )

    assert scanned == 1
    assert changed == 0
    assert conn.execute.call_count == 1


@patch("pipeline.work_text_repair.psycopg.connect")
@patch("pipeline.work_text_repair.latest_corpus_snapshot_version_with_works")
def test_run_work_text_repair_cli_uses_latest_snapshot_when_omitted(
    mock_latest: MagicMock, mock_connect: MagicMock
) -> None:
    mock_latest.return_value = "source-from-db"
    select_r = MagicMock()
    select_r.fetchall.return_value = []
    conn = MagicMock()
    conn.execute.return_value = select_r
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = conn
    mock_cm.__exit__.return_value = None
    mock_connect.return_value = mock_cm

    snap, scanned, updated = run_work_text_repair_cli(
        database_url="postgresql://x", corpus_snapshot_version=None, dry_run=True
    )

    assert snap == "source-from-db"
    assert scanned == 0
    assert updated == 0
    mock_connect.assert_called_once()
