from __future__ import annotations

from types import SimpleNamespace

import pytest

import pipeline.cli as cli


def test_main_redacts_unhandled_exception_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "postgresql://admin:super-secret@db.example.test/research?token=sentinel"
    parser = SimpleNamespace(parse_args=lambda: SimpleNamespace(command="test"))

    monkeypatch.setattr(cli, "build_parser", lambda: parser)

    def _fail_dispatch(*args, **kwargs) -> None:
        raise RuntimeError(f"connection failed for {secret}")

    monkeypatch.setattr(cli, "dispatch_command", _fail_dispatch)

    with pytest.raises(SystemExit) as caught:
        cli.main()

    captured = capsys.readouterr()
    assert caught.value.code == 1
    assert captured.out == ""
    assert captured.err == "RuntimeError: details redacted\n"
    assert secret not in captured.err
    assert "super-secret" not in captured.err
    assert "sentinel" not in captured.err
