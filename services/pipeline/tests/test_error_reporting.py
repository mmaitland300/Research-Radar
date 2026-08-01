from pipeline.error_reporting import safe_exception_summary, safe_url_for_artifact


def test_safe_exception_summary_omits_exception_details() -> None:
    secret = "super-secret-sentinel"
    details = (
        f"postgresql://admin:{secret}@db.example.test/research\n"
        f"password={secret} api_key={secret}"
    )

    summary = safe_exception_summary(RuntimeError(details))

    assert summary == "RuntimeError: details redacted"
    assert secret not in summary
    assert "postgresql://" not in summary


def test_safe_exception_summary_never_stringifies_exception() -> None:
    class UnstringifiableError(RuntimeError):
        def __str__(self) -> str:
            raise AssertionError("exception must not be stringified")

    assert safe_exception_summary(UnstringifiableError()) == (
        "UnstringifiableError: details redacted"
    )


def test_safe_url_for_artifact_removes_credentials_query_and_fragment() -> None:
    value = safe_url_for_artifact(
        "https://userinfo-secret:password-secret@api.example.test:8443/v1/models"
        "?query-secret=value#fragment-secret"
    )

    assert value == "https://api.example.test:8443/v1/models"
    for secret in (
        "userinfo-secret",
        "password-secret",
        "query-secret",
        "fragment-secret",
    ):
        assert secret not in value
