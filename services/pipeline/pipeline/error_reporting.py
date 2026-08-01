"""Safe values for durable error records."""

from __future__ import annotations


def safe_exception_summary(exc: BaseException) -> str:
    """Return a non-sensitive label suitable for logs and durable records."""
    return f"{type(exc).__name__}: details redacted"
