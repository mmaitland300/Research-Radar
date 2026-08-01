"""Safe values for durable error records."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


def safe_exception_summary(exc: BaseException) -> str:
    """Return a non-sensitive label suitable for logs and durable records."""
    return f"{type(exc).__name__}: details redacted"


def safe_url_for_artifact(url: str) -> str:
    """Keep only a URL's scheme, host, port, and path for a durable artifact."""
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
    except (TypeError, ValueError):
        return "invalid://redacted"
    if not parsed.scheme or not hostname:
        return "invalid://redacted"

    authority = f"[{hostname}]" if ":" in hostname else hostname
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is not None:
        authority = f"{authority}:{port}"
    return urlunsplit((parsed.scheme, authority, parsed.path, "", ""))
