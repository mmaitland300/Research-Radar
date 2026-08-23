from __future__ import annotations


def register_release_parsers(subparsers) -> None:
    parser = subparsers.add_parser(
        "public-release-promote",
        help="Validate and promote one exact succeeded ranking run for public serving",
    )
    parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Exact immutable ranking run to validate and promote",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL (defaults to DATABASE_URL or PG* variables)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full promotion gate without appending a promotion",
    )
