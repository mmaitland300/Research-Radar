from __future__ import annotations

from pipeline.ranking_run import (
    BRIDGE_ELIGIBILITY_MODE_CURRENT,
    MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY,
    SUPPORTED_BRIDGE_ELIGIBILITY_MODES,
    validate_bridge_eligibility_mode,
    validate_bridge_weight_for_bridge_family,
)


def register_core_parsers(subparsers) -> None:
    policy_parser = subparsers.add_parser("print-policy", help="Print the active corpus policy")
    policy_parser.add_argument("--json", action="store_true", help="Print as JSON")

    bootstrap_parser = subparsers.add_parser("bootstrap-plan", help="Write bootstrap snapshot, ingest run, and query plans")
    bootstrap_parser.add_argument("--output", default="artifacts", help="Output directory")
    bootstrap_parser.add_argument("--note", default="Bootstrap ingest planning run", help="Snapshot note")
    src = bootstrap_parser.add_mutually_exclusive_group(required=False)
    src.add_argument(
        "--resolve-openalex",
        action="store_true",
        help="Resolve canonical source IDs via OpenAlex /sources (required unless DB or coded IDs exist)",
    )
    src.add_argument(
        "--database-source-ids",
        action="store_true",
        help="Load openalex_source_id from Postgres source_policies (after a prior bootstrap-run resolve)",
    )
    bootstrap_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL for --database-source-ids (default: DATABASE_URL or PG* env)",
    )
    bootstrap_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent when using --resolve-openalex",
    )

    run_parser = subparsers.add_parser(
        "bootstrap-run",
        help="Execute OpenAlex bootstrap: raw pages, Postgres load, manifest (needs DATABASE_URL or PG*)",
    )
    run_parser.add_argument("--output", default="artifacts", help="Manifest and snapshot metadata directory")
    run_parser.add_argument("--raw-root", default="artifacts", help="Root directory for raw OpenAlex page JSON")
    run_parser.add_argument("--note", default="API bootstrap ingest", help="Snapshot note")
    run_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL env or PGHOST/PGUSER/PGPASSWORD/PGDATABASE)",
    )
    run_parser.add_argument(
        "--max-pages-per-source",
        type=int,
        default=None,
        help="Cap pages per venue plan (for smoke tests; default: paginate until exhausted)",
    )
    run_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent (default: OPENALEX_MAILTO env)",
    )

    embed_parser = subparsers.add_parser(
        "embed-works",
        help="Write one embedding per included work from title + abstract",
    )
    embed_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label stored on embeddings rows (e.g. v1-title-abstract-1536)",
    )
    embed_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    embed_parser.add_argument(
        "--model",
        default="text-embedding-3-small",
        help="Embedding model label for the provider request (default: text-embedding-3-small)",
    )
    embed_parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Texts per embedding request batch",
    )
    embed_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap missing works processed on this run (smoke tests only; omit for full snapshot coverage)",
    )
    embed_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ranking_parser = subparsers.add_parser(
        "ranking-run",
        help="Create ranking_runs row, write stub paper_scores, finalize (Step 2 plumbing)",
    )
    ranking_parser.add_argument(
        "--ranking-version",
        required=True,
        help="Algorithm / config label (e.g. v0-heuristic-no-embeddings)",
    )
    ranking_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    ranking_parser.add_argument(
        "--embedding-version",
        default="none-v0",
        help="Embedding artifact version label stored on the run",
    )
    ranking_parser.add_argument(
        "--cluster-version",
        default=None,
        help="Optional succeeded clustering_runs.cluster_version for ML2-5a bridge_score column (must match snapshot + embedding-version)",
    )
    ranking_parser.add_argument("--note", default=None, help="Optional run notes")
    ranking_parser.add_argument(
        "--low-cite-min-year",
        type=int,
        default=2019,
        help="Undercited family: min publication year (default 2019; see docs/candidate-pool-low-cite.md)",
    )
    ranking_parser.add_argument(
        "--low-cite-max-citations",
        type=int,
        default=30,
        help="Undercited family: max citation_count inclusive (default 30)",
    )
    ranking_parser.add_argument(
        "--bridge-weight-for-family-bridge",
        type=lambda s: validate_bridge_weight_for_bridge_family(float(s)),
        default=0.0,
        metavar="W",
        help=(
            "Bridge family only: weight on cluster-boundary bridge_score in final_score (ML2-5b). "
            f"Default 0.0 (ML2-5a). Range [0.0, {MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY}]."
        ),
    )
    ranking_parser.add_argument(
        "--bridge-eligibility-mode",
        type=lambda s: validate_bridge_eligibility_mode(s),
        default=BRIDGE_ELIGIBILITY_MODE_CURRENT,
        choices=sorted(SUPPORTED_BRIDGE_ELIGIBILITY_MODES),
        help=(
            "Bridge eligibility policy for bridge-family rows. "
            "Default current behavior; use top50_cross_cluster_gte_0_40 for stricter threshold sweep mode."
        ),
    )
    ranking_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    cluster_parser = subparsers.add_parser(
        "cluster-works",
        help="Cluster embedded included works for a snapshot/version identity",
    )
    cluster_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label to cluster (must already exist in embeddings table)",
    )
    cluster_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Cluster assignment artifact label written to clusters + clustering_runs",
    )
    cluster_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    cluster_parser.add_argument(
        "--cluster-count",
        type=int,
        default=12,
        help="Target number of clusters for kmeans-l2-v0 (default 12)",
    )
    cluster_parser.add_argument(
        "--max-iterations",
        type=int,
        default=20,
        help="Maximum kmeans iterations (default 20)",
    )
    cluster_parser.add_argument("--note", default=None, help="Optional run notes")
    cluster_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    repair_parser = subparsers.add_parser(
        "repair-works-text",
        help="Re-apply title/abstract text cleanup (mojibake, HTML entities) to included works in a snapshot",
    )
    repair_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    repair_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would change without writing",
    )
    repair_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    cov_parser = subparsers.add_parser(
        "embedding-coverage",
        help="Report how many included works in a snapshot have rows in embeddings for a version",
    )
    cov_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label (same as embed-works and cluster-works)",
    )
    cov_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    cov_parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Exit with code 1 if any included work is missing an embedding, or (with --cluster-version) any included work lacks a cluster row",
    )
    cov_parser.add_argument(
        "--cluster-version",
        default=None,
        help="Optional cluster artifact label: report included works missing a clusters row (after cluster-works)",
    )
    cov_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
