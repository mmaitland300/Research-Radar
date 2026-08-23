# Pipeline Package Boundary

`pipeline` is the product package for corpus ingest, embeddings, ranking,
runtime scorer serving helpers, and the command-line tools that operate those
paths.

The package also still contains older `ml_*.py` modules from offline scorer,
labeling, worksheet, and rollout-review work. Treat those modules as legacy
research tooling unless they are explicitly called by runtime serving code.
New experiment code should live under an `experiments/` namespace or on an
archive branch, not as more top-level `pipeline/ml_*.py` files.

Current boundaries:

- Product CLI parsers are registered through `pipeline.cli_app.product_parsers`.
- Legacy ML/R&D CLI parsers are registered through
  `pipeline.cli_app.ml_legacy_parsers`.
- Legacy ML/R&D CLI dispatch is grouped behind
  `pipeline.cli_app.ml_legacy_dispatch`.
- Runtime API scorer serving currently imports only
  `pipeline.ml_scorer_rollout_serving` and
  `pipeline.ml_bridge_scorer_rollout_serving`.

## Public release promotion

`public-release-promote` is the operator boundary for selecting one immutable
ranking run for public serving. Always validate the exact run first:

```bash
python -m pipeline.cli public-release-promote \
  --ranking-run-id rank-... \
  --dry-run
```

Omit `--dry-run` only after the reported snapshot, embedding coverage, family
row counts, and optional clustering artifact are the intended release. The
command never resolves a "latest" run and has no force bypass. Promotions are
append-only; promoting a prior known-good run is the rollback operation.

During the initial `0003` rollout, use this command in `--dry-run` mode only.
The public promotion log is not authoritative until the API serving resolver
is switched to consume it; appending earlier could make metadata disagree
with the still-legacy page defaults.

When moving legacy modules, keep command names and import compatibility until
the dependent tests and API serving path have been migrated deliberately.
