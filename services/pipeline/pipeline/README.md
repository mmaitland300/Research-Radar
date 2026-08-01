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

When moving legacy modules, keep command names and import compatibility until
the dependent tests and API serving path have been migrated deliberately.
