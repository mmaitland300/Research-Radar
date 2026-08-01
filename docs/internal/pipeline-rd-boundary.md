# Pipeline R&D Boundary Cleanup

This note tracks the cleanup path for the remaining ML/R&D modules in
`services/pipeline/pipeline`.

## Current Shape

- `pipeline.cli` is small and delegates parser registration and dispatch into
  `pipeline.cli_app`.
- Parser registration already separates product commands from legacy ML/R&D
  commands through `product_parsers.py` and `ml_legacy_parsers.py`.
- Dispatch now mirrors that split through `ml_legacy_dispatch.py`.
- The FastAPI app imports pipeline ML code only through bounded serving modules:
  `pipeline.ml_scorer_rollout_serving` and
  `pipeline.ml_bridge_scorer_rollout_serving`.
- The package root still contains many `ml_*.py` modules from offline
  evaluation, label dataset, worksheet, scorer experiment, and rollout-review
  work. This pass found 80 top-level `ml_*.py` modules under
  `services/pipeline/pipeline`.

## Boundary Rules

- Product pipeline code stays under `services/pipeline/pipeline`.
- New experiment code goes under the repo-level `experiments/` namespace or an
  archive branch.
- Existing `ml_*.py` modules should be treated as legacy offline/R&D tooling
  unless a runtime path imports them.
- Runtime scorer serving moves only with API tests and import compatibility.
- Command names should remain stable while modules move.
- Generated worksheets, eval dumps, logs, and run artifacts stay out of `main`.

## Suggested PR Sequence

1. Keep the CLI boundary explicit: product parser/dispatch entrypoints should
   not import each legacy ML command family one by one.
2. Isolate runtime scorer serving helpers from offline experiment modules.
3. Move one low-risk legacy family at a time behind a clearer namespace, keeping
   compatibility shims until tests and CLI dispatch are migrated.
4. Remove compatibility shims only after direct imports have been replaced and
   validation is green.
5. Revisit corpus expansion, labeled evaluation, and scorer work after the
   product-vs-R&D boundary is easier to inspect.

## Non-Goals

- No model retraining or corpus expansion in this cleanup.
- No change to API behavior, ranking output, CLI command names, or generated
  artifacts.
- No broad deletion of historical experiment helpers without a separate archive
  decision.
