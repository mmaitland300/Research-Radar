# Documentation map

Start with the repo root: [README](../README.md) for what the project is and
how to run it, [ARCHITECTURE](../ARCHITECTURE.md) for system shape, and
[EVALUATION](../EVALUATION.md) for what is and is not validated.

## Reviewer path

- [reviewer-brief.md](reviewer-brief.md) - technical brief for a structured
  review session.
- [release-smoke-checklist.md](release-smoke-checklist.md) - manual smoke
  steps used for the public baseline.
- [releases/v0.1.0-public-baseline.md](releases/v0.1.0-public-baseline.md) -
  the pinned public baseline and its known limits.
- [public-roadmap.md](public-roadmap.md) - high-level plan.

## Operator path

- [bootstrap-run-tutorial.md](bootstrap-run-tutorial.md) - full local
  Postgres bootstrap walkthrough with failure checkpoints.
- [build-brief.md](build-brief.md) - product thesis and intended corpus
  policy (broader than the currently wired sources).
- [candidate-pool-low-cite.md](candidate-pool-low-cite.md) - frozen
  definition of the undercited candidate pool (referenced from code).
- [recommendation-review-rubric.md](recommendation-review-rubric.md) -
  labeling rubric for manual recommendation review.
- [security/](security) - security triage notes.

## Internal notes

[internal/](internal) holds the working planning documents (implementation
roadmap, execution plans, retrieval review worksheets). They are kept for
provenance, use milestone shorthand, and are not maintained as public
documentation.

## Pinned model artifacts

[audit/](audit) contains only the frozen artifacts the deployed scorers load
or pin against. The full historical experiment record lives on the
`archive/ml-governance-audit` branch.
