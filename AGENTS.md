# Working conventions

Rules for contributors and AI agents working in this repository. They exist
because a June 2026 cleanup removed ~100k lines of accumulated process
scaffolding; these conventions keep it from coming back.

## Repository shape

- `main` is the product: pipeline, API, web app, and curated documentation.
- Historical experiment records live on archive branches (currently
  `archive/ml-governance-audit`), not on `main`.
- New experiment code goes under an `experiments/` namespace or an archive
  branch, never mixed into `services/pipeline/pipeline/` product modules.

## What never gets committed to main

- Generated worksheets, eval dumps, labeling CSVs, run logs, or screenshots.
  Write working data to `artifacts/` (gitignored) and commit only the
  conclusion (a few lines in `EVALUATION.md` or a short doc).
- Frozen artifacts required at runtime are the one exception; they live in
  `docs/audit/` with an entry in its README explaining what loads them.
- Process records: authorization requests, grants, pilot reviews, readiness
  bundles. If a change needs a decision, record the decision and its rationale
  in the PR description or a short doc - not as code, tests, or CI steps.

## Commits and PRs

- Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`,
  `test:`), imperative subject, <= 72 characters.
- Commit outcomes, not ceremony. A request -> grant -> run -> review sequence
  is one commit describing what changed and why.
- All changes to `main` go through a PR with CI green (`npm run validate`).

## Code size and structure

- Flag any file crossing ~1,000 lines in the PR; split it instead of growing
  it. No new god-files: `cli.py`, `apps/api/app/main.py`, and
  `apps/web/app/recommended/page.tsx` are being split, not extended.
- Module names describe what code does, not a process step. If a proposed
  name needs more than ~4 underscores, the design is wrong.
- New CLI subcommands need a one-line justification in the PR; prefer flags on
  existing commands.

## Documentation

- Outward docs: repo root (`README`, `ARCHITECTURE`, `EVALUATION`) and
  `docs/` top level, indexed in `docs/README.md`.
- Internal planning notes: `docs/internal/`, clearly marked as such.
- `EVALUATION.md` stays a short, conclusions-only status guide. Detailed
  evidence goes to archive branches.
- No new milestone codenames (ML1d, ML2-5a, ...) in code, docstrings, or
  outward docs.

## After each work session

Do a short consolidation pass before the PR: delete dead code and unused
helpers the session introduced, merge near-duplicate modules, and confirm
`git status` shows no stray generated files.
