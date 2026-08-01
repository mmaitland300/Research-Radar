# Experiments

Use this namespace for new offline experiment code that is not part of the
product pipeline, API, or web app.

Keep generated worksheets, eval dumps, logs, and local run artifacts out of this
directory. Write those to `artifacts/` and commit only concise conclusions or
runtime-required frozen artifacts through the documented review path.

Existing historical experiment records remain on archive branches. Existing
legacy `pipeline/ml_*.py` modules should move here only through narrow PRs that
preserve CLI behavior and import compatibility.
