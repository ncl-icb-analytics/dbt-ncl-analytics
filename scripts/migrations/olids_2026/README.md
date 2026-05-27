# OLIDS 2026 Schema Migration

Tooling for the OLIDS Data Store / `DATA_LAKE.OLIDS` schema realignment
(issue #747). Major version bump (1.x → 2.0).

## Files

- `column_renames.yml` — source-of-truth mapping of old → new column names per
  staging model. Hand-curated from issue #747. Includes type-coercion hints
  for the few columns whose type changes (DATE → TIMESTAMP_NTZ, TEXT → BINARY).
- `rewrite_staging.py` — rewrites `models/staging/olids/*.sql` and `.yml` using
  `column_renames.yml`. Idempotent; safe to re-run. Reports unhandled columns.
- `rewrite_downstream.py` — rewrites references to renamed columns in
  `models/{modelling,reporting,published}/**/*.{sql,yml}` and
  `macros/**/*.sql`. Uses alias resolution; emits a report of unresolved
  references for manual review.

## Workflow

1. Review and edit `column_renames.yml` against issue #747.
2. **Commit before running codemod** — provides a clean rollback point.
3. Run `python scripts/migrations/olids_2026/rewrite_staging.py`.
4. Run `python scripts/migrations/olids_2026/rewrite_downstream.py`.
5. Commit codemod output as a separate commit.
6. Run `dbt parse` and `dbt compile --select state:modified+`. Fix any
   remaining manual cases (concept macros, patient_person.id, FLAG removal).
7. After source regeneration (Phase 4), build in dev.
8. Run `python scripts/qa/capture_model_row_counts.py --label post-olids-2026`.
9. Diff against `pre-olids-2026` baseline.
