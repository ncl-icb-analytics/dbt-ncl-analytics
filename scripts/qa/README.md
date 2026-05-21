# QA Tooling

Scripts for capturing and comparing row-count baselines of production dbt models.
Built for the OLIDS 2026 schema migration but reusable for any breaking change.

## Workflow

1. **Capture baseline** before a breaking change:
   ```bash
   python scripts/qa/capture_model_row_counts.py --label pre-olids-2026
   ```
   Writes to `DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_ROW_COUNT_BASELINE` (the
   canonical store) and snapshots `qa/baseline_pre-olids-2026.json` for local
   reference. `qa/baseline_*.json` is gitignored — Snowflake is the source of
   truth.

2. **Apply change**, redeploy to prod (or a comparable env).

3. **Capture post-change baseline**:
   ```bash
   python scripts/qa/capture_model_row_counts.py --label post-olids-2026
   ```

4. **Diff**:
   ```bash
   python scripts/qa/compare_baseline.py --before pre-olids-2026 --after post-olids-2026
   ```

## How counts are obtained

- `BASE TABLE` / materialized: latest `ROW_COUNT_LOG` entry within `--log-window-days`
  (default 7). Falls back to live `COUNT(*)` if no recent entry.
- `VIEW`: always live `COUNT(*)`.

## Connection

Uses `snowflake.connector` with `connection_name="data-platform-manager"` from
`~/.snowflake/connections.toml`. Override with `--connection`.

## Excludes

Schemas matching `INFORMATION_SCHEMA`, `DBT_DEV%`, `DBT_TEST%`, `TEST_AUDIT%`,
`DBT_SNAPSHOTS`, `ELEMENTARY%` are skipped (developer / observability state, not
the model contract).
