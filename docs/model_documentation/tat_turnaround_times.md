# Imaging Turnaround Times (TAT) — object reference

Quick reference of every object in the imaging diagnostic Turnaround Times pipeline, for
review. Provider submissions are ingested into Snowflake by the `tat_provider_ingest` pipeline
(Snowflake-Deployment repo) and modelled in dbt here.

Lineage: **provider files → `DATA_LAKE.TAT` (raw) → dbt raw → staging → modelling**.

PRs: Snowflake-Deployment #23 (ingest) · dbt-analytics #818 (modelling).

## 1. Ingest objects — `DATA_LAKE.TAT` (live now)

Created by `sql/01_setup.sql` + `sql/03_procs.sql` in the `tat_provider_ingest` pipeline.

| Object | FQN | Type | Purpose |
|---|---|---|---|
| Raw landing table | `DATA_LAKE.TAT.TURNAROUND_TIMES_RAW` | TABLE | All-STRING 1:1 landing of provider submissions (148 files, ~9.43M rows) |
| Ingest log | `DATA_LAKE.TAT.TAT_INGEST_LOG` | TABLE | Per-file load audit (status, rows, errors, who) |
| Stage | `DATA_LAKE.TAT.TAT_SUBMISSIONS` | STAGE | Transient upload pipe (`incoming/`, cleared after load) |
| File format | `DATA_LAKE.TAT.TAT_CSV` | FILE FORMAT | CSV parse (header, BOM, NA sentinels) |
| xlsx converter | `DATA_LAKE.TAT.SP_TAT_CONVERT_XLSX()` | PROCEDURE (Python) | Converts staged `.xlsx` → `.csv` |
| Raw loader | `DATA_LAKE.TAT.SP_TAT_LOAD_RAW()` | PROCEDURE (SQL) | `COPY INTO` the raw table + write ingest log |
| Datetime parser | `DATA_LAKE.TAT.PARSE_TAT_TS(VARCHAR)` | FUNCTION | UK `DD/MM/YYYY HH:MI` (+ ISO) → `TIMESTAMP_NTZ` |

## 2. dbt models

Names are case-insensitive in Snowflake. Prod objects exist once the dbt PR is merged + run;
the dev objects are built now (from validation runs).

| Model | Layer | Prod FQN | Dev FQN |
|---|---|---|---|
| `raw_tat_turnaround_times_raw` | raw (view) | `STAGING.DBT_RAW.RAW_TAT_TURNAROUND_TIMES_RAW` | `DEV__STAGING.DBT_RAW.RAW_TAT_TURNAROUND_TIMES_RAW` |
| `stg_tat_turnaround_times` | staging (table) | `STAGING.TAT.STG_TAT_TURNAROUND_TIMES` | `DEV__STAGING.TAT.STG_TAT_TURNAROUND_TIMES` |
| `int_tat_turnaround_times` | modelling (table) | `MODELLING.COMMISSIONING_MODELLING.INT_TAT_TURNAROUND_TIMES` | `DEV__MODELLING.COMMISSIONING_MODELLING.INT_TAT_TURNAROUND_TIMES` |

- **raw** — 1:1 passthrough, cleaned column names (generated).
- **staging** — typed + normalised: header-spelling variants COALESCEd, UK datetimes parsed,
  trust/period derived from filename. Grain = `tat_event_id` (exact re-loads de-duplicated).
- **modelling** — TAT hours, Flex/Freeze classification (out-of-range dropped), cancer flag
  standardised, restated to the latest submission per trust + data period. **Analyst-facing
  table replacing `DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW`.**

## 3. Tests

| Test | Model | Column |
|---|---|---|
| `unique`, `not_null` | both stg + int | `tat_event_id` (grain) |
| `accepted_values` (`Freeze`, `Flex`) | int | `data_type` |
| row count ≥ 1 | both stg + int | — |

## 4. What to sense-check

- **Counts** (dev, latest run): raw ~9.43M → stg 9,415,704 → int 9,235,786 (Flex 6.13M / Freeze 3.11M), all 7 trusts.
- **Datetime parsing** — providers send UK `DD/MM/YYYY HH:MI`; xlsx-converted files arrive ISO. ~99.8% of test datetimes parse; the rest are genuinely missing/blank.
- **Flex/Freeze** — `datedifftest` = whole months between the file's data period and the test month: `-3` (or one hardcoded file) → Freeze, `-2/-1` → Flex, else dropped.
- **Restatement** — only the latest submission per (trust, data period) survives in the modelling layer.
- **Known**: 3 historical NMUH files had blank header columns (cleaned on load); 4 xlsx months also had a same-named `.csv` (de-duplicated as same trust+period).
