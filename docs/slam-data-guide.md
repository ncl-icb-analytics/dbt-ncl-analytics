# SLAM Data: Source-to-Staging Methodology

How the four SLAM contract-monitoring feeds (ACM, PLCM, DrPLCM, DePLCM) are transformed between a provider's submission and the tables in `STAGING`. Each stage states the problem it addresses and the method applied.

## The pipeline

```
1. Provider file            spreadsheet/CSV, layout varies by provider/month/revision
        │
        ▼
2. WNL_ServicesDataLocal    values stored positionally as Col1..ColN;
   (ISL loader)             column meanings stored separately as a T-SQL string
        │                   per file in Log_ProcessingEvent.SourceColumnHeaders
        ▼
3. Layout resolution        every distinct layout fingerprinted (md5) into
   (DATA_LAKE.SDL pipeline) META_SCHEMA_VERSIONS; each file matched to its layout
        │                   (LSPLCM alone: 101 layouts across 18,583 files)
        ▼
4. <feed>_raw views         positions resolved to named columns -
        │                   one CASE branch per layout per column
        ▼
5. <feed>_mapped views      canonical column names (COALESCE across provider
        │                   spelling variants) + TRY_CAST data types
        ▼
6. DATA_LAKE.SDL.<FEED>     materialised tables, loaded nightly, with the META_*
        │                   tables tracking files, exceptions and row coverage
        ▼
7. STAGING.<FEED>.STG_*     dbt staging: cleaned dv_ fields, sparse columns
        │                   dropped, tested on every refresh
        ▼
8. STG_*_LATEST views       current provider statement per (provider, FY, month) -
                            the reporting entry points
```

Stages 1–6 are the platform pipeline (Snowflake-Deployment repo, `services_data_local`). Stages 7–8 are this dbt project.

## 2–3. Positional storage and layout resolution

**Problem.** Provider files do not share a column layout. The ISL loader stores every file's values positionally (`Col1..ColN`) in `Master_<feed>_Data`; the table carries no column meanings. What each position meant is recorded per file in `Log_ProcessingEvent.SourceColumnHeaders`, as the T-SQL select fragment the loader used:

```
[Col1] AS [FinancialMonth], [Col2] AS [CCG], [Col3] AS [Provider Code], ...
```

That string is the only record of each file's schema.

**Method.**

1. Parse each `SourceColumnHeaders` string into (position → column name) pairs.
2. Fingerprint the full mapping with an md5 hash — every distinct layout becomes one `version_id` in `META_SCHEMA_VERSIONS`. LSPLCM has 101; the four SLAM feeds together have 236.
3. Match every file to a version in `META_FILE_VERSIONS`:
   - Primary: the file's header string equals a known layout, byte for byte.
   - Fallbacks, for files whose log row has no headers: same `HeaderID` as a matched sibling file, then same `ProfileCode` (same layout family). `MATCH_METHOD` records which path resolved each file. Cascade-matched files can be loaded long after newer ones, which is why file ids are not in load order.
   - No match: the file is excluded and listed in `DATA_LAKE.SDL.META_UNMAPPED_FILES` with a reason.

**Coverage.** At the time of writing, 3 SLAM files (of ~50,000 loaded) are unmapped — all `data_not_logged`, where the log row exists but the data rows never arrived upstream. New files appear briefly as `not_yet_refreshed` until the nightly refresh. `DATA_LAKE.SDL.META_ROW_COUNT_COMPARE` shows source-vs-loaded row coverage per feed (>99.999%).

## 4–5. Position-to-name resolution and canonical mapping

The `<feed>_raw` views make the recovery executable. Each row joins to its file's `version_id`, and every output column is a CASE over versions stating where that column lives in each layout. From the LSPLCM view:

```sql
CASE
    WHEN fv."VERSION_ID" IN ('72a5f19843d37d2d') THEN d."Col49"
    WHEN fv."VERSION_ID" IN ('5dca51a88420d9a7') THEN d."Col32"
    WHEN fv."VERSION_ID" IN ('150ecfb64c560215') THEN d."Col46"
END AS activity_actual
```

The same column arrived in position 49, 32 or 46 depending on the layout. Rows from layouts that never contained the column are NULL. The full LSPLCM view is ~580 columns and 944 CASE expressions, generated from the metadata and regenerated whenever a new layout appears.

The `<feed>_mapped` views reconcile naming drift: where providers spelled the same concept differently across layouts (`ADHOC_ITEM_CODE` vs `ADHOCITEM_CODE`), a curated mapping CSV declares the canonical name and the view COALESCEs the variants into it, applying TRY_CAST data types where declared.

## 6. DATA_LAKE.SDL tables

Materialised nightly from the `_mapped` views. Minimal cleaning by design: canonical column names, positions resolved, very sparse columns removed. Values remain provider text — costs with `£` and commas, free-text dates and financial years pass through unchanged. DATA_LAKE preserves what arrived.

## 7. STAGING tables (dbt)

Grain stays 1:1 with `DATA_LAKE.SDL` — same rows, no joins to other data, no business logic.

| Field | Method | Failure behaviour |
|---|---|---|
| `dv_financial_year` | Validated to `'YYYYYY'`; accepts `202526`, `2025/26`, `2025-26`, `2025-2026`; second year must follow the first | Junk (`215551`) and ambiguous bare years (`2020`) → NULL, then the FY token in the platform file name (`PLCM_2627_InformationStandard...`) is used as fallback |
| `dv_financial_month` | Whole number 1 (April) – 12 (March) | Junk and fractional values → NULL |
| `dv_total_cost` and all price/activity/quantity fields | Parsed to `NUMBER(38,6)`: strips currency symbols and thousands commas; accounting-style `(1,234.56)` → negative | Non-numeric text (`TBC`) → NULL |
| `dv_dataset_created_at` | Provider's `DATE_AND_TIME_DATA_SET_CREATED` parsed across every format found in profiling (ISO, UK, US AM/PM, Excel serial numbers, several broken variants) | Unparseable values → NULL |
| `dv_provider_code` | ODS code cleaned via the Dictionary: site-suffixed codes that are not valid org codes resolve to the parent (`RAS00` → `RAS`) | — |
| Activity/clinical dates | Parsed from UK date formats | Unparseable → NULL |
| Column pruning | Columns <5% populated dropped (LSPLCM: 580 → ~60) | Originals remain in DATA_LAKE.SDL |

The governing rule: invalid values become NULL, never guesses, and originals are retained — in a `*_raw` column alongside the `dv_` field, or in DATA_LAKE.SDL.

Staging does not validate code columns (POD, service, TFC pass through as submitted), join names onto codes, or apply reporting logic.

## 8. Latest-submission resolution (`_LATEST` views)

**Problem.** SLAM files are cumulative year-to-date restatements (typically 4–6 months per file), with no marker for which submission is current. Summing the staging tables directly double-counts roughly 8x. The provider-populated `DATE_AND_TIME_DATA_SET_CREATED` cannot order submissions reliably: ~10% of files contain more than one value in the field, and same-day resubmissions are ambiguous. `meta_file_id` is not monotonic with load time (~45% of consecutive loads have inverted ids, mainly cascade-matched back-dated files).

**Method.** Two dates do different jobs:

- **Which month a row belongs to** comes from the data itself — `dv_financial_year` / `dv_financial_month` as stated inside the file. Arrival timing never assigns periods.
- **Which file wins when two files state the same month** is decided by the platform processing log (`META_FILE_REGISTRY.CREATED_DATETIME` — when ISL loaded the file), with file/batch id tiebreaks.

Resolution is per reporting month, not per file: for each provider and financial year, every stated month independently selects the most recently loaded file containing data for that month. Consequences:

- A full-year restatement supersedes all months it states.
- A submission split across files (M1–6 + M7–12) keeps both halves.
- A single-month correction replaces only that month.

The winning file per slice is published in `STAGING.SLAM.STG_SLAM_LATEST_SUBMISSION` (feed × provider × FY × month → file). The `_LATEST` views join the staging tables to it.

## Known limitations

1. **~0.4% of rows have NULL `dv_financial_year`/`month`** (Drugs feed figure) — genuinely unrecoverable values. These rows fall out of month-level reporting; raw values remain in `financial_year_raw` / `financial_month_raw`.
2. **74 cost values (of 51.6m populated, Drugs) do not parse** — true junk, NULL in `dv_total_cost`.
3. **Backloaded history ordering**: for ~340 provider-months (none in 26/27, concentrated 20/21–23/24), load order and the provider-stated creation date disagree about which file is latest. The views follow load order.
4. **Upstream rebuilds rewrite history**: if the platform pipeline rebuilds a feed (schema drift), the staging tables are rebuilt from it and figures can change. Nightly tests flag grain breaks.
5. **Unmapped files**: anything in `META_UNMAPPED_FILES` is absent from all downstream layers. Currently 3 SLAM files, all missing their data upstream rather than awaiting mapping.

## Verification queries

```sql
-- parse coverage: source cost values vs cleaned
select count(t.total_cost) as src, count(s.dv_total_cost) as cleaned
from DATA_LAKE.SDL.LSDRPLCM t
join STAGING.LSDRPLCM.STG_LSDRPLCM s on s.meta_sk_row_id = t.meta_sk_row_id;

-- winning files for a period
select meta_file_id from STAGING.SLAM.STG_SLAM_LATEST_SUBMISSION
where feed = 'LSACM' and dv_financial_year = '202526' and dv_financial_month = 12;

-- rows excluded from month-level reporting (unrecoverable periods)
select financial_year_raw, count(*) from STAGING.LSDRPLCM.STG_LSDRPLCM
where dv_financial_year is null group by 1 order by 2 desc;

-- restatement collapse: history vs latest
select (select count(*) from STAGING.LSDRPLCM.STG_LSDRPLCM) as history_rows,
       (select count(*) from STAGING.LSDRPLCM.STG_LSDRPLCM_LATEST) as latest_rows;
```

Column descriptions in Snowsight state what each derived field was parsed from and how failures behave. The cleaning code is in `macros/transformations/parse_slam.sql` and `models/staging/commissioning/ls*/`.
