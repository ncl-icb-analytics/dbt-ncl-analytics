# SLAM Data: From Provider File to STAGING

How the four SLAM contract-monitoring feeds (ACM, PLCM, DrPLCM, DePLCM) are transformed between a provider's submission and the tables you query in `STAGING`. Written for analysts who previously cleaned this data themselves — if a step below conflicts with a rule you applied, or misses one, tell us: that is a potential hole in either your old numbers or our new ones, and both are worth knowing about.

## The journey

```
1. Provider file            spreadsheet/CSV, layout varies by provider/month/revision
        │
        ▼
2. WNL_ServicesDataLocal    values stored positionally as Col1..ColN;
   (CSU loader)             column meanings stored separately as a T-SQL string
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

Stages 1–6 are the platform pipeline (Snowflake-Deployment repo, `services_data_local`). Stages 7–8 are this dbt project. The rest of this page covers what each stage does to the data and where the known limitations sit.

## Stage by stage

### 2–3. Positional storage and layout resolution

Provider files do not share a column layout. The loader stores every file's values positionally (`Col1..ColN`) and records what each position meant in a per-file metadata string. The platform pipeline fingerprints each distinct layout and matches every file to one:

- Primary match: the file's header string equals a known layout.
- Fallbacks (for files loaded without headers): same `HeaderID` as a matched sibling file, then same `ProfileCode`. `META_FILE_VERSIONS.MATCH_METHOD` records which path resolved each file.
- No match: the file is excluded and listed in `DATA_LAKE.SDL.META_UNMAPPED_FILES` with a reason.

**Where to look for holes:** files in `META_UNMAPPED_FILES` are invisible downstream. `DATA_LAKE.SDL.META_ROW_COUNT_COMPARE` shows source-vs-loaded coverage per feed (>99.999% at the time of writing). If a provider-month you expect is missing entirely, check these two views first.

**Why this matters to your old queries:** any query written directly against the old positional data, or against a view that assumed one fixed layout, silently misread files from other layouts. Values like specialty names appearing in the financial-year column are the downstream signature of this — column misalignment, not provider typos.

### 4–5. Position-to-name resolution and canonical mapping

The `_raw` views turn positions back into named columns with one CASE branch per layout (the LSPLCM view is ~266 KB of generated SQL — not hand-maintained, regenerated from the metadata). The `_mapped` views then reconcile provider spelling variants into one canonical column (e.g. `ADHOC_ITEM_CODE` and `ADHOCITEM_CODE` arrive as different columns and are COALESCEd) and apply TRY_CAST types.

### 6. DATA_LAKE.SDL tables

Materialised nightly. These are the "permanent home" tables and do minimal cleaning: canonical column names, positions resolved, very sparse columns removed. Values are still provider text — costs with `£` and commas, free-text dates, junk financial years all pass through unchanged. This is deliberate: DATA_LAKE preserves what arrived.

### 7. STAGING tables (dbt)

Grain stays 1:1 with `DATA_LAKE.SDL` — same rows, no joins to other data, no business logic. What changes:

| Cleaning | Rule | Failure behaviour |
|---|---|---|
| `dv_financial_year` | Validated to `'YYYYYY'`; accepts `202526`, `2025/26`, `2025-26`, `2025-2026`; second year must follow the first | Junk (`215551`) and ambiguous bare years (`2020`) → NULL, then the FY token in the platform file name (`PLCM_2627_InformationStandard...`) is used as fallback |
| `dv_financial_month` | Whole number 1 (April) – 12 (March) | Junk (`110`, `400`) and fractional values → NULL |
| `dv_total_cost` and all price/activity/quantity fields | Parsed to `NUMBER(38,6)`: strips currency symbols, thousands commas; accounting-style `(1,234.56)` → negative | Non-numeric text (`TBC`) → NULL |
| `dv_dataset_created_at` | Provider's `DATE_AND_TIME_DATA_SET_CREATED` parsed across every format found in profiling (ISO, UK, US AM/PM, Excel serial numbers, several broken variants) | Unparseable values → NULL |
| `dv_provider_code` | ODS code cleaned via the Dictionary: site-suffixed codes that are not valid org codes resolve to the parent (`RAS00` → `RAS`) | — |
| Activity/clinical dates | Parsed from UK date formats | Unparseable → NULL |
| Column pruning | Columns <5% populated are dropped (LSPLCM: 580 → ~60) | Originals remain in DATA_LAKE.SDL |

The governing rule throughout: **invalid values become NULL, never guesses**, and originals are retained — either in a `*_raw` column alongside the `dv_` field, or in DATA_LAKE.SDL.

What staging deliberately does **not** do: validate code columns (POD codes, service codes, TFC pass through as submitted), join names onto codes, or apply any reporting logic.

### 8. Latest-submission resolution (`_LATEST` views)

SLAM files are cumulative year-to-date restatements (typically 4–6 months per file). Summing the staging tables directly double-counts roughly 8x. The `_LATEST` views fix this.

Two dates are involved, doing different jobs:

- **Which month a row belongs to** comes from the data itself — `dv_financial_year` / `dv_financial_month` as stated inside the file. Arrival timing never assigns periods.
- **Which file wins when two files state the same month** is decided by the platform processing log (`META_FILE_REGISTRY.CREATED_DATETIME` — when ISL loaded the file), with file/batch id tiebreaks.

We deliberately do not use the provider's `DATE_AND_TIME_DATA_SET_CREATED` for ordering: ~10% of files contain more than one value in that field, and same-day resubmissions make it ambiguous. We also do not order by `meta_file_id`: it is not monotonic with load time (~45% of consecutive loads have inverted ids, mainly from back-dated files matched by the cascade).

Resolution is **per reporting month, not per file**. For each provider and financial year, every stated month independently selects the most recently loaded file containing data for that month. Consequences:

- A full-year restatement supersedes all months it states.
- A submission split across files (M1–6 + M7–12) keeps both halves.
- A single-month correction replaces only that month.

The winning file per slice is published in `STAGING.SLAM.STG_SLAM_LATEST_SUBMISSION` (feed × provider × FY × month → file). If you previously maintained your own list of "correct" FileIDs for a period, compare it against this table — disagreement in either direction is worth reporting.

## Known limitations

Stated here so they are findable, not discovered:

1. **~0.4% of rows have NULL `dv_financial_year`/`month`** (Drugs feed figure) — values that were genuinely unrecoverable. These rows fall out of month-level reporting; their raw values remain in `financial_year_raw` / `financial_month_raw`.
2. **74 cost values (of 51.6m populated, Drugs) do not parse** — true junk, NULL in `dv_total_cost`.
3. **Backloaded history ordering**: for ~340 provider-months (none in 26/27, concentrated 20/21–23/24), load order and the provider-stated creation date disagree about which file is latest. The views follow load order. If you analyse restatement history in those years, be aware.
4. **Upstream rebuilds rewrite history**: if the platform pipeline rebuilds a feed (schema drift), the staging tables are rebuilt from it; figures can change. The nightly build's tests flag grain breaks if this goes wrong.
5. **Unmapped files**: anything in `META_UNMAPPED_FILES` is absent from all downstream layers until mapped.

## Verifying it yourself

Every claim above is checkable. Useful starting points:

```sql
-- parse coverage: how many source cost values survive cleaning?
select count(t.total_cost) as src, count(s.dv_total_cost) as cleaned
from DATA_LAKE.SDL.LSDRPLCM t
join STAGING.LSDRPLCM.STG_LSDRPLCM s on s.meta_sk_row_id = t.meta_sk_row_id;

-- your old file-id list vs the lookup
select meta_file_id from STAGING.SLAM.STG_SLAM_LATEST_SUBMISSION
where feed = 'LSACM' and dv_financial_year = '202526' and dv_financial_month = 12;

-- rows excluded from month-level reporting (unrecoverable periods)
select financial_year_raw, count(*) from STAGING.LSDRPLCM.STG_LSDRPLCM
where dv_financial_year is null group by 1 order by 2 desc;

-- restatement collapse: history vs latest
select (select count(*) from STAGING.LSDRPLCM.STG_LSDRPLCM) as history_rows,
       (select count(*) from STAGING.LSDRPLCM.STG_LSDRPLCM_LATEST) as latest_rows;
```

Every column also carries a description in Snowsight stating what it was parsed from and how failures behave. The cleaning code itself is in `macros/transformations/parse_slam.sql` and `models/staging/commissioning/ls*/`.

If you find a value the cleaning handles wrongly, or a rule you used to apply that we don't: raise it. The point of centralising this is that a fix lands once, for everyone, with a test.
