# Project Myria data pipeline

How the Myria (BEVC) models turn raw inputs into the monthly RFL submission, the
control group, and the evaluation cohort. Read this to understand what each model
does and where its data comes from.

## What it produces

- **High-risk patients** — Barnet/Enfield adults with a recent non-elective (NEL)
  admission at Barnet Hospital and a high-risk condition. This is the cohort sent to
  RFL each month for the Doccla virtual-ward offer.
- **Control group** — comparable NCL patients with NEL activity who are *not* in the
  high-risk cohort, for the propensity-matched evaluation.
- **Eligible evaluation cohort** — the patients actually submitted over time, fed to
  the propensity-matching script.

## Two input streams

| Stream | Source | Role |
|---|---|---|
| Secondary-care activity | SUS APC / OP / UEC encounters (`int_sus_*_encounter`) | Derives NEL admissions and ICD-10 condition flags — the bulk of the logic |
| Canonical Myria files | `DATA_LAKE__NCL.MYRIA` (`ENROLLED_PATIENTS`, `MATCHED_PATIENTS`), loaded by `MYRIA.LOAD_NEW_FILES()` | Doccla enrolment status and the returned matched population. One row per patient per file, stamped `FILE_DATE` |

## Lineage

```
SUS encounters ─┐
                │
  int_myria_attendances_diagnoses   (one row per attendance × diagnosis, daily)
                │
  int_myria_conditions              (one row per patient: NEL flags/counts + condition flags)
                ├──────────────► fct_person_myria_high_risk_patients   (Barnet/Enfield, BH NEL, ≥1 condition)
                └──────────────► fct_person_myria_control_group        (other NCL NEL, excludes high-risk)
                                        │
                                  *_published (views)
                                        │
                                  *_published_snapshot (SCD2 daily history)
                                        │
                          ┌─────────────┴──────────────┐
        int_myria_eligible_propensity_match    int_myria_monthly_output
        (eval cohort, fed to matching script)  (monthly QA of who was sent)

DATA_LAKE__NCL.MYRIA ─► raw_myria_* ─► stg_myria_* ─► (enrolled status joined into eval cohort;
                                                       matched population held canonically)
```

## Layer by layer

### Raw / staging — `raw_myria_*`, `stg_myria_*`
1:1 passthrough of the canonical files with cleaned column names. `stg_myria_enrolled_patients`
holds one row per patient per Doccla file; the latest `file_date` is the current enrolment state.

### `int_myria_attendances_diagnoses` (table, `daily`)
Unions inpatient, outpatient and A&E encounters with their diagnoses — one row per
attendance × diagnosis, for activity before the start of the current month. Carries
provider/site, point-of-delivery (`pod`, e.g. `NEL-ZLOS`/`NEL-LOS+1` for non-elective
inpatient), diagnosis code, dates, and `activity_months_ago` for period windowing. This is
the activity backbone for everything downstream.

### `int_myria_conditions` (table, `daily`)
Aggregates to one row per patient. Derives:
- NEL admission flags and counts by provider scope — Barnet Hospital (site `RAL26`), RFL
  (`RAL`/`RAP`), NCL providers, non-NCL — since 1-Jan-2025.
- Rolling NEL counts (6 / 12 / 24 months) for tiering.
- ICD-10 condition flags (heart failure, COPD, dementia, CKD, etc.).
- Current GP/local-authority and death flags, joined from PDS, the practice-neighbourhood
  dimension, and the death registry.

### `fct_person_myria_high_risk_patients` (table, `daily`)
The RFL cohort. Filters `int_myria_conditions` to: ≥1 Barnet Hospital NEL admission, local
authority Barnet or Enfield, age ≥18 at last discharge, alive, and ≥1 high-risk condition.
Sums the condition flags into `total_high_risk_conditions`.

### `fct_person_myria_control_group` (table, `daily`)
Control candidates. Any NCL or non-NCL NEL admission, across all five NCL boroughs, ≥1
condition, age ≥18, alive — and **excludes** anyone already in the high-risk cohort (the two
cohorts are mutually exclusive).

### `*_published` (views) + `*_published_snapshot` (snapshots)
The published views expose the two reporting tables. Each is snapshotted daily (`check`
strategy, key = `patient_id`, `hard_deletes: invalidate`), giving an SCD2 history of who was
in each cohort on any date. The snapshot is what makes "who was sent in month X"
reconstructable after the fact.

### `int_myria_eligible_propensity_match` (table, `myria_eval`)
Builds the evaluation cohort. Reads the high-risk snapshot **as of each monthly send date**
(currently hardcoded `UNION` blocks, ~6,061 patients), keeps each patient's earliest
appearance, then attaches OLIDS/PDS demographics, care-home and activity status, a
pseudonymised `hex_id` (`hxflake_pseudo_generation` macro), and current enrolment from the
latest `stg_myria_enrolled_patients` file. Output feeds the propensity-matching script; the
returned population lands back in `MATCHED_PATIENTS`.

### `int_myria_monthly_output` (view, `myria_eval`)
Monthly QA of the current cohort: reads the live high-risk snapshot version
(`dbt_valid_to is null`) and flags inactive / deceased / palliative-care patients, with
OLIDS and PDS cross-checks. Used to review the list before it goes to RFL.

## Notes for anyone changing this

- **Site vs trust codes.** Activity attaches at *site* level (Barnet Hospital = `RAL26`);
  the trust is `RAL`. `provider_site_name` must resolve site codes against the full
  organisation dictionary, not the trust-only `dict_organisation_nhs_provider` (type 41).
- **The snapshot is the cohort's source of record.** Dev and prod snapshots are separate
  stateful lineages, so validate cohort changes against the prod snapshot, not a dev-vs-prod
  table diff.
- **Send dates aren't recorded.** The eval cohort anchors on hardcoded send dates because
  the actual RFL send (≈2nd Tuesday) is not stored anywhere. Persisting each submission
  would make this data-driven.
