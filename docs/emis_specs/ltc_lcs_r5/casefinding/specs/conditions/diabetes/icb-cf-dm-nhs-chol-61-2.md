# ICB_CF_DM_NHS_CHOL_61_2

Report title: [ICB_CF_DM_NHS_CHOL_61_2] Requires Cholesterol check
Folder: 1) Casefinding R2 > [CF-DM] High risk Diabetes
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_DM_NHS_61_2D" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_DM_61_BASE** — Require Patient Details where Age at least 17 years old. Include patients who do not match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_DM_61_woEX** — Exclude patients who match Clinical Codes with Diabetes resolved, Diabetes mellitus in remission, Type I diabetes mellitus in remission +4 more OR Diabetes resolved, Diabetes mellitus in remission, Type I diabetes mellitus in remission +1 more then Latest 1 OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1. Include patients who match Clinical Codes with Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised, HbA1c (haemoglobin A1c) level (monitoring ranges) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised, HbA1c (haemoglobin A1c) level (diagnostic reference range) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised where Value > 0 then Latest 1 where numeric value >= 48.
4. **ICB_CF_DM_61** — Include patients who do not match Clinical Codes with High risk of diabetes mellitus annual review declined where Date within the last 3 years.
5. **ICB_CF_DM_NHS_61_2D** — Include patients who do not match Patients included in search NHSHC_ELIGIBLE.
   - Combines: **NHSHC_ELIGIBLE**
6. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `requires_cholesterol_check_vs1` (1 code)
  - Where date within the last 3 years — `date >= today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_DM_61_woEX | `latest_hba1c_48_and_no_diagnosis_of_dm_vs1` |  |  | SNOMED | 7 | Diabetes resolved, Diabetes mellitus in remission, Type I diabetes mellitus i... | 18df1b4c |
| ICB_CF_DM_61_woEX | `latest_hba1c_48_and_no_diagnosis_of_dm_vs2` |  |  | SNOMED | 4 | Diabetes resolved, Diabetes mellitus in remission, Type I diabetes mellitus i... | 3ad987ee |
| ICB_CF_DM_61_woEX | `latest_hba1c_48_and_no_diagnosis_of_dm_vs3` |  |  | SNOMED | 3 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 95d9e41a |
| ICB_CF_DM_61 | `latest_hba1c_48_and_no_diagnosis_of_dm_vs1` |  |  | SNOMED | 1 | High risk of diabetes mellitus annual review declined | 29adf77b |
| ICB_CF_DM_NHS_CHOL_61_2 | `requires_cholesterol_check_vs1` |  | 1 | SNOMED | 1 | Serum cholesterol level | 474af1a3 |

## Caveats

- ICB_CF_DM_61_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_DM_61_woEX references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.