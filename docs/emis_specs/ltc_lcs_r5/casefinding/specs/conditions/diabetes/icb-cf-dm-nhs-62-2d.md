# ICB_CF_DM_NHS_62_2D

Report title: [ICB_CF_DM_NHS_62_2D] Not Eligible for NHS health check
Folder: 1) Casefinding R2 > [CF-DM] High risk Diabetes
Source: NCL LTC LCS R5.0 updated: 27112025
Description: If not eligible for NHS Health Check, offer blood test (HbA1c & Chol), record BMI, BP & smoking status and offer lifestyle advice

## What this search does

Start with the patients found by "ICB_CF_DM_62" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_DM_62_BASE** — Require Patient Details where Age at least 17 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1. Include patients who match Clinical Codes with Gestational diabetes mellitus, H/O gestational diabetes mellitus, Gestational diabetes mellitus uncontrolled +2 more.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_DM_62_woEX** — Exclude patients who match Patients included in search ICB_CF_DM_61_woEX. Include patients who do not match Clinical Codes with Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised, HbA1c (haemoglobin A1c) level (monitoring ranges) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised, HbA1c (haemoglobin A1c) level (diagnostic reference range) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised where Date within the last 1 year.
   - Combines: **ICB_CF_DM_61_woEX**
4. **ICB_CF_DM_62** — Include patients who do not match Clinical Codes with High risk of diabetes mellitus annual review declined where Date within the last 3 years.
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- They appear in the results of the search **NHSHC_ELIGIBLE**

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_DM_62_BASE | `patients_on_gestational_dm_vs1` |  |  | SNOMED | 5 | Gestational diabetes mellitus, H/O gestational diabetes mellitus, Gestational... | f9a800d3 |
| ICB_CF_DM_62_woEX | `gestational_dm_and_no_hba1c_in_the_last_year_vs1` |  |  | SNOMED | 3 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 95d9e41a |
| ICB_CF_DM_62 | `gestational_dm_and_no_hba1c_in_the_last_year_vs1` |  |  | SNOMED | 1 | High risk of diabetes mellitus annual review declined | 29adf77b |

## Caveats

- ICB_CF_DM_62_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.