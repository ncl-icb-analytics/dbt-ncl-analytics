# ICB_CF_CKD_61_1D

Report title: [ICB_CF_CKD_61_1D] With most recent 2 eGFR<60 and not on CKD Register
Folder: 1) Casefinding R2 > [CF-CKD] CKD Case finding
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CKD_61_1D_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CKD_61_BASE** — Require Patient Details where Age at least 17 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item c913f5a7-1256-4de6-871e-23650e72765e. Include patients who do not match Clinical Codes with Chronic kidney disease stage 5, Chronic kidney disease stage 4, Chronic kidney disease stage 3 +154 more.
   - Combines: **ICS_METABOLIC_LTC**
3. **ICB_CF_CKD_61_1D_woEX** — Include patients who match Clinical Codes with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres, eGFR (estimated glomerular filtration rate) using cystatin C Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres +5 more where Value > 0 AND Clinical Codes with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin, eGFR (estimated glomerular filtration rate) using CKD-Epi (Chronic Kidney Disease Epidemiology Collaboration) formula +3 more where Value > 0 then Latest 1 where numeric value < 60.
4. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` (1 code)
  - Where date within the last 3 years — `date > today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CKD_61_BASE | `eligible_for_ckd_case_finding_vs1` |  |  | SNOMED | 162 | Chronic kidney disease stage 5, Chronic kidney disease stage 4, Chronic kidne... | b134a93a |
| ICB_CF_CKD_61_1D_woEX | `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` |  |  | SNOMED | 8 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | da882bb6 |
| ICB_CF_CKD_61_1D_woEX | `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs2` |  |  | SNOMED | 6 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 10267b1d |
| ICB_CF_CKD_61_1D | `with_most_recent_2_egfr60_and_not_on_ckd_reg_vs1` |  | 1 | SNOMED | 1 | Chronic kidney disease screening | b8a16fba |

## Caveats

- ICB_CF_CKD_61_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CKD_61_BASE references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.