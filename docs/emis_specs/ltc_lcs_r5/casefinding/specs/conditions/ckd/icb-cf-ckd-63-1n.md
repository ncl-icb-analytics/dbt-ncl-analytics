# ICB_CF_CKD_63_1N

Report title: [ICB_CF_CKD_63_1N] Requires review of CKD coding
Folder: 1) Casefinding R2 > [CF-CKD] CKD Case finding
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CKD_63_1D" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CKD_61_BASE** — Require Patient Details where Age at least 17 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item c913f5a7-1256-4de6-871e-23650e72765e. Include patients who do not match Clinical Codes with Chronic kidney disease stage 5, Chronic kidney disease stage 4, Chronic kidney disease stage 3 +154 more.
   - Combines: **ICS_METABOLIC_LTC**
3. **ICB_CF_CKD_63_1D_woEX** — Exclude patients who match Patients included in search ICB_CF_CKD_61_1D_woEX OR patients included in search ICB_CF_CKD_62_woEX. Include patients who match Clinical Codes with Urine albumin:creatinine ratio where Value > 0 then Latest 1 where numeric value > 70.
   - Combines: **ICB_CF_CKD_61_1D_woEX**; **ICB_CF_CKD_62_woEX**
4. **ICB_CF_CKD_63_1D** — Include patients who do not match Clinical Codes with Chronic kidney disease screening where Date within the last 3 years.
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- They appear in the results of the search **LTC LCS: CKD Register***

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CKD_61_BASE | `eligible_for_ckd_case_finding_vs1` |  |  | SNOMED | 162 | Chronic kidney disease stage 5, Chronic kidney disease stage 4, Chronic kidne... | b134a93a |
| ICB_CF_CKD_63_1D_woEX | `with_most_recent_uacr70_and_not_on_ckd_reg_vs1` |  |  | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |
| ICB_CF_CKD_63_1D | `with_most_recent_uacr70_and_not_on_ckd_regckd_diagnosi_vs1` |  |  | SNOMED | 1 | Chronic kidney disease screening | b8a16fba |

## Caveats

- ICB_CF_CKD_61_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CKD_61_BASE references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.