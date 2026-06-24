# ICB_CF_CKD_64_1D

Report title: [ICB_CF_CKD_64_1D] At risk of CKD who are not on CKD register
Folder: 1) Casefinding R2 > [CF-CKD] CKD Case finding
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CKD_64_1D_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CKD_64_BASE** — Require Patient Details where Age at least 17 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item c913f5a7-1256-4de6-871e-23650e72765e. Include patients who do not match Clinical Codes with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin, eGFR (estimated glomerular filtration rate) using CKD-Epi (Chronic Kidney Disease Epidemiology Collaboration) formula +4 more where Value > 0 AND Date within the last 1 year OR Clinical Codes with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin, eGFR (estimated glomerular filtration rate) using CKD-Epi (Chronic Kidney Disease Epidemiology Collaboration) formula +4 more where Date within the last 1 year.
   - Combines: **ICS_METABOLIC_LTC**
3. **ICB_CF_CKD_64_1D_woEX** — Exclude patients who match Patients included in search ICB_CF_CKD_61_1D_woEX OR patients included in search ICB_CF_CKD_62_woEX OR patients included in search ICB_CF_CKD_63_1D_woEX. Include patients who match any of: Clinical Codes with History of acute kidney injury, Acute kidney injury stage 1, Acute kidney injury stage 2 +5 more where Date within the last 3 years OR Clinical Codes with BPH - benign prostatic hypertrophy, Gout, Systemic lupus erythematosus +25 more; OR Medication Issues with Lithium Salts OR Lithium Carbonate, Lithium Citrate OR Lithium Carbonate (Essential Pharma) OR Sulfasalazine, Tacrolimus where Date of Issue within the last 6 months; OR Clinical Codes with Microscopic haematuria, Recurrent microscopic haematuria, Persistent microscopic haematuria +2 more then Latest 1 OR Clinical Codes with Microscopic haematuria, Recurrent microscopic haematuria, Persistent microscopic haematuria +1 more then Latest 1.
   - Combines: **ICB_CF_CKD_61_1D_woEX**; **ICB_CF_CKD_62_woEX**; **ICB_CF_CKD_63_1D_woEX**
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
  - Code in: `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` (1 code)
  - Where date within the last 3 years — `date > today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CKD_64_BASE | `ckd_case_finding_vs1` |  |  | SNOMED | 7 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | af82e157 |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` |  |  | SNOMED | 11 | History of acute kidney injury, Acute kidney injury stage 1, Acute kidney inj... | 0e38ef78 |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs2` |  |  | SNOMED | 28 | BPH - benign prostatic hypertrophy, Gout, Systemic lupus erythematosus +25 more | 5570106b |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs3` |  |  | Drug Group | 1 | Lithium Salts | 49b0e30d |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs4` |  |  | SCT Const | 2 | Lithium Carbonate, Lithium Citrate | 5cdef610 |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs5` |  |  | Brand | 1 | Lithium Carbonate (Essential Pharma) | f3e325e6 |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs6` |  |  | SCT Const | 2 | Sulfasalazine, Tacrolimus | c46c099f |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs7` |  |  | SNOMED | 5 | Microscopic haematuria, Recurrent microscopic haematuria, Persistent microsco... | 77728758 |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs8` |  |  | SNOMED | 2 | Urine blood test = negative, Urine microscopy: red cells | fc68d5bb |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs9` |  |  | SNOMED | 4 | Microscopic haematuria, Recurrent microscopic haematuria, Persistent microsco... | 86edb9ea |
| ICB_CF_CKD_64_1D_woEX | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs10` |  |  | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |
| ICB_CF_CKD_64_1D | `at_risk_of_ckd_who_are_not_on_ckd_reg_vs1` |  | 1 | SNOMED | 1 | Chronic kidney disease screening | b8a16fba |

## Caveats

- ICB_CF_CKD_64_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CKD_64_BASE references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.