<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 17i5r9q0-6a5s-je-01r5-1mv04k71noux
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CKD Register- LTC LCS Priority Group 3 (MR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CKD Register*" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-5. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: CKD Register*** — Require Patient Details where Age at least 18 years old. Include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 5 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **On CKD Register- LTC LCS Priority Group 1(HRC)**
- They appear in the results of the search **On CKD Register- LTC LCS Priority Group 2 (HR)**

### Rule 2 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value > 60
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 30

### Rule 3 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when **ALL (AND)** of the following are true:

- They match the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1` (see Caveats)
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value > 60
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 3

### Rule 4 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 45 and <= 59
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 3 and <= 30

### Rule 5 of 5 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 30 and <= 44
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value < 3

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On CKD Register- LTC LCS Priority Group 3 (MR) | `on_ckd_reg_pg3_mr_vs1` |  | 2, 3, 4, 5 | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| On CKD Register- LTC LCS Priority Group 3 (MR) | `on_ckd_reg_pg3_mr_vs2` |  | 2, 3, 4, 5 | SNOMED | 4 | Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine micr... | d501652f |

## Caveats

- LTC LCS: CKD Register* references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This search references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.