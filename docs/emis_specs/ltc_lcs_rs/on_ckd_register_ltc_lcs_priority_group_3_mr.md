<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 17i5r9q0-6a5s-je-01r5-1mv04k71noux
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CKD Register- LTC LCS Priority Group 3 (MR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CKD Register*" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-5.

## Who we start with

1. **LTC LCS: CKD Register*** — Start with currently registered patients. Require Patient Details where Age at least 18 years old. Include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 5

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **On CKD Register- LTC LCS Priority Group 1(HRC)**
- They appear in the results of the search **On CKD Register- LTC LCS Priority Group 2 (HR)**

### Rule 2 of 5

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value > 60
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 30

### Rule 3 of 5

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when ALL of the following are true:
- They match the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1` (see Caveats)
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value > 60
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 3

### Rule 4 of 5

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 45 and <= 59
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 3 and <= 30

### Rule 5 of 5

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 30 and <= 44
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value < 3

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On CKD Register- LTC LCS Priority Group 3 (MR) | `on_ckd_reg_pg3_mr_vs1` |  | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| On CKD Register- LTC LCS Priority Group 3 (MR) | `on_ckd_reg_pg3_mr_vs2` |  | SNOMED | 4 | Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine micr... | d501652f |

## Caveats

- LTC LCS: CKD Register* references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This search references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.