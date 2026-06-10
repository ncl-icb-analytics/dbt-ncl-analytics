<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1cwk8q70-lcaf-77-0rn6-17imjci14mrx
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CKD Register- LTC LCS Priority Group 1(HRC)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CKD Register*" (see below). A patient is included when they match any one of Rules 1-2. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: CKD Register*** — Require Patient Details where Age at least 18 years old. Include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Continue to Rule 2 | Inclusion route |
| 2 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 2 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg1_hrc_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value < 15

### Rule 2 of 2 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg1_hrc_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 250

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On CKD Register- LTC LCS Priority Group 1(HRC) | `on_ckd_reg_pg1_hrc_vs1` |  | 1 | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| On CKD Register- LTC LCS Priority Group 1(HRC) | `on_ckd_reg_pg1_hrc_vs2` |  | 2 | SNOMED | 4 | Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine micr... | d501652f |

## Caveats

- LTC LCS: CKD Register* references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.