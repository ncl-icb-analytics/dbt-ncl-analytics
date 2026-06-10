<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0944jzf1-v7rx-ar-11dz-1l2zyel0ucmw
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 3- Advised to see dentist

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "on Diabetes Register- LTC LCS Priority Group 3 (MR)" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **LTC LCS: Diabetes Register*** — Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
3. **on Diabetes Register- LTC LCS Priority Group 3 (MR)** — Exclude patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR). Include patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa).
   - Combines: **on Diabetes Register- LTC LCS Priority Group 1 (HRC)**; **on Diabetes Register- LTC LCS Priority Group 2 (HR)**; **on Diabetes Register- LTC LCS Priority Group 3B (MRb)**; **on Diabetes Register- LTC LCS Priority Group 3A (MRa)**
4. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 1 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `3_advised_to_see_dentist_vs1` (3 codes), or `3_advised_to_see_dentist_vs2` (1 code)
  - Keep only the latest matching record

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| 3- Advised to see dentist | `3_advised_to_see_dentist_vs1` |  | 1 | SNOMED | 3 | Registered with dentist, Patient not registered with dentist, Advised to see ... | 6860b227 |
| 3- Advised to see dentist | `3_advised_to_see_dentist_vs2` |  | 1 | SNOMED | 1 | Advised to see dentist | b16dccb6 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.