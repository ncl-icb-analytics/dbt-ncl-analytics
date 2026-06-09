<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0futo7m1-3awg-vc-16vr-09763rw1vqi4
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 0- NO record of Dentist info

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "on Diabetes Register- LTC LCS Priority Group 4 (LR)" (see below). Rule 1 includes only patients who do NOT match it.

## Who we start with

1. **LTC LCS: Diabetes Register*** — Start with currently registered patients. Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
2. **on Diabetes Register- LTC LCS Priority Group 4 (LR)** — Start with the patients found by "LTC LCS: Diabetes Register*". Finally include patients who do not match Patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb).
3. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `0_no_record_of_dentist_info_vs1` (3 codes)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| 0- NO record of Dentist info | `0_no_record_of_dentist_info_vs1` |  | SNOMED | 3 | Registered with dentist, Patient not registered with dentist, Advised to see ... | 6860b227 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.