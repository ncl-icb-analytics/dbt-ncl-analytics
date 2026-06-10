<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0fd5t6m1-uamh-t6-12xi-1r4hxzd0448k
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 1- Registered with Dentist

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "Type 1 Diabetes*" (see below). A patient is included when they match Rule 1.

## Who we start with

1. **LTC LCS: Diabetes Register*** — Start with currently registered patients. Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
2. **Type 1 Diabetes*** — Start with the patients found by "LTC LCS: Diabetes Register*". Include patients who match Clinical Codes with Refset: 999003371000230102 OR Refset: 999004691000230108 OR Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +79 more then Latest 1.
3. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `1_registered_with_dentist_vs1` (3 codes), or `1_registered_with_dentist_vs2` (1 code)
  - Keep only the latest matching record

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| Type 1 Diabetes* | `type_1_dm_vs1` | DMRES_COD | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| Type 1 Diabetes* | `type_1_dm_vs2` | DM_COD | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| Type 1 Diabetes* | `type_1_dm_vs3` |  | SNOMED | 105 | Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabete... | 10923643 |
| 1- Registered with Dentist | `1_registered_with_dentist_vs1` |  | SNOMED | 3 | Registered with dentist, Patient not registered with dentist, Advised to see ... | 6860b227 |
| 1- Registered with Dentist | `1_registered_with_dentist_vs2` |  | SNOMED | 1 | Registered with dentist | ab6c482a |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.