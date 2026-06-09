<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1x0ilqd1-i68v-m9-1b36-0bhi4600hdw2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: Diabetes Register*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. Patients must match Rule 1 to stay in. A patient is included when they match Rule 2.

## Who we start with

Currently registered patients.

## Inclusion logic, step by step

### Rule 1 of 2

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:
- **Patient Details**
  - Where age at least 17 years old

### Rule 2 of 2

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `dm_reg_vs1` (1 code — cluster DM_COD)
  - Keep only the latest matching record
  - Must also have a linked record (its date after the date of the record above):
    - **Clinical Codes** (clinical events) — patient must NOT have a matching record
      - Code in: `dm_reg_vs2` (1 code — cluster DMRES_COD)
      - Keep only the latest matching record

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.