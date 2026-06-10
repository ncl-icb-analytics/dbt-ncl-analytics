<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0mr4awn0-zgbu-v7-0xey-0sbwohg0uhvx
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: NAFLD Register*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. A patient is included when they match Rule 1.

## Who we start with

Currently registered patients.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `nafld_reg_vs1` (16 codes)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: NAFLD Register* | `nafld_reg_vs1` |  | SNOMED | 16 | Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alco... | f5b3dad9 |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.