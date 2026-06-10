<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1d29hsx0-qy4o-ta-0vhd-1a47bw10zc18
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CHD Register- LTC LCS Priority Group 1 (HRC)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CHD Register*" (see below). A patient is included when they match Rule 1.

## Who we start with

1. **LTC LCS: CHD Register*** — Start with currently registered patients. Include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `on_chd_reg_pg1_hrc_vs1` (1 code — cluster CHD_COD)
  - Where date within the last 1 month
  - Where episode type is First or New or Flare Up
- **Clinical Codes** (clinical events)
  - Code in: `on_chd_reg_pg1_hrc_vs1` (1 code — cluster CHD_COD)
  - Where date within the last 1 month
  - Where problem significance is Significant

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On CHD Register- LTC LCS Priority Group 1 (HRC) | `on_chd_reg_pg1_hrc_vs1` | CHD_COD | SNOMED | 1 | Refset: 999000771000230107 | d908caa0 |
| On CHD Register- LTC LCS Priority Group 1 (HRC) | `on_chd_reg_pg1_hrc_vs2` |  | Internal | 3 | First, New, Flare Up | bd7fde07 |
| On CHD Register- LTC LCS Priority Group 1 (HRC) | `on_chd_reg_pg1_hrc_vs3` |  | Internal | 1 | Significant | 8de0b3c4 |

## Caveats

- LTC LCS: CHD Register* references the EMIS library item `d730ee6f-1b38-4553-8f8e-7dc8b3042f4c`, whose logic is not included in this XML export. It is likely **CHD Register** (inferred from wrapper report "LTC LCS: CHD Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.