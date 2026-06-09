<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1d29hsx0-qy4o-ta-0vhd-1a47bw10zc18
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On CHD Register- LTC LCS Priority Group 1 (HRC)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On CHD Register- LTC LCS Priority Group 1 (HRC)
Parent population: Based on "LTC LCS: CHD Register*" search results

## Parent Chain
- LTC LCS: CHD Register*: Start with currently registered patients. Finally include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).
  Library refs: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)

## Library Items
- LTC LCS: CHD Register*: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c); wrapper reports: LTC LCS: CHD Register*

## Target Report Logic
Start with based on "ltc lcs: chd register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999000771000230107 OR First, New, Flare Up where Date within the last 1 month AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999000771000230107 OR Significant where Date within the last 1 month AND Problem Significance = Significant.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999000771000230107 OR First, New, Flare Up where Date within the last 1 month AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999000771000230107 OR Significant where Date within the last 1 month AND Problem Significance = Significant)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Refset: 999000771000230107 OR First, New, Flare Up where Date within the last 1 month AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999000771000230107 OR Significant where Date within the last 1 month AND Problem Significance = Significant
- Clinical Codes [EVENTS]
  - ValueSets: `on_chd_reg_pg1_hrc_vs1`, `on_chd_reg_pg1_hrc_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_chd_reg_pg1_hrc_vs1`
  - Filter: Date IN within the last 1 month
    - From: within the last 1 month
  - Filter: Episode (First, New...)
    - Filter ValueSets: `on_chd_reg_pg1_hrc_vs2`
- Clinical Codes [EVENTS]
  - ValueSets: `on_chd_reg_pg1_hrc_vs1`, `on_chd_reg_pg1_hrc_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_chd_reg_pg1_hrc_vs1`
  - Filter: Date IN within the last 1 month
    - From: within the last 1 month
  - Filter: Problem Significance
    - Filter ValueSets: `on_chd_reg_pg1_hrc_vs3`


## ValueSet Friendly Names
### LTC LCS: CHD Register*
- None
### On CHD Register- LTC LCS Priority Group 1 (HRC)
- `on_chd_reg_pg1_hrc_vs1` (SNOMED, 1 codes): Refset: 999000771000230107 | Cluster: CHD_COD
- `on_chd_reg_pg1_hrc_vs2` (Internal, 3 codes): First, New, Flare Up
- `on_chd_reg_pg1_hrc_vs3` (Internal, 1 codes): Significant