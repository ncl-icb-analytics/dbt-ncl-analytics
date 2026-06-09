<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 18mja1v1-10zn-dd-0cdc-0a5u24d1qyue
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*
Parent population: Based on "LTC LCS: Stroke/TIA Register*" search results

## Parent Chain
- LTC LCS: Stroke/TIA Register*: Start with currently registered patients. Finally include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).
  Library refs: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42)

## Library Items
- LTC LCS: Stroke/TIA Register*: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42); wrapper reports: LTC LCS: Stroke/TIA Register*

## Target Report Logic
Start with based on "ltc lcs: stroke/tia register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR First, New, Flare Up where Date within the last 30 days AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR Significant where Date within the last 30 days AND Problem Significance = Significant.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR First, New, Flare Up where Date within the last 30 days AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR Significant where Date within the last 30 days AND Problem Significance = Significant)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR First, New, Flare Up where Date within the last 30 days AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR Significant where Date within the last 30 days AND Problem Significance = Significant
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg1_hrc_vs1`, `on_stroketia_reg_pg1_hrc_vs2`, `on_stroketia_reg_pg1_hrc_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg1_hrc_vs1`, `on_stroketia_reg_pg1_hrc_vs2`
  - Filter: Date IN within the last 30 days
    - From: within the last 30 days
  - Filter: Episode (First, New...)
    - Filter ValueSets: `on_stroketia_reg_pg1_hrc_vs3`
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg1_hrc_vs1`, `on_stroketia_reg_pg1_hrc_vs2`, `on_stroketia_reg_pg1_hrc_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg1_hrc_vs1`, `on_stroketia_reg_pg1_hrc_vs2`
  - Filter: Date IN within the last 30 days
    - From: within the last 30 days
  - Filter: Problem Significance
    - Filter ValueSets: `on_stroketia_reg_pg1_hrc_vs4`


## ValueSet Friendly Names
### LTC LCS: Stroke/TIA Register*
- None
### On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*
- `on_stroketia_reg_pg1_hrc_vs1` (SNOMED, 1 codes): Refset: 999005531000230105 | Cluster: STRK_COD
- `on_stroketia_reg_pg1_hrc_vs2` (SNOMED, 1 codes): Refset: 999005291000230109 | Cluster: TIA_COD
- `on_stroketia_reg_pg1_hrc_vs3` (Internal, 3 codes): First, New, Flare Up
- `on_stroketia_reg_pg1_hrc_vs4` (Internal, 1 codes): Significant