<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1wmdwbm1-bml2-gs-1hi7-0hjjpxc0395f
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On PAD Register- LTC LCS Priority Group 1 (HRC)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On PAD Register- LTC LCS Priority Group 1 (HRC)
Parent population: Based on "LTC LCS: PAD Register*" search results

## Parent Chain
- LTC LCS: PAD Register*: Start with currently registered patients. Finally include patients who match PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65).
  Library refs: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65)

## Library Items
- LTC LCS: PAD Register*: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65); wrapper reports: LTC LCS: PAD Register*

## Target Report Logic
Start with based on "ltc lcs: pad register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date within the last 90 days.

Boolean logic:
(Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date within the last 90 days)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date within the last 90 days
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg1_hrc_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg1_hrc_vs1`
  - Filter: Date IN within the last 90 days
    - From: within the last 90 days


## ValueSet Friendly Names
### LTC LCS: PAD Register*
- None
### On PAD Register- LTC LCS Priority Group 1 (HRC)
- `on_pad_reg_pg1_hrc_vs1` (SNOMED, 53 codes): Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more