<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0bohg7e1-ihf6-ep-1bek-0cfdbrx0h26c
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On CHD Register- LTC LCS Priority Group 2 (HR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On CHD Register- LTC LCS Priority Group 2 (HR)
Parent population: Based on "LTC LCS: CHD Register*" search results

## Parent Chain
- LTC LCS: CHD Register*: Start with currently registered patients. Finally include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).
  Library refs: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)

## Library Items
- LTC LCS: CHD Register*: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c); wrapper reports: LTC LCS: CHD Register*

## Target Report Logic
Start with based on "ltc lcs: chd register*" search results. Exclude patients who match Patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC); Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date before 1 year ago. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date within the last 1 year to before 1 month ago.

Boolean logic:
NOT (patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC)) AND NOT (Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date before 1 year ago) AND (Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date within the last 1 year to before 1 month ago)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC)
- Population ref: On CHD Register- LTC LCS Priority Group 1 (HRC) (6c2418f3-b92c-4d74-8df7-de41376923e3)

### Rule 2 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date before 1 year ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_chd_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_chd_reg_pg2_hr_vs1`
  - Filter: Date IN before 1 year ago
    - To: before 1 year ago

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date within the last 1 year to before 1 month ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_chd_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_chd_reg_pg2_hr_vs1`
  - Filter: Date IN within the last 1 year to before 1 month ago
    - From: within the last 1 year
    - To: before 1 month ago


## ValueSet Friendly Names
### LTC LCS: CHD Register*
- None
### On CHD Register- LTC LCS Priority Group 2 (HR)
- `on_chd_reg_pg2_hr_vs1` (SNOMED, 1 codes): Refset: 999000771000230107 | Cluster: CHD_COD