<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 19hrq4u1-v9dc-su-1fh8-1u0teja1mk7g
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Hypertension Register- LTC LCS Priority Group 2 (HR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Hypertension Register- LTC LCS Priority Group 2 (HR)
Parent population: Based on "LTC LCS: Hypertension Register*" search results

## Parent Chain
- LTC LCS: Hypertension Register*: Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
  Library refs: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with based on "ltc lcs: hypertension register*" search results. Require Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months. Exclude patients who match Patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC). Finally include patients who match Patients included in search Priority Group 2a (ICB) OR patients included in search Priority Group 2b (ICB).

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months) AND NOT (patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC)) AND (patients included in search Priority Group 2a (ICB) OR patients included in search Priority Group 2b (ICB))

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg2_hr_vs1`, `on_htn_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_pg2_hr_vs1`, `on_htn_reg_pg2_hr_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 2 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC)
- Population ref: On Hypertension Register- LTC LCS Priority Group 1 (HRC) (4968805e-6847-40bf-90f9-385f19192d9d)

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: patients included in search Priority Group 2a (ICB) OR patients included in search Priority Group 2b (ICB)
- Population ref: Priority Group 2a (ICB) (f8a10ce9-983d-43a4-a8da-0d829d81ce33)
- Population ref: Priority Group 2b (ICB) (5b04e0b5-89d7-4be1-83f9-1da8d03f30bb)


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None
### On Hypertension Register- LTC LCS Priority Group 2 (HR)
- `on_htn_reg_pg2_hr_vs1` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_htn_reg_pg2_hr_vs2` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD