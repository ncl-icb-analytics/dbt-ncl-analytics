<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0xbphb91-oqgg-cn-0k3s-0o6e0ot0lk9l
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Hypertension Register- LTC LCS Priority Group 2 (HR) v3

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Hypertension Register- LTC LCS Priority Group 2 (HR) v3
Parent population: Based on "LTC LCS: Hypertension Register*" search results

## Parent Chain
- LTC LCS: Hypertension Register*: Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
  Library refs: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with based on "ltc lcs: hypertension register*" search results. Require Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months. Exclude patients who match Patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3. Finally include patients who match Patients included in search Priority Group 2a (ICB) v3 OR patients included in search Priority Group 2b (ICB) v3.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months) AND NOT (patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3) AND (patients included in search Priority Group 2a (ICB) v3 OR patients included in search Priority Group 2b (ICB) v3)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg2_hr_v3_vs1`, `on_htn_reg_pg2_hr_v3_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_pg2_hr_v3_vs1`, `on_htn_reg_pg2_hr_v3_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 2 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3
- Population ref: On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 (bf30380c-6e3e-4a4a-ba3d-8529d45ce74f)

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: patients included in search Priority Group 2a (ICB) v3 OR patients included in search Priority Group 2b (ICB) v3
- Population ref: Priority Group 2a (ICB) v3 (e9561b9a-be72-4b08-aa86-45560369c08b)
- Population ref: Priority Group 2b (ICB) v3 (0bec63f6-e2ea-4fba-a01e-741e549d2a56)


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None
### On Hypertension Register- LTC LCS Priority Group 2 (HR) v3
- `on_htn_reg_pg2_hr_v3_vs1` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_htn_reg_pg2_hr_v3_vs2` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD