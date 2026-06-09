<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0t7a40d0-33c6-bg-1fzx-052017d0cnen
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On COPD Register- LTC LCS Priority Group 2 (HR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On COPD Register- LTC LCS Priority Group 2 (HR)
Parent population: Based on "LTC LCS: COPD Register*" search results

## Parent Chain
- LTC LCS: COPD Register*: Start with currently registered patients. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD.
  Library refs: ee5b135f-b9b2-4ef7-8b51-939a754cf935

## Library Items
- LTC LCS: COPD Register*: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Target Report Logic
Start with based on "ltc lcs: copd register*" search results. Exclude patients who match Patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC). Finally include patients who match Clinical Codes [EVENTS] with 2 COPD exacerbations in past year, 3+ COPD exacerbations in past year, Acute exacerbation of COPD where Date within the last 12 months.

Boolean logic:
NOT (patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC)) AND (Clinical Codes [EVENTS] with 2 COPD exacerbations in past year, 3+ COPD exacerbations in past year, Acute exacerbation of COPD where Date within the last 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC)
- Population ref: On COPD Register- LTC LCS Priority Group 1 (HRC) (e07489c2-3c39-42da-961f-f905d81e607d)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Percent predicted FEV1 then Latest 1 where numeric value < 50
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg2_hr_vs1`
  - Restriction: Latest 1 where numeric value < 50
    - Condition: READCODE IN
    - Condition: NUMERIC_VALUE IN | < 50

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Chronic cor pulmonale where Date within the last 5 years
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg2_hr_vs2`
  - Filter: Date IN within the last 5 years
    - From: within the last 5 years

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Medical Research Council (MRC) Breathlessness Scale: grade 4, Medical Research Council Breathlessness Scale grade 4, MRC Breathlessness Scale: grade 4
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg2_hr_vs3`

### Rule 5 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with 2 COPD exacerbations in past year, 3+ COPD exacerbations in past year, Acute exacerbation of COPD where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg2_hr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg2_hr_vs4`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months


## ValueSet Friendly Names
### LTC LCS: COPD Register*
- `copd_reg_vs1` (SNOMED, 1 codes): Refset: 999011571000230107 | Cluster: COPD_COD
- `copd_reg_vs3` (SNOMED, 1 codes): Refset: 999020251000230104 | Cluster: FEV1FVC_COD
- `copd_reg_vs4` (SNOMED, 1 codes): Refset: 999020291000230109 | Cluster: FEV1FVCL70_COD
- `copd_reg_vs5` (SNOMED, 1 codes): UK NHS primary care data extraction - General practice data extraction - FEV1 FVC ratio below 70 per cent simple reference set
### On COPD Register- LTC LCS Priority Group 2 (HR)
- `on_copd_reg_pg2_hr_vs1` (SNOMED, 1 codes): Percent predicted FEV1
- `on_copd_reg_pg2_hr_vs2` (SNOMED, 1 codes): Chronic cor pulmonale
- `on_copd_reg_pg2_hr_vs3` (SNOMED, 3 codes): Medical Research Council (MRC) Breathlessness Scale: grade 4, Medical Research Council Breathlessness Scale grade 4, MRC Breathlessness Scale: grade 4
- `on_copd_reg_pg2_hr_vs4` (SNOMED, 3 codes): 2 COPD exacerbations in past year, 3+ COPD exacerbations in past year, Acute exacerbation of COPD