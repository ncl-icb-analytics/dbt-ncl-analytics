<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1cwk8q70-lcaf-77-0rn6-17imjci14mrx
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On CKD Register- LTC LCS Priority Group 1(HRC)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On CKD Register- LTC LCS Priority Group 1(HRC)
Parent population: Based on "LTC LCS: CKD Register*" search results

## Parent Chain
- LTC LCS: CKD Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
  Library refs: c913f5a7-1256-4de6-871e-23650e72765e

## Library Items
- LTC LCS: CKD Register*: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)

## Target Report Logic
Start with based on "ltc lcs: ckd register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value > 250.

Boolean logic:
(Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value > 250)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value < 15
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg1_hrc_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg1_hrc_vs1`
  - Restriction: Latest 1 where numeric value < 15
    - Condition: NUMERIC_VALUE IN | < 15

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value > 250
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg1_hrc_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg1_hrc_vs2`
  - Restriction: Latest 1 where numeric value > 250
    - Condition: NUMERIC_VALUE IN | > 250


## ValueSet Friendly Names
### LTC LCS: CKD Register*
- None
### On CKD Register- LTC LCS Priority Group 1(HRC)
- `on_ckd_reg_pg1_hrc_vs1` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_ckd_reg_pg1_hrc_vs2` (SNOMED, 4 codes): Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more