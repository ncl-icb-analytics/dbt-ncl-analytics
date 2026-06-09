<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 17i5r9q0-6a5s-je-01r5-1mv04k71noux
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On CKD Register- LTC LCS Priority Group 3 (MR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On CKD Register- LTC LCS Priority Group 3 (MR)
Parent population: Based on "LTC LCS: CKD Register*" search results

## Parent Chain
- LTC LCS: CKD Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
  Library refs: c913f5a7-1256-4de6-871e-23650e72765e

## Library Items
- LTC LCS: CKD Register*: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)
- On CKD Register- LTC LCS Priority Group 3 (MR): Unknown library item (3de35e4f-7964-4f24-a0b4-fd42930a1dd1)

## Target Report Logic
Start with based on "ltc lcs: ckd register*" search results. Exclude patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CKD Register- LTC LCS Priority Group 2 (HR). Finally include patients who match Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 30 and <= 44 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value < 3.

Boolean logic:
NOT (patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CKD Register- LTC LCS Priority Group 2 (HR)) AND (Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 30 and <= 44 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value < 3)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CKD Register- LTC LCS Priority Group 2 (HR)
- Population ref: On CKD Register- LTC LCS Priority Group 1(HRC) (a94b76df-d4bf-4587-956a-bd10f551dc0b)
- Population ref: On CKD Register- LTC LCS Priority Group 2 (HR) (e98e7e2b-5be2-485b-9fd9-7d20d8d128de)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value > 60 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value >= 30
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Restriction: Latest 1 where numeric value > 60
    - Condition: NUMERIC_VALUE IN | > 60
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Restriction: Latest 1 where numeric value >= 30
    - Condition: NUMERIC_VALUE IN | >= 30

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value > 60 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value >= 3 AND library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1
- Library item: Unknown library item (3de35e4f-7964-4f24-a0b4-fd42930a1dd1)
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Restriction: Latest 1 where numeric value > 60
    - Condition: NUMERIC_VALUE IN | > 60
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Restriction: Latest 1 where numeric value >= 3
    - Condition: NUMERIC_VALUE IN | >= 3

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 45 and <= 59 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value >= 3 and <= 30
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Restriction: Latest 1 where numeric value >= 45 and <= 59
    - Condition: NUMERIC_VALUE IN | >= 45 and <= 59
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Restriction: Latest 1 where numeric value >= 3 and <= 30
    - Condition: NUMERIC_VALUE IN | >= 3 and <= 30

### Rule 5 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 30 and <= 44 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value < 3
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs1`
  - Restriction: Latest 1 where numeric value >= 30 and <= 44
    - Condition: NUMERIC_VALUE IN | >= 30 and <= 44
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg3_mr_vs2`
  - Restriction: Latest 1 where numeric value < 3
    - Condition: NUMERIC_VALUE IN | < 3


## ValueSet Friendly Names
### LTC LCS: CKD Register*
- None
### On CKD Register- LTC LCS Priority Group 3 (MR)
- `on_ckd_reg_pg3_mr_vs1` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_ckd_reg_pg3_mr_vs2` (SNOMED, 4 codes): Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more