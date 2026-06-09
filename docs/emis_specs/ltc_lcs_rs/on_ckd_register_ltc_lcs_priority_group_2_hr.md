<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0d56nam0-ztr1-g1-0ph1-06thze01idhr
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On CKD Register- LTC LCS Priority Group 2 (HR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On CKD Register- LTC LCS Priority Group 2 (HR)
Parent population: Based on "LTC LCS: CKD Register*" search results

## Parent Chain
- LTC LCS: CKD Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
  Library refs: c913f5a7-1256-4de6-871e-23650e72765e

## Library Items
- LTC LCS: CKD Register*: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)

## Target Report Logic
Start with based on "ltc lcs: ckd register*" search results. Require Medication Issues [MEDICATION_ISSUES] with Antihypertensive Drugs where Date of Issue within the last 3 months. Exclude patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC). Finally include patients who match Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months.

Boolean logic:
(Medication Issues [MEDICATION_ISSUES] with Antihypertensive Drugs where Date of Issue within the last 3 months) AND NOT (patients included in search On CKD Register- LTC LCS Priority Group 1(HRC)) AND (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On CKD Register- LTC LCS Priority Group 1(HRC)
- Population ref: On CKD Register- LTC LCS Priority Group 1(HRC) (a94b76df-d4bf-4587-956a-bd10f551dc0b)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 45 and <= 59 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value > 30
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs1`
  - Restriction: Latest 1 where numeric value >= 45 and <= 59
    - Condition: NUMERIC_VALUE IN | >= 45 and <= 59
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs2`
  - Restriction: Latest 1 where numeric value > 30
    - Condition: NUMERIC_VALUE IN | > 30

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 30 and <= 44 AND Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value > 3
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs1`
  - Restriction: Latest 1 where numeric value >= 30 and <= 44
    - Condition: NUMERIC_VALUE IN | >= 30 and <= 44
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs2`
  - Restriction: Latest 1 where numeric value > 3
    - Condition: NUMERIC_VALUE IN | > 3

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 15 and <= 29
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs1`
  - Restriction: Latest 1 where numeric value >= 15 and <= 29
    - Condition: NUMERIC_VALUE IN | >= 15 and <= 29

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more then Latest 1 where numeric value >= 70 and < 250
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs2`
  - Restriction: Latest 1 where numeric value >= 70 and < 250
    - Condition: NUMERIC_VALUE IN | >= 70 and < 250

### Rule 6 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Medication Issues [MEDICATION_ISSUES] with Antihypertensive Drugs where Date of Issue within the last 3 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_ckd_reg_pg2_hr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs3`
  - Filter: Date of Issue IN within the last 3 months
    - From: within the last 3 months
  - Filter: Prescription Type

### Rule 7 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs4`, `on_ckd_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs4`, `on_ckd_reg_pg2_hr_vs5`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs6`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs7`, `on_ckd_reg_pg2_hr_vs4`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs4`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs9`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs9`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value > 90
            - Condition: NUMERIC_VALUE IN | > 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1
                - Condition: NUMERIC_VALUE IN | >= 1

### Rule 8 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs4`, `on_ckd_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs4`, `on_ckd_reg_pg2_hr_vs5`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs6`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs7`, `on_ckd_reg_pg2_hr_vs4`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs4`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs9`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs9`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1
            - Condition: NUMERIC_VALUE IN | >= 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 150
                - Condition: NUMERIC_VALUE IN | > 150

### Rule 9 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs4`, `on_ckd_reg_pg2_hr_vs5`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs5`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs8`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_ckd_reg_pg2_hr_vs10`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_ckd_reg_pg2_hr_vs10`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs5`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs10`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs10`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value > 90
            - Condition: NUMERIC_VALUE IN | > 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1
                - Condition: NUMERIC_VALUE IN | >= 1

### Rule 10 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs4`, `on_ckd_reg_pg2_hr_vs5`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs5`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs8`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_ckd_reg_pg2_hr_vs10`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_ckd_reg_pg2_hr_vs10`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_ckd_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_ckd_reg_pg2_hr_vs5`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_ckd_reg_pg2_hr_vs10`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_ckd_reg_pg2_hr_vs10`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1
            - Condition: NUMERIC_VALUE IN | >= 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_ckd_reg_pg2_hr_vs8`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 150
                - Condition: NUMERIC_VALUE IN | > 150


## ValueSet Friendly Names
### LTC LCS: CKD Register*
- None
### On CKD Register- LTC LCS Priority Group 2 (HR)
- `on_ckd_reg_pg2_hr_vs1` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_ckd_reg_pg2_hr_vs2` (SNOMED, 4 codes): Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine microalbumin/creatinine ratio +1 more
- `on_ckd_reg_pg2_hr_vs3` (Drug Group, 1 codes): Antihypertensive Drugs
- `on_ckd_reg_pg2_hr_vs4` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_ckd_reg_pg2_hr_vs5` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD
- `on_ckd_reg_pg2_hr_vs6` (SNOMED, 49 codes): Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic blood pressure +46 more | Cluster: Systolic Blood Pressure
- `on_ckd_reg_pg2_hr_vs7` (SNOMED, 45 codes): Minimum diastolic blood pressure, Minimum day interval diastolic blood pressure, Minimum 24 hour diastolic blood pressure +42 more | Cluster: Diastolic Blood Pressure
- `on_ckd_reg_pg2_hr_vs8` (SNOMED, 36 codes): Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood pressure +33 more | Cluster: Systolic Blood Pressure
- `on_ckd_reg_pg2_hr_vs9` (SNOMED, 32 codes): Increased diastolic arterial pressure, High diastolic arterial pressure, Increased diastolic blood pressure +29 more | Cluster: Diastolic Blood Pressure
- `on_ckd_reg_pg2_hr_vs8` (SNOMED, 13 codes): Minimum systolic blood pressure, Average home systolic blood pressure, Average day interval systolic blood pressure +10 more | Cluster: Systolic Blood Pressure
- `on_ckd_reg_pg2_hr_vs10` (SNOMED, 13 codes): Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, Ambulatory diastolic blood pressure +10 more | Cluster: Diastolic Blood Pressure