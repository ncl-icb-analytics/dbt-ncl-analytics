<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 03t5g221-8mul-2j-0h23-1nco5yj0yqh2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Hypertension Register- LTC LCS Priority Group 3B (MRb)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Hypertension Register- LTC LCS Priority Group 3B (MRb)
Parent population: Based on "LTC LCS: Hypertension Register*" search results

## Parent Chain
- LTC LCS: Hypertension Register*: Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
  Library refs: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with based on "ltc lcs: hypertension register*" search results. Require Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months; Patient Details [PATIENTS] where Age more than 80 years old. Exclude patients who match Patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa); Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months. Finally include patients who do not match Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months) AND (Patient Details [PATIENTS] where Age more than 80 years old) AND NOT (patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa)) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 2 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa)
- Population ref: On Hypertension Register- LTC LCS Priority Group 1 (HRC) (4968805e-6847-40bf-90f9-385f19192d9d)
- Population ref: On Hypertension Register- LTC LCS Priority Group 2 (HR) (a5be7c52-a638-4cac-804b-293756f7e62f)
- Population ref: On Hypertension Register- LTC LCS Priority Group 3A (MRa) (2219ace9-fb28-4dea-ade9-e7cb05751c3f)

### Rule 3 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs3`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs3`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs4`, `on_htn_reg_priority_group_3b_mrb_vs1`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs4`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs6`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs6`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 140
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 140

### Rule 4 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 135
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 135

### Rule 5 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Patient Details [PATIENTS] where Age more than 80 years old
- Patient Details [PATIENTS]
  - Filter: Age IN more than 80 years old
    - From: more than 80 years old

### Rule 6 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs3`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs3`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs4`, `on_htn_reg_priority_group_3b_mrb_vs1`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs4`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs6`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs6`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 150
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 150

### Rule 7 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: AND
- Summary: Included if it does not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs1`, `on_htn_reg_priority_group_3b_mrb_vs2`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs2`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_priority_group_3b_mrb_vs5`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 145
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 145


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None
### On Hypertension Register- LTC LCS Priority Group 3B (MRb)
- `on_htn_reg_priority_group_3b_mrb_vs1` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_htn_reg_priority_group_3b_mrb_vs2` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD
- `on_htn_reg_priority_group_3b_mrb_vs3` (SNOMED, 49 codes): Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic blood pressure +46 more | Cluster: Systolic Blood Pressure
- `on_htn_reg_priority_group_3b_mrb_vs4` (SNOMED, 45 codes): Minimum diastolic blood pressure, Minimum day interval diastolic blood pressure, Minimum 24 hour diastolic blood pressure +42 more | Cluster: Diastolic Blood Pressure
- `on_htn_reg_priority_group_3b_mrb_vs5` (SNOMED, 36 codes): Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood pressure +33 more | Cluster: Systolic Blood Pressure
- `on_htn_reg_priority_group_3b_mrb_vs6` (SNOMED, 32 codes): Increased diastolic arterial pressure, High diastolic arterial pressure, Increased diastolic blood pressure +29 more | Cluster: Diastolic Blood Pressure
- `on_htn_reg_priority_group_3b_mrb_vs5` (SNOMED, 13 codes): Minimum systolic blood pressure, Average home systolic blood pressure, Average day interval systolic blood pressure +10 more | Cluster: Systolic Blood Pressure
- `on_htn_reg_priority_group_3b_mrb_vs7` (SNOMED, 13 codes): Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, Ambulatory diastolic blood pressure +10 more | Cluster: Diastolic Blood Pressure