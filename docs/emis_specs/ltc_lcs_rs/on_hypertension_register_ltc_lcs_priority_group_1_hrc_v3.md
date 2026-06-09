<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1vxztxi1-k8xa-vb-1o40-17rw0q41r18j
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3
Parent population: Based on "LTC LCS: Hypertension Register*" search results

## Parent Chain
- LTC LCS: Hypertension Register*: Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
  Library refs: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with based on "ltc lcs: hypertension register*" search results. Require Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months. Exclude patients who match Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months. Finally include patients who do not match Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`, `on_htn_reg_pg1_hrc_v3_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`, `on_htn_reg_pg1_hrc_v3_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 2 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`, `on_htn_reg_pg1_hrc_v3_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`, `on_htn_reg_pg1_hrc_v3_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_pg1_hrc_v3_vs3`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs3`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_pg1_hrc_v3_vs4`, `on_htn_reg_pg1_hrc_v3_vs1`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs4`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_pg1_hrc_v3_vs6`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs6`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 120
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 120
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 180
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 180

### Rule 3 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: AND
- Summary: Included if it does not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg1_hrc_v3_vs1`, `on_htn_reg_pg1_hrc_v3_vs2`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_pg1_hrc_v3_vs2`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs2`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_pg1_hrc_v3_vs7`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs7`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_htn_reg_pg1_hrc_v3_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs2`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_htn_reg_pg1_hrc_v3_vs7`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 115
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 115
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_htn_reg_pg1_hrc_v3_vs5`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 170
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 170


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None
### On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3
- `on_htn_reg_pg1_hrc_v3_vs1` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_htn_reg_pg1_hrc_v3_vs2` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD
- `on_htn_reg_pg1_hrc_v3_vs3` (SNOMED, 49 codes): Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic blood pressure +46 more | Cluster: Systolic Blood Pressure
- `on_htn_reg_pg1_hrc_v3_vs4` (SNOMED, 45 codes): Minimum diastolic blood pressure, Minimum day interval diastolic blood pressure, Minimum 24 hour diastolic blood pressure +42 more | Cluster: Diastolic Blood Pressure
- `on_htn_reg_pg1_hrc_v3_vs5` (SNOMED, 36 codes): Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood pressure +33 more | Cluster: Systolic Blood Pressure
- `on_htn_reg_pg1_hrc_v3_vs6` (SNOMED, 32 codes): Increased diastolic arterial pressure, High diastolic arterial pressure, Increased diastolic blood pressure +29 more | Cluster: Diastolic Blood Pressure
- `on_htn_reg_pg1_hrc_v3_vs5` (SNOMED, 13 codes): Minimum systolic blood pressure, Average home systolic blood pressure, Average day interval systolic blood pressure +10 more | Cluster: Systolic Blood Pressure
- `on_htn_reg_pg1_hrc_v3_vs7` (SNOMED, 13 codes): Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, Ambulatory diastolic blood pressure +10 more | Cluster: Diastolic Blood Pressure