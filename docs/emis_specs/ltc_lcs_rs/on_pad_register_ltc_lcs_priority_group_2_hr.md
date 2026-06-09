<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 17iba9o0-03av-za-1vjx-0fc1u3d0ac34
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On PAD Register- LTC LCS Priority Group 2 (HR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On PAD Register- LTC LCS Priority Group 2 (HR)
Parent population: Based on "LTC LCS: PAD Register*" search results

## Parent Chain
- LTC LCS: PAD Register*: Start with currently registered patients. Finally include patients who match PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65).
  Library refs: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65)

## Library Items
- LTC LCS: PAD Register*: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65); wrapper reports: LTC LCS: PAD Register*

## Target Report Logic
Start with based on "ltc lcs: pad register*" search results. Require Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date before 12 months ago. Exclude patients who match Patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC); Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months. Finally include patients who do not match Patient Details [PATIENTS] NOT where Age under 80 years old AND Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months.

Boolean logic:
(Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date before 12 months ago) AND NOT (patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC)) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Patient Details [PATIENTS] NOT where Age under 80 years old AND Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC)
- Population ref: On PAD Register- LTC LCS Priority Group 1 (HRC) (224df436-6871-45cc-b733-f063a59ba527)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date within the last 365 days to before 90 days ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs1`
  - Filter: Date IN within the last 365 days to before 90 days ago
    - From: within the last 365 days
    - To: before 90 days ago

### Rule 3 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date before 12 months ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs1`
  - Filter: Date IN before 12 months ago
    - To: before 12 months ago

### Rule 4 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs2`, `on_pad_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs2`, `on_pad_reg_pg2_hr_vs3`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs4`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs4`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs5`, `on_pad_reg_pg2_hr_vs2`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs5`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs2`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs7`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 140
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 140

### Rule 5 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs2`, `on_pad_reg_pg2_hr_vs3`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs3`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs3`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs6`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_pad_reg_pg2_hr_vs8`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_pad_reg_pg2_hr_vs8`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs3`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs8`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs8`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 135
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 135

### Rule 6 (Additional)
- Clause type: informational
- Pass: Next rule
- Fail: Include
- Operator: AND
- Summary: Patient Details [PATIENTS] where Age more than 80 years old
- Patient Details [PATIENTS]
  - Filter: Age IN more than 80 years old
    - From: more than 80 years old

### Rule 7 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs2`, `on_pad_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs2`, `on_pad_reg_pg2_hr_vs3`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs4`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs4`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs5`, `on_pad_reg_pg2_hr_vs2`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs5`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs2`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs7`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs7`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 150
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 150

### Rule 8 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: AND
- Summary: Included if it does not match: Patient Details [PATIENTS] NOT where Age under 80 years old AND Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Patient Details [PATIENTS] (NOT)
  - Filter: Age IN under 80 years old
    - To: under 80 years old
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs2`, `on_pad_reg_pg2_hr_vs3`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs3`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs3`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs6`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_pad_reg_pg2_hr_vs8`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_pad_reg_pg2_hr_vs8`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg2_hr_vs3`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_pad_reg_pg2_hr_vs8`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_pad_reg_pg2_hr_vs8`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_pad_reg_pg2_hr_vs6`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 145
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 145


## ValueSet Friendly Names
### LTC LCS: PAD Register*
- None
### On PAD Register- LTC LCS Priority Group 2 (HR)
- `on_pad_reg_pg2_hr_vs1` (SNOMED, 53 codes): Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more
- `on_pad_reg_pg2_hr_vs2` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_pad_reg_pg2_hr_vs3` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD
- `on_pad_reg_pg2_hr_vs4` (SNOMED, 49 codes): Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic blood pressure +46 more | Cluster: Systolic Blood Pressure
- `on_pad_reg_pg2_hr_vs5` (SNOMED, 45 codes): Minimum diastolic blood pressure, Minimum day interval diastolic blood pressure, Minimum 24 hour diastolic blood pressure +42 more | Cluster: Diastolic Blood Pressure
- `on_pad_reg_pg2_hr_vs6` (SNOMED, 36 codes): Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood pressure +33 more | Cluster: Systolic Blood Pressure
- `on_pad_reg_pg2_hr_vs7` (SNOMED, 32 codes): Increased diastolic arterial pressure, High diastolic arterial pressure, Increased diastolic blood pressure +29 more | Cluster: Diastolic Blood Pressure
- `on_pad_reg_pg2_hr_vs6` (SNOMED, 13 codes): Minimum systolic blood pressure, Average home systolic blood pressure, Average day interval systolic blood pressure +10 more | Cluster: Systolic Blood Pressure
- `on_pad_reg_pg2_hr_vs8` (SNOMED, 13 codes): Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, Ambulatory diastolic blood pressure +10 more | Cluster: Diastolic Blood Pressure