<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0qghi8r0-lwhd-96-033h-070tzqb11cj5
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*
Parent population: Based on "LTC LCS: Stroke/TIA Register*" search results

## Parent Chain
- LTC LCS: Stroke/TIA Register*: Start with currently registered patients. Finally include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).
  Library refs: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42)

## Library Items
- LTC LCS: Stroke/TIA Register*: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42); wrapper reports: LTC LCS: Stroke/TIA Register*

## Target Report Logic
Start with based on "ltc lcs: stroke/tia register*" search results. Require Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 where Date before 1 year ago. Exclude patients who match Patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months; Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months. Finally include patients who do not match Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 where Date before 1 year ago) AND NOT (patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*
- Population ref: On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* (f352f100-8bef-45f7-a6f5-95616b326015)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR First, New, Flare Up where Date within the last 365 days to before 30 days ago AND Episode = First, New, Flare Up OR Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 OR Significant where Date within the last 365 days to before 30 days ago AND Problem Significance = Significant
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs1`, `on_stroketia_reg_pg2_hr_vs2`, `on_stroketia_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs1`, `on_stroketia_reg_pg2_hr_vs2`
  - Filter: Date IN within the last 365 days to before 30 days ago
    - From: within the last 365 days
    - To: before 30 days ago
  - Filter: Episode (First, New...)
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs3`
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs1`, `on_stroketia_reg_pg2_hr_vs2`, `on_stroketia_reg_pg2_hr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs1`, `on_stroketia_reg_pg2_hr_vs2`
  - Filter: Date IN within the last 365 days to before 30 days ago
    - From: within the last 365 days
    - To: before 30 days ago
  - Filter: Problem Significance
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs4`

### Rule 3 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 where Date before 1 year ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs1`, `on_stroketia_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs1`, `on_stroketia_reg_pg2_hr_vs2`
  - Filter: Date IN before 1 year ago
    - To: before 1 year ago

### Rule 4 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs5`, `on_stroketia_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs5`, `on_stroketia_reg_pg2_hr_vs6`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs7`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs7`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs8`, `on_stroketia_reg_pg2_hr_vs5`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs8`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs5`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs10`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs10`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
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
  - ValueSets: `on_stroketia_reg_pg2_hr_vs5`, `on_stroketia_reg_pg2_hr_vs6`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs6`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_stroketia_reg_pg2_hr_vs11`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs11`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs6`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs11`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs11`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
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
  - ValueSets: `on_stroketia_reg_pg2_hr_vs5`, `on_stroketia_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs5`, `on_stroketia_reg_pg2_hr_vs6`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs7`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs7`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs8`, `on_stroketia_reg_pg2_hr_vs5`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs8`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs5`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs10`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs10`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 150
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 150

### Rule 8 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: AND
- Summary: Included if it does not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs5`, `on_stroketia_reg_pg2_hr_vs6`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs6`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs6`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_stroketia_reg_pg2_hr_vs11`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs11`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs6`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_stroketia_reg_pg2_hr_vs11`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs11`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `on_stroketia_reg_pg2_hr_vs9`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `on_stroketia_reg_pg2_hr_vs9`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 145
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 145


## ValueSet Friendly Names
### LTC LCS: Stroke/TIA Register*
- None
### On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*
- `on_stroketia_reg_pg2_hr_vs1` (SNOMED, 1 codes): Refset: 999005531000230105 | Cluster: STRK_COD
- `on_stroketia_reg_pg2_hr_vs2` (SNOMED, 1 codes): Refset: 999005291000230109 | Cluster: TIA_COD
- `on_stroketia_reg_pg2_hr_vs3` (Internal, 3 codes): First, New, Flare Up
- `on_stroketia_reg_pg2_hr_vs4` (Internal, 1 codes): Significant
- `on_stroketia_reg_pg2_hr_vs5` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `on_stroketia_reg_pg2_hr_vs6` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD
- `on_stroketia_reg_pg2_hr_vs7` (SNOMED, 49 codes): Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic blood pressure +46 more | Cluster: Systolic Blood Pressure
- `on_stroketia_reg_pg2_hr_vs8` (SNOMED, 45 codes): Minimum diastolic blood pressure, Minimum day interval diastolic blood pressure, Minimum 24 hour diastolic blood pressure +42 more | Cluster: Diastolic Blood Pressure
- `on_stroketia_reg_pg2_hr_vs9` (SNOMED, 36 codes): Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood pressure +33 more | Cluster: Systolic Blood Pressure
- `on_stroketia_reg_pg2_hr_vs10` (SNOMED, 32 codes): Increased diastolic arterial pressure, High diastolic arterial pressure, Increased diastolic blood pressure +29 more | Cluster: Diastolic Blood Pressure
- `on_stroketia_reg_pg2_hr_vs9` (SNOMED, 13 codes): Minimum systolic blood pressure, Average home systolic blood pressure, Average day interval systolic blood pressure +10 more | Cluster: Systolic Blood Pressure
- `on_stroketia_reg_pg2_hr_vs11` (SNOMED, 13 codes): Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, Ambulatory diastolic blood pressure +10 more | Cluster: Diastolic Blood Pressure