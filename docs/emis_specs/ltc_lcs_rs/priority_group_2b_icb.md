<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 13raf1e0-3v8e-3i-1x5m-04yx0md1ngbh
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: Priority Group 2b (ICB)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: Priority Group 2b (ICB)
Parent population: Based on "LTC LCS: Hypertension Register*" search results

## Parent Chain
- LTC LCS: Hypertension Register*: Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
  Library refs: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with based on "ltc lcs: hypertension register*" search results. Require Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months; Clinical Codes [EVENTS] with Black African, Black Caribbean, Black Caribbean +75 more; Clinical Codes [EVENTS] with Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +441 more OR Clinical Codes [EVENTS] with Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +268 more OR Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +35 more OR Clinical Codes [EVENTS] with Claudication, Charcot  s syndrome, IC - Intermittent claudication +29 more OR Clinical Codes [EVENTS] with Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occurrent and due to chronic kidney disease stage 3, Chronic kidney disease stage 5 on dialysis +103 more OR Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation then Latest 1 where numeric value < 60 OR Clinical Codes [EVENTS] with Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosis, Hyperosmolar hyperglycemic coma due to diabetes mellitus without ketoacidosis, Lactic acidosis with diabetes mellitus +524 more OR Clinical Codes [EVENTS] with Body mass index, BMI - Body mass index, Body mass index +2 more then Latest 1 where numeric value > 35. Exclude patients who match Patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC); Patients included in search Priority Group 2a (ICB); Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months. Finally include patients who do not match Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months) AND (Clinical Codes [EVENTS] with Black African, Black Caribbean, Black Caribbean +75 more) AND (Clinical Codes [EVENTS] with Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +441 more OR Clinical Codes [EVENTS] with Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +268 more OR Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +35 more OR Clinical Codes [EVENTS] with Claudication, Charcot  s syndrome, IC - Intermittent claudication +29 more OR Clinical Codes [EVENTS] with Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occurrent and due to chronic kidney disease stage 3, Chronic kidney disease stage 5 on dialysis +103 more OR Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation then Latest 1 where numeric value < 60 OR Clinical Codes [EVENTS] with Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosis, Hyperosmolar hyperglycemic coma due to diabetes mellitus without ketoacidosis, Lactic acidosis with diabetes mellitus +524 more OR Clinical Codes [EVENTS] with Body mass index, BMI - Body mass index, Body mass index +2 more then Latest 1 where numeric value > 35) AND NOT (patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC)) AND NOT (patients included in search Priority Group 2a (ICB)) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months) AND NOT (Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs1`, `priority_group_2b_icb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs1`, `priority_group_2b_icb_vs2`
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
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search Priority Group 2a (ICB)
- Population ref: Priority Group 2a (ICB) (f8a10ce9-983d-43a4-a8da-0d829d81ce33)

### Rule 4 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Black African, Black Caribbean, Black Caribbean +75 more
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs3`

### Rule 5 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +441 more OR Clinical Codes [EVENTS] with Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +268 more OR Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +35 more OR Clinical Codes [EVENTS] with Claudication, Charcot  s syndrome, IC - Intermittent claudication +29 more OR Clinical Codes [EVENTS] with Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occurrent and due to chronic kidney disease stage 3, Chronic kidney disease stage 5 on dialysis +103 more OR Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation then Latest 1 where numeric value < 60 OR Clinical Codes [EVENTS] with Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosis, Hyperosmolar hyperglycemic coma due to diabetes mellitus without ketoacidosis, Lactic acidosis with diabetes mellitus +524 more OR Clinical Codes [EVENTS] with Body mass index, BMI - Body mass index, Body mass index +2 more then Latest 1 where numeric value > 35
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs4`
  - Filter: Episode (First, New...)
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs5`, `priority_group_2b_icb_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs5`, `priority_group_2b_icb_vs6`
  - Filter: Episode (First, New...)
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs7`
  - Filter: Episode (First, New...)
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs8`
  - Filter: Date
    - To: <=
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs9`
  - Restriction: Latest 1 where numeric value < 60
    - Condition: NUMERIC_VALUE IN | < 60
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs10`
  - Filter: Date
    - To: <=
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs11`
  - Restriction: Latest 1 where numeric value > 35
    - Condition: NUMERIC_VALUE IN | > 35

### Rule 6 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more where Date within the last 12 months then Latest 100 AND Clinical Codes [EVENTS] with Refset: 999036281000230108 then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs1`, `priority_group_2b_icb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs1`, `priority_group_2b_icb_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
  - Restriction: Latest 100
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `priority_group_2b_icb_vs12`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `priority_group_2b_icb_vs12`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `priority_group_2b_icb_vs13`, `priority_group_2b_icb_vs1`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `priority_group_2b_icb_vs13`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where SNOMED code IN: CLINBP_COD
            - Condition: READCODE IN | CLINBP_COD
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs1`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `priority_group_2b_icb_vs14`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `priority_group_2b_icb_vs14`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `priority_group_2b_icb_vs15`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `priority_group_2b_icb_vs15`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 90
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 90
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `priority_group_2b_icb_vs14`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `priority_group_2b_icb_vs14`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 140
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 140

### Rule 7 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: AND
- Summary: Included if it does not match: Clinical Codes [EVENTS] with Refset: 999036281000230108 OR 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 1 AND Clinical Codes [EVENTS] with 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more then Latest 100 where date > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs1`, `priority_group_2b_icb_vs2`
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `priority_group_2b_icb_vs2`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `priority_group_2b_icb_vs2`
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `priority_group_2b_icb_vs14`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `priority_group_2b_icb_vs14`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `priority_group_2b_icb_vs16`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `priority_group_2b_icb_vs16`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value > 0
                - Condition: NUMERIC_VALUE IN | > 0
- Clinical Codes [EVENTS]
  - ValueSets: `priority_group_2b_icb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `priority_group_2b_icb_vs2`
  - Restriction: Latest 100 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `priority_group_2b_icb_vs14`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `priority_group_2b_icb_vs14`
      - Filter: Value IN > 0
        - From: > 0
      - Restriction: Latest 100
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `priority_group_2b_icb_vs16`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `priority_group_2b_icb_vs16`
          - Filter: Value IN > 0
            - From: > 0
          - Restriction: Latest 1 where numeric value >= 1 and <= 85
            - Condition: NUMERIC_VALUE IN | >= 1 and <= 85
          - Linked criterion:
            - Clinical Codes [EVENTS]
              - ValueSets: `priority_group_2b_icb_vs14`
              - Relationship: Linked on DATE
              - Filter: Clinical Code
                - Filter ValueSets: `priority_group_2b_icb_vs14`
              - Filter: Value IN > 0
                - From: > 0
              - Restriction: Latest 1 where numeric value >= 1 and <= 135
                - Condition: NUMERIC_VALUE IN | >= 1 and <= 135


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None
### Priority Group 2b (ICB)
- `priority_group_2b_icb_vs1` (SNOMED, 1 codes): Refset: 999036281000230108 | Cluster: CLINBP_COD
- `priority_group_2b_icb_vs2` (SNOMED, 5 codes): 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitoring +1 more | Cluster: HOMEAMBBP_COD
- `priority_group_2b_icb_vs3` (SNOMED, 78 codes): Black African, Black Caribbean, Black Caribbean +75 more
- `priority_group_2b_icb_vs4` (SNOMED, 444 codes): Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +441 more | Cluster: CHD_COD
- `priority_group_2b_icb_vs5` (SNOMED, 271 codes): Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +268 more | Cluster: STRK_COD
- `priority_group_2b_icb_vs6` (SNOMED, 38 codes): Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +35 more | Cluster: TIA_COD
- `priority_group_2b_icb_vs7` (SNOMED, 32 codes): Claudication, Charcot  s syndrome, IC - Intermittent claudication +29 more | Cluster: PAD_COD
- `priority_group_2b_icb_vs8` (SNOMED, 106 codes): Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occurrent and due to chronic kidney disease stage 3, Chronic kidney disease stage 5 on dialysis +103 more | Cluster: CKD_COD
- `priority_group_2b_icb_vs9` (SNOMED, 1 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation
- `priority_group_2b_icb_vs10` (SNOMED, 527 codes): Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosis, Hyperosmolar hyperglycemic coma due to diabetes mellitus without ketoacidosis, Lactic acidosis with diabetes mellitus +524 more | Cluster: DM_COD
- `priority_group_2b_icb_vs11` (SNOMED, 5 codes): Body mass index, BMI - Body mass index, Body mass index +2 more
- `priority_group_2b_icb_vs12` (SNOMED, 49 codes): Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic blood pressure +46 more | Cluster: Systolic Blood Pressure
- `priority_group_2b_icb_vs13` (SNOMED, 45 codes): Minimum diastolic blood pressure, Minimum day interval diastolic blood pressure, Minimum 24 hour diastolic blood pressure +42 more | Cluster: Diastolic Blood Pressure
- `priority_group_2b_icb_vs14` (SNOMED, 36 codes): Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood pressure +33 more | Cluster: Systolic Blood Pressure
- `priority_group_2b_icb_vs15` (SNOMED, 32 codes): Increased diastolic arterial pressure, High diastolic arterial pressure, Increased diastolic blood pressure +29 more | Cluster: Diastolic Blood Pressure
- `priority_group_2b_icb_vs14` (SNOMED, 13 codes): Minimum systolic blood pressure, Average home systolic blood pressure, Average day interval systolic blood pressure +10 more | Cluster: Systolic Blood Pressure
- `priority_group_2b_icb_vs16` (SNOMED, 13 codes): Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, Ambulatory diastolic blood pressure +10 more | Cluster: Diastolic Blood Pressure