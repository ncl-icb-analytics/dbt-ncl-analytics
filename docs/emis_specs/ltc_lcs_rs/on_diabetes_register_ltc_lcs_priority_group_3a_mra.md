<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0v8wf381-yh31-o5-1bu5-0eu2jd91lu1u
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: on Diabetes Register- LTC LCS Priority Group 3A (MRa)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: on Diabetes Register- LTC LCS Priority Group 3A (MRa)
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Require Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 58 and <= 75. Exclude patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR). Finally include patients who match Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000 OR Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000.

Boolean logic:
(Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 58 and <= 75) AND NOT (patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR)) AND (Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000 OR Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR)
- Population ref: on Diabetes Register- LTC LCS Priority Group 1 (HRC) (d132f6b5-fefd-4d7e-a16d-d33e5387cd81)
- Population ref: on Diabetes Register- LTC LCS Priority Group 2 (HR) (684e6c7a-077e-4f61-99b0-3324aee1d76f)

### Rule 2 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 58 and <= 75
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs1`
  - Restriction: Latest 1 where numeric value > 58 and <= 75
    - Condition: NUMERIC_VALUE IN | > 58 and <= 75

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +539 more where Date within the last 12 months OR Clinical Codes [EVENTS] with Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +345 more OR Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +39 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs2`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs3`, `on_dm_reg_priority_group_3a_mra_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs3`, `on_dm_reg_priority_group_3a_mra_vs4`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 30 and < 44
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs5`
  - Restriction: Latest 1 where numeric value >= 30 and < 44
    - Condition: NUMERIC_VALUE IN | >= 30 and < 44

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio then Latest 1 where numeric value > 30
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs6`
  - Restriction: Latest 1 where numeric value > 30
    - Condition: NUMERIC_VALUE IN | > 30

### Rule 6 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000 OR Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs7`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1000
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_dm_reg_priority_group_3a_mra_vs8`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs8`
      - Filter: Date
        - To: <=
      - Restriction: Latest 1000
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_dm_reg_priority_group_3a_mra_vs9`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs9`
          - Filter: Date
            - To: <=
          - Restriction: Latest 1 where numeric value >= 140
            - Condition: NUMERIC_VALUE IN | >= 140
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3a_mra_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs7`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1000
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_dm_reg_priority_group_3a_mra_vs8`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs8`
      - Filter: Date
        - To: <=
      - Restriction: Latest 1000
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_dm_reg_priority_group_3a_mra_vs10`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_dm_reg_priority_group_3a_mra_vs10`
          - Filter: Date
            - To: <=
          - Restriction: Latest 1 where numeric value >= 90
            - Condition: NUMERIC_VALUE IN | >= 90


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### on Diabetes Register- LTC LCS Priority Group 3A (MRa)
- `on_dm_reg_priority_group_3a_mra_vs1` (SNOMED, 3 codes): Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised | Cluster: IFCCHBAM_COD
- `on_dm_reg_priority_group_3a_mra_vs2` (SNOMED, 542 codes): Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +539 more | Cluster: CHD_COD
- `on_dm_reg_priority_group_3a_mra_vs3` (SNOMED, 348 codes): Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +345 more | Cluster: STRK_COD
- `on_dm_reg_priority_group_3a_mra_vs4` (SNOMED, 42 codes): Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +39 more | Cluster: TIA_COD
- `on_dm_reg_priority_group_3a_mra_vs5` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_dm_reg_priority_group_3a_mra_vs6` (SNOMED, 1 codes): Urine albumin:creatinine ratio
- `on_dm_reg_priority_group_3a_mra_vs7` (SNOMED, 190 codes): O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more | Cluster: BP_COD
- `on_dm_reg_priority_group_3a_mra_vs8` (SNOMED, 43 codes): Average systolic blood pressure, Average night interval systolic blood pressure, Average day interval systolic blood pressure +40 more | Cluster: Systolic Blood Pressure
- `on_dm_reg_priority_group_3a_mra_vs9` (SNOMED, 43 codes): Average systolic blood pressure, Average night interval systolic blood pressure, Average day interval systolic blood pressure +40 more
- `on_dm_reg_priority_group_3a_mra_vs10` (SNOMED, 46 codes): Average diastolic blood pressure, Average night interval diastolic blood pressure, Average day interval diastolic blood pressure +43 more | Cluster: Diastolic Blood Pressure