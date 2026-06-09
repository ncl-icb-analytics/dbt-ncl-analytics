<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1sqm7ar1-48iw-4o-1a31-0obqzp51fiox
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: on Diabetes Register- LTC LCS Priority Group 3B (MRb)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: on Diabetes Register- LTC LCS Priority Group 3B (MRb)
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Require Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 48 and <= 58. Exclude patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa). Finally include patients who match Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000 OR Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000.

Boolean logic:
(Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 48 and <= 58) AND NOT (patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa)) AND (Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000 OR Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa)
- Population ref: on Diabetes Register- LTC LCS Priority Group 1 (HRC) (d132f6b5-fefd-4d7e-a16d-d33e5387cd81)
- Population ref: on Diabetes Register- LTC LCS Priority Group 2 (HR) (684e6c7a-077e-4f61-99b0-3324aee1d76f)
- Population ref: on Diabetes Register- LTC LCS Priority Group 3A (MRa) (ad2cbe36-3a96-4446-950c-1edffd856e3d)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 58 and <= 75
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs1`
  - Restriction: Latest 1 where numeric value > 58 and <= 75
    - Condition: NUMERIC_VALUE IN | > 58 and <= 75

### Rule 3 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 48 and <= 58
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs1`
  - Restriction: Latest 1 where numeric value > 48 and <= 58
    - Condition: NUMERIC_VALUE IN | > 48 and <= 58

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Erectile dysfunction, C/O erectile dysfunction OR Clinical Codes [EVENTS] with Background diabetic retinopathy, Diabetic retinopathy, O/E - left eye background diabetic retinopathy +17 more OR Clinical Codes [EVENTS] with Body mass index, BMI - Body mass index, Body mass index +2 more then Latest 1 where numeric value > 35 OR Clinical Codes [EVENTS] with Diabetic neuropathy, Acute painful diabetic neuropathy, Chronic painful diabetic neuropathy +1 more OR Clinical Codes [EVENTS] with O/E - Right diabetic foot at moderate risk, O/E - Left diabetic foot at moderate risk, O/E - Right diabetic foot at high risk +3 more OR Clinical Codes [EVENTS] with Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failure +40 more OR Medication Issues [MEDICATION_ISSUES] with Insulins OR Exenatide, Liraglutide, Lixisenatide +2 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Claudication, Bilateral lower limb atherosclerosis pain at rest co-occurrent and due to atherosclerosis, Pain at rest of bilateral lower limbs co-occurrent and due to atherosclerosis +81 more
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs2`
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs3`
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs4`
  - Restriction: Latest 1 where numeric value > 35
    - Condition: NUMERIC_VALUE IN | > 35
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs5`
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs6`
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs7`
  - Filter: Episode (First, New...)
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs8`, `on_dm_reg_priority_group_3b_mrb_vs9`
  - Filter: Drug
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs8`, `on_dm_reg_priority_group_3b_mrb_vs9`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs10`

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 45 and < 49
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs11`
  - Restriction: Latest 1 where numeric value >= 45 and < 49
    - Condition: NUMERIC_VALUE IN | >= 45 and < 49

### Rule 6 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio then Latest 1 where numeric value >= 3 and <= 30
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs12`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs12`
  - Restriction: Latest 1 where numeric value >= 3 and <= 30
    - Condition: NUMERIC_VALUE IN | >= 3 and <= 30

### Rule 7 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000 OR Clinical Codes [EVENTS] with O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more then Latest 1000
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs13`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs13`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1000
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs14`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs14`
      - Filter: Date
        - To: <=
      - Restriction: Latest 1000
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs15`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs15`
          - Filter: Date
            - To: <=
          - Restriction: Latest 1 where numeric value >= 140
            - Condition: NUMERIC_VALUE IN | >= 140
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs13`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs13`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1000
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs14`
      - Relationship: Linked on DATE
      - Filter: Clinical Code
        - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs14`
      - Filter: Date
        - To: <=
      - Restriction: Latest 1000
      - Linked criterion:
        - Clinical Codes [EVENTS]
          - ValueSets: `on_dm_reg_priority_group_3b_mrb_vs16`
          - Relationship: Linked on DATE
          - Filter: Clinical Code
            - Filter ValueSets: `on_dm_reg_priority_group_3b_mrb_vs16`
          - Filter: Date
            - To: <=
          - Restriction: Latest 1 where numeric value >= 90
            - Condition: NUMERIC_VALUE IN | >= 90


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### on Diabetes Register- LTC LCS Priority Group 3B (MRb)
- `on_dm_reg_priority_group_3b_mrb_vs1` (SNOMED, 3 codes): Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised | Cluster: IFCCHBAM_COD
- `on_dm_reg_priority_group_3b_mrb_vs2` (SNOMED, 2 codes): Erectile dysfunction, C/O erectile dysfunction
- `on_dm_reg_priority_group_3b_mrb_vs3` (SNOMED, 20 codes): Background diabetic retinopathy, Diabetic retinopathy, O/E - left eye background diabetic retinopathy +17 more
- `on_dm_reg_priority_group_3b_mrb_vs4` (SNOMED, 5 codes): Body mass index, BMI - Body mass index, Body mass index +2 more
- `on_dm_reg_priority_group_3b_mrb_vs5` (SNOMED, 4 codes): Diabetic neuropathy, Acute painful diabetic neuropathy, Chronic painful diabetic neuropathy +1 more
- `on_dm_reg_priority_group_3b_mrb_vs6` (SNOMED, 6 codes): O/E - Right diabetic foot at moderate risk, O/E - Left diabetic foot at moderate risk, O/E - Right diabetic foot at high risk +3 more
- `on_dm_reg_priority_group_3b_mrb_vs7` (SNOMED, 43 codes): Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failure +40 more | Cluster: HF_COD
- `on_dm_reg_priority_group_3b_mrb_vs8` (Drug Group, 1 codes): Insulins
- `on_dm_reg_priority_group_3b_mrb_vs9` (SCT Const, 5 codes): Exenatide, Liraglutide, Lixisenatide +2 more
- `on_dm_reg_priority_group_3b_mrb_vs10` (SNOMED, 84 codes): Claudication, Bilateral lower limb atherosclerosis pain at rest co-occurrent and due to atherosclerosis, Pain at rest of bilateral lower limbs co-occurrent and due to atherosclerosis +81 more
- `on_dm_reg_priority_group_3b_mrb_vs11` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_dm_reg_priority_group_3b_mrb_vs12` (SNOMED, 1 codes): Urine albumin:creatinine ratio
- `on_dm_reg_priority_group_3b_mrb_vs13` (SNOMED, 190 codes): O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measurement using oscillometric monitoring device with opportunistic atrial fibrillation detection +187 more | Cluster: BP_COD
- `on_dm_reg_priority_group_3b_mrb_vs14` (SNOMED, 43 codes): Average systolic blood pressure, Average night interval systolic blood pressure, Average day interval systolic blood pressure +40 more | Cluster: Systolic Blood Pressure
- `on_dm_reg_priority_group_3b_mrb_vs15` (SNOMED, 43 codes): Average systolic blood pressure, Average night interval systolic blood pressure, Average day interval systolic blood pressure +40 more
- `on_dm_reg_priority_group_3b_mrb_vs16` (SNOMED, 46 codes): Average diastolic blood pressure, Average night interval diastolic blood pressure, Average day interval diastolic blood pressure +43 more | Cluster: Diastolic Blood Pressure