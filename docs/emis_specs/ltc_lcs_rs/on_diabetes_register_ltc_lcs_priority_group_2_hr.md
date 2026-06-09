<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1plpqim0-29ue-v2-1xc3-14d0kwn0psps
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: on Diabetes Register- LTC LCS Priority Group 2 (HR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: on Diabetes Register- LTC LCS Priority Group 2 (HR)
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Exclude patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC). Finally include patients who match Clinical Codes [EVENTS] with Urine albumin:creatinine ratio then Latest 1 where numeric value > 70.

Boolean logic:
NOT (patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC)) AND (Clinical Codes [EVENTS] with Urine albumin:creatinine ratio then Latest 1 where numeric value > 70)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC)
- Population ref: on Diabetes Register- LTC LCS Priority Group 1 (HRC) (d132f6b5-fefd-4d7e-a16d-d33e5387cd81)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 75 and <= 90
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg2_hr_vs1`
  - Restriction: Latest 1 where numeric value > 75 and <= 90
    - Condition: NUMERIC_VALUE IN | > 75 and <= 90

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with O/E - Right diabetic foot - ulcerated, O/E - Left diabetic foot - ulcerated, Neuropathic diabetic ulcer - foot +30 more where Date within the last 3 years
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg2_hr_vs2`
  - Filter: Date IN within the last 3 years
    - From: within the last 3 years

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +441 more then Earliest 1 where date >= today - 1 year
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg2_hr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg2_hr_vs3`
  - Filter: Episode (First, New...)
  - Restriction: Earliest 1 where date >= today - 1 year
    - Condition: DATE IN | >= today - 1 year

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +268 more OR Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +35 more then Earliest 1 where date >= today - 1 year
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg2_hr_vs4`, `on_dm_reg_pg2_hr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg2_hr_vs4`, `on_dm_reg_pg2_hr_vs5`
  - Filter: Episode (First, New...)
  - Restriction: Earliest 1 where date >= today - 1 year
    - Condition: DATE IN | >= today - 1 year

### Rule 6 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value >= 15 and < 29
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg2_hr_vs6`
  - Restriction: Latest 1 where numeric value >= 15 and < 29
    - Condition: NUMERIC_VALUE IN | >= 15 and < 29

### Rule 7 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio then Latest 1 where numeric value > 70
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg2_hr_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg2_hr_vs7`
  - Restriction: Latest 1 where numeric value > 70
    - Condition: NUMERIC_VALUE IN | > 70


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### on Diabetes Register- LTC LCS Priority Group 2 (HR)
- `on_dm_reg_pg2_hr_vs1` (SNOMED, 3 codes): Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised | Cluster: IFCCHBAM_COD
- `on_dm_reg_pg2_hr_vs2` (SNOMED, 33 codes): O/E - Right diabetic foot - ulcerated, O/E - Left diabetic foot - ulcerated, Neuropathic diabetic ulcer - foot +30 more
- `on_dm_reg_pg2_hr_vs3` (SNOMED, 444 codes): Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +441 more | Cluster: CHD_COD
- `on_dm_reg_pg2_hr_vs4` (SNOMED, 271 codes): Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +268 more | Cluster: STRK_COD
- `on_dm_reg_pg2_hr_vs5` (SNOMED, 38 codes): Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +35 more | Cluster: TIA_COD
- `on_dm_reg_pg2_hr_vs6` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_dm_reg_pg2_hr_vs7` (SNOMED, 1 codes): Urine albumin:creatinine ratio