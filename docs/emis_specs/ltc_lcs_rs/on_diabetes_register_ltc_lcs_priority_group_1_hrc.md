<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0nwd4ju0-2il8-2f-0xd9-1de46p216pf0
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: on Diabetes Register- LTC LCS Priority Group 1 (HRC)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: on Diabetes Register- LTC LCS Priority Group 1 (HRC)
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Require Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 75. Finally include patients who match Clinical Codes [EVENTS] with Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failure +40 more OR Medication Issues [MEDICATION_ISSUES] with Insulins OR Exenatide, Liraglutide, Lixisenatide +2 more where Date of Issue within the last 6 months.

Boolean logic:
(Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 75) AND (Clinical Codes [EVENTS] with Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failure +40 more OR Medication Issues [MEDICATION_ISSUES] with Insulins OR Exenatide, Liraglutide, Lixisenatide +2 more where Date of Issue within the last 6 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 90
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg1_hrc_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs1`
  - Restriction: Latest 1 where numeric value > 90
    - Condition: NUMERIC_VALUE IN | > 90

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres then Latest 1 where numeric value < 15
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg1_hrc_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs2`
  - Restriction: Latest 1 where numeric value < 15
    - Condition: NUMERIC_VALUE IN | < 15

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio then Latest 1 where numeric value > 250
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg1_hrc_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs3`
  - Restriction: Latest 1 where numeric value > 250
    - Condition: NUMERIC_VALUE IN | > 250

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with ELF (Enhanced Liver Fibrosis) score, ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score +1 more then Latest 1 where numeric value > 9.8
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg1_hrc_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs4`
  - Restriction: Latest 1 where numeric value > 9.8
    - Condition: NUMERIC_VALUE IN | > 9.8

### Rule 5 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 75
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg1_hrc_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs1`
  - Restriction: Latest 1 where numeric value > 75
    - Condition: NUMERIC_VALUE IN | > 75

### Rule 6 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failure +40 more OR Medication Issues [MEDICATION_ISSUES] with Insulins OR Exenatide, Liraglutide, Lixisenatide +2 more where Date of Issue within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_dm_reg_pg1_hrc_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs5`
  - Filter: Episode (First, New...)
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_dm_reg_pg1_hrc_vs6`, `on_dm_reg_pg1_hrc_vs7`
  - Filter: Drug
    - Filter ValueSets: `on_dm_reg_pg1_hrc_vs6`, `on_dm_reg_pg1_hrc_vs7`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### on Diabetes Register- LTC LCS Priority Group 1 (HRC)
- `on_dm_reg_pg1_hrc_vs1` (SNOMED, 3 codes): Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised | Cluster: IFCCHBAM_COD
- `on_dm_reg_pg1_hrc_vs2` (SNOMED, 2 codes): GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation, eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres
- `on_dm_reg_pg1_hrc_vs3` (SNOMED, 1 codes): Urine albumin:creatinine ratio
- `on_dm_reg_pg1_hrc_vs4` (SNOMED, 4 codes): ELF (Enhanced Liver Fibrosis) score, ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score +1 more
- `on_dm_reg_pg1_hrc_vs5` (SNOMED, 43 codes): Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failure +40 more | Cluster: HF_COD
- `on_dm_reg_pg1_hrc_vs6` (Drug Group, 1 codes): Insulins
- `on_dm_reg_pg1_hrc_vs7` (SCT Const, 5 codes): Exenatide, Liraglutide, Lixisenatide +2 more