<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 113u00v0-lw5g-ik-0dq3-1fmi1w91jtkg
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025
Parent population: Based on "GROUP1- HRC" search results

## Parent Chain
- GROUP1- HRC: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Finally include patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- None

## Target Report Logic
Start with based on "group1- hrc" search results.

## Detailed Rule Logic
### Patient Details
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Patient Details [PATIENTS]

### Record of Interpreter Information
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Interpreter needed, Interpreter present, Presence of interpreter +13 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs1`
  - Restriction: Latest 1

### Homelessness
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Homeless, Homeless enhanced services administration, No longer homeless OR Homeless, Homeless enhanced services administration then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs2`, `1hrandcomplex_updated_complex_ltcs_aug_2025_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs2`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Housebound
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Housebound, No longer housebound OR Housebound then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs4`, `1hrandcomplex_updated_complex_ltcs_aug_2025_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs4`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Care Home resident
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Lives in care home, Provision of general practitioner intermediate care, Previously lived in care home OR Lives in care home, Provision of general practitioner intermediate care then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs6`, `1hrandcomplex_updated_complex_ltcs_aug_2025_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs6`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Cardiovascular disease
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Atrial Fibrillation
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Hypertension
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Hyperlipidaemia
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Hyperlipidaemia then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs8`
  - Restriction: Latest 1

### Diabetes
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Chronic Kidney Disease
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### NA Fatty Liver Disease
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver disease, Fatty liver +13 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs9`
  - Restriction: Latest 1

### COPD
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Asthma
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Serious MI
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with On severe mental illness register, Removed from severe mental illness register OR On severe mental illness register then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs10`, `1hrandcomplex_updated_complex_ltcs_aug_2025_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs10`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Learning Disability
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] then Latest 1
- Clinical Codes [EVENTS]
  - Filter: Clinical Code
  - Restriction: Latest 1

### Frailty
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moderately frail +3 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs12`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs12`
  - Restriction: Latest 1

### Smoking Status
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Ex-smoker, Cigarette smoker, Current smoker +24 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs13`
  - Filter: Clinical Code
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs13`
  - Restriction: Latest 1

### BMI
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Body mass index, Body mass index 30+ - obesity, Body mass index 25-29 - overweight +13 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs14`
  - Filter: Clinical Code
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs14`
  - Restriction: Latest 1

### O/E BP Reading
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with O/E - blood pressure reading then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs15`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs15`
  - Restriction: Latest 1

### HbA1c
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised, HbA1c (haemoglobin A1c) level (monitoring ranges) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised, HbA1c (haemoglobin A1c) level (diagnostic reference range) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs16`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs16`
  - Restriction: Latest 1

### eGFR
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres, eGFR (estimated glomerular filtration rate) using cystatin C Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres, GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation +1 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs17`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs17`
  - Restriction: Latest 1

### UACR
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Urine albumin:creatinine ratio, Urine albumin/creatinine ratio measurement, Urine protein/creatinine ratio +3 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs18`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs18`
  - Restriction: Latest 1

### TC
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Serum total cholesterol level, Total cholesterol level, Serum fasting total cholesterol +3 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs19`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs19`
  - Restriction: Latest 1

### LDL
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Serum low density lipoprotein cholesterol level, Calculated LDL (low density lipoprotein) cholesterol level, Plasma LDL (low density lipoprotein) cholesterol level +4 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs20`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs20`
  - Restriction: Latest 1

### MoC- Check & Test Appointment on
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Chronic disease initial assessment then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs21`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs21`
  - Restriction: Latest 1

### MoC- Follow-Up Appointment on
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Review of Personalised Care and Support Plan then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs22`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs22`
  - Restriction: Latest 1

### Rpt count
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Medication Courses [MEDICATION_COURSES]
- Medication Courses [MEDICATION_COURSES]
  - Filter: Prescription Type
  - Filter: Course Status (Current, Past etc)


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP1- HRC
- `high_risk_complexity_vs1` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
- `high_risk_complexity_vs2` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `high_risk_complexity_vs3` (SNOMED, 105 codes): Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +102 more
### 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs1` (SNOMED, 16 codes): Interpreter needed, Interpreter present, Presence of interpreter +13 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs2` (SNOMED, 3 codes): Homeless, Homeless enhanced services administration, No longer homeless
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs3` (SNOMED, 2 codes): Homeless, Homeless enhanced services administration
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs4` (SNOMED, 2 codes): Housebound, No longer housebound
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs5` (SNOMED, 1 codes): Housebound
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs6` (SNOMED, 3 codes): Lives in care home, Provision of general practitioner intermediate care, Previously lived in care home
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs7` (SNOMED, 2 codes): Lives in care home, Provision of general practitioner intermediate care
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs8` (SNOMED, 1 codes): Hyperlipidaemia
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs9` (SNOMED, 16 codes): NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver disease, Fatty liver +13 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs10` (SNOMED, 2 codes): On severe mental illness register, Removed from severe mental illness register
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs11` (SNOMED, 1 codes): On severe mental illness register
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs12` (SNOMED, 6 codes): Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moderately frail +3 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs13` (SNOMED, 27 codes): Ex-smoker, Cigarette smoker, Current smoker +24 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs14` (SNOMED, 16 codes): Body mass index, Body mass index 30+ - obesity, Body mass index 25-29 - overweight +13 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs15` (SNOMED, 1 codes): O/E - blood pressure reading
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs16` (SNOMED, 3 codes): Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised, HbA1c (haemoglobin A1c) level (monitoring ranges) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised, HbA1c (haemoglobin A1c) level (diagnostic reference range) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs17` (SNOMED, 4 codes): eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres, eGFR (estimated glomerular filtration rate) using cystatin C Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres, GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation +1 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs18` (SNOMED, 6 codes): Urine albumin:creatinine ratio, Urine albumin/creatinine ratio measurement, Urine protein/creatinine ratio +3 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs19` (SNOMED, 6 codes): Serum total cholesterol level, Total cholesterol level, Serum fasting total cholesterol +3 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs20` (SNOMED, 7 codes): Serum low density lipoprotein cholesterol level, Calculated LDL (low density lipoprotein) cholesterol level, Plasma LDL (low density lipoprotein) cholesterol level +4 more
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs21` (SNOMED, 1 codes): Chronic disease initial assessment
- `1hrandcomplex_updated_complex_ltcs_aug_2025_vs22` (SNOMED, 1 codes): Review of Personalised Care and Support Plan