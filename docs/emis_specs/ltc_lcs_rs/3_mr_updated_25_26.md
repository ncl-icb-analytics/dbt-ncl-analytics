<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1fdx5301-8jhm-3r-08a2-1v8ybsv02b2r
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 3_MR updated 25-26

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 3_MR updated 25-26
Parent population: Based on "GROUP3- MR" search results

## Parent Chain
- GROUP3- MR: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Exclude patients who match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR. Finally include patients who match Patients included in search On AF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On CKD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On CHD Register- LTC LCS Priority Group 3 (MR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3 OR patients included in search On NAFLD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On COPD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On HF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On PAD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- None

## Target Report Logic
Start with based on "group3- mr" search results.

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
  - ValueSets: `3mr_updated_25_26_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs1`
  - Restriction: Latest 1

### Homeless
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Homeless, Homeless enhanced services administration, No longer homeless OR Homeless, Homeless enhanced services administration then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `3mr_updated_25_26_vs2`, `3mr_updated_25_26_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs2`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Housebound
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Housebound, No longer housebound OR Housebound then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `3mr_updated_25_26_vs4`, `3mr_updated_25_26_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs4`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Care Home resident
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Lives in care home, Provision of general practitioner intermediate care, Previously lived in care home OR Lives in care home, Provision of general practitioner intermediate care then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `3mr_updated_25_26_vs6`, `3mr_updated_25_26_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs6`
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
  - ValueSets: `3mr_updated_25_26_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs8`
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
- Summary: Clinical Codes [EVENTS] with NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver disease, Fatty liver +9 more then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `3mr_updated_25_26_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs9`
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
  - ValueSets: `3mr_updated_25_26_vs10`, `3mr_updated_25_26_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs10`
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
  - ValueSets: `3mr_updated_25_26_vs12`
  - Filter: Clinical Code
    - Filter ValueSets: `3mr_updated_25_26_vs12`
  - Restriction: Latest 1


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP3- MR
- None
### 3_MR updated 25-26
- `3mr_updated_25_26_vs1` (SNOMED, 16 codes): Interpreter needed, Interpreter present, Presence of interpreter +13 more
- `3mr_updated_25_26_vs2` (SNOMED, 3 codes): Homeless, Homeless enhanced services administration, No longer homeless
- `3mr_updated_25_26_vs3` (SNOMED, 2 codes): Homeless, Homeless enhanced services administration
- `3mr_updated_25_26_vs4` (SNOMED, 2 codes): Housebound, No longer housebound
- `3mr_updated_25_26_vs5` (SNOMED, 1 codes): Housebound
- `3mr_updated_25_26_vs6` (SNOMED, 3 codes): Lives in care home, Provision of general practitioner intermediate care, Previously lived in care home
- `3mr_updated_25_26_vs7` (SNOMED, 2 codes): Lives in care home, Provision of general practitioner intermediate care
- `3mr_updated_25_26_vs8` (SNOMED, 1 codes): Hyperlipidaemia
- `3mr_updated_25_26_vs9` (SNOMED, 12 codes): NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver disease, Fatty liver +9 more
- `3mr_updated_25_26_vs10` (SNOMED, 2 codes): On severe mental illness register, Removed from severe mental illness register
- `3mr_updated_25_26_vs11` (SNOMED, 1 codes): On severe mental illness register
- `3mr_updated_25_26_vs12` (SNOMED, 6 codes): Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moderately frail +3 more