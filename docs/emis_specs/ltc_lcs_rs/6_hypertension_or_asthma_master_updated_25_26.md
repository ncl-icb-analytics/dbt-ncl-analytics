<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 19f4w5z0-pzul-74-04u4-1a54mdr0uwi8
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 6_ Hypertension or Asthma Master updated 25-26

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 6_ Hypertension or Asthma Master updated 25-26
Parent population: Based on "Hypertension or Asthma Master" search results

## Parent Chain
- Hypertension or Asthma Master: Start with based on "6. all on hypertension or asthma register" search results. Finally include patients who do not match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR OR patients included in search GROUP3- MR OR patients included in search GROUP4- LR.
- 6. All on Hypertension or Asthma register: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*.

## Library Items
- None

## Target Report Logic
Start with based on "hypertension or asthma master" search results.

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
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs1`
  - Restriction: Latest 1

### Homeless
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Homeless, Homeless enhanced services administration, No longer homeless OR Homeless, Homeless enhanced services administration then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs2`, `6_htn_or_asthma_master_updated_25_26_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs2`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Housebound
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Housebound, No longer housebound OR Housebound then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs4`, `6_htn_or_asthma_master_updated_25_26_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs4`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Care Home resident
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Lives in care home, Provision of general practitioner intermediate care, Previously lived in care home OR Lives in care home, Provision of general practitioner intermediate care then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs6`, `6_htn_or_asthma_master_updated_25_26_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs6`
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
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs8`
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
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs9`
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
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs10`, `6_htn_or_asthma_master_updated_25_26_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs10`
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
  - ValueSets: `6_htn_or_asthma_master_updated_25_26_vs12`
  - Filter: Clinical Code
    - Filter ValueSets: `6_htn_or_asthma_master_updated_25_26_vs12`
  - Restriction: Latest 1


## ValueSet Friendly Names
### 6. All on Hypertension or Asthma register
- None
### Hypertension or Asthma Master
- None
### 6_ Hypertension or Asthma Master updated 25-26
- `6_htn_or_asthma_master_updated_25_26_vs1` (SNOMED, 16 codes): Interpreter needed, Interpreter present, Presence of interpreter +13 more
- `6_htn_or_asthma_master_updated_25_26_vs2` (SNOMED, 3 codes): Homeless, Homeless enhanced services administration, No longer homeless
- `6_htn_or_asthma_master_updated_25_26_vs3` (SNOMED, 2 codes): Homeless, Homeless enhanced services administration
- `6_htn_or_asthma_master_updated_25_26_vs4` (SNOMED, 2 codes): Housebound, No longer housebound
- `6_htn_or_asthma_master_updated_25_26_vs5` (SNOMED, 1 codes): Housebound
- `6_htn_or_asthma_master_updated_25_26_vs6` (SNOMED, 3 codes): Lives in care home, Provision of general practitioner intermediate care, Previously lived in care home
- `6_htn_or_asthma_master_updated_25_26_vs7` (SNOMED, 2 codes): Lives in care home, Provision of general practitioner intermediate care
- `6_htn_or_asthma_master_updated_25_26_vs8` (SNOMED, 1 codes): Hyperlipidaemia
- `6_htn_or_asthma_master_updated_25_26_vs9` (SNOMED, 12 codes): NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver disease, Fatty liver +9 more
- `6_htn_or_asthma_master_updated_25_26_vs10` (SNOMED, 2 codes): On severe mental illness register, Removed from severe mental illness register
- `6_htn_or_asthma_master_updated_25_26_vs11` (SNOMED, 1 codes): On severe mental illness register
- `6_htn_or_asthma_master_updated_25_26_vs12` (SNOMED, 6 codes): Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moderately frail +3 more