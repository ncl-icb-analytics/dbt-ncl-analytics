<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1qo890b1-70go-22-1fxk-0cltrea06cz7
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: C&T completed + Letter sent fy

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: C&T completed + Letter sent fy
Parent population: Based on "LTC LCS: NAFLD Register*- EXTRA" search results

## Parent Chain
- LTC LCS: NAFLD Register*- EXTRA: Start with currently registered patients. Require Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more. Exclude patients who match Patients included in search LTC LCS: NAFLD Register v2*. Finally include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: nafld register*- extra" search results. Require Clinical Codes [EVENTS] with Chronic disease initial assessment where Date at least 01/04/2024. Finally include patients who match Clinical Codes [EVENTS] with Long term condition summary sent to patient where Date within the last 1 year.

Boolean logic:
(Clinical Codes [EVENTS] with Chronic disease initial assessment where Date at least 01/04/2024) AND (Clinical Codes [EVENTS] with Long term condition summary sent to patient where Date within the last 1 year)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Chronic disease initial assessment where Date at least 01/04/2024
- Clinical Codes [EVENTS]
  - ValueSets: `ct_completed_letter_sent_fy_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `ct_completed_letter_sent_fy_vs1`
  - Filter: Date IN at least 01/04/2024
    - From: at least 01/04/2024

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Long term condition summary sent to patient where Date within the last 1 year
- Clinical Codes [EVENTS]
  - ValueSets: `ct_completed_letter_sent_fy_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `ct_completed_letter_sent_fy_vs2`
  - Filter: Date IN within the last 1 year
    - From: within the last 1 year


## ValueSet Friendly Names
### LTC LCS: NAFLD Register*- EXTRA
- `nafld_reg_extra_vs1` (SNOMED, 16 codes): Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more
### C&T completed + Letter sent fy
- `ct_completed_letter_sent_fy_vs1` (SNOMED, 1 codes): Chronic disease initial assessment
- `ct_completed_letter_sent_fy_vs2` (SNOMED, 1 codes): Long term condition summary sent to patient