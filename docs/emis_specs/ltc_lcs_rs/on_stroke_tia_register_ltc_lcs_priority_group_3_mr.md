<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1k94gx00-4h27-ze-0487-1ngku9506vvs
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*
Parent population: Based on "LTC LCS: Stroke/TIA Register*" search results

## Parent Chain
- LTC LCS: Stroke/TIA Register*: Start with currently registered patients. Finally include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).
  Library refs: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42)

## Library Items
- LTC LCS: Stroke/TIA Register*: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42); wrapper reports: LTC LCS: Stroke/TIA Register*

## Target Report Logic
Start with based on "ltc lcs: stroke/tia register*" search results. Require Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 where Date before 1 year ago. Exclude patients who match Patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*; Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months OR Medication Courses [MEDICATION_COURSES] with Atorvastatin, Simvastatin, Fluvastatin +1 more. Finally include patients who do not match Medication Issues [MEDICATION_ISSUES] with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Statin declined then Latest 1 where date > today - 1 year.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 where Date before 1 year ago) AND NOT (patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*) AND NOT (Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months OR Medication Courses [MEDICATION_COURSES] with Atorvastatin, Simvastatin, Fluvastatin +1 more) AND NOT (Medication Issues [MEDICATION_ISSUES] with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Statin declined then Latest 1 where date > today - 1 year)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*
- Population ref: On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* (f352f100-8bef-45f7-a6f5-95616b326015)
- Population ref: On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* (cea8e923-831d-4e42-89e2-9f409b510f6d)

### Rule 2 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999005531000230105 OR Refset: 999005291000230109 where Date before 1 year ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs1`, `on_stroketia_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs1`, `on_stroketia_reg_pg3_mr_vs2`
  - Filter: Date IN before 1 year ago
    - To: before 1 year ago

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more then Latest 1 where numeric value > 2.5
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs3`
  - Restriction: Latest 1 where numeric value > 2.5
    - Condition: NUMERIC_VALUE IN | > 2.5

### Rule 4 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months OR Medication Courses [MEDICATION_COURSES] with Atorvastatin, Simvastatin, Fluvastatin +1 more
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs4`
  - Filter: Drug
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs4`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Courses [MEDICATION_COURSES]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs5`
  - Filter: Course Status (Current, Past etc)
  - Filter: Prescription Type
  - Filter: Drug
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs5`

### Rule 5 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: Medication Issues [MEDICATION_ISSUES] with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Statin declined then Latest 1 where date > today - 1 year
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs6`
  - Filter: Drug
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs6`
  - Filter: Date of Issue
    - To: <=
  - Restriction: Latest 1 where issue date > today - 12 months
    - Condition: ISSUE_DATE IN | > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs6`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_stroketia_reg_pg3_mr_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_stroketia_reg_pg3_mr_vs7`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date > today - 1 year
    - Condition: DATE IN | > today - 1 year


## ValueSet Friendly Names
### LTC LCS: Stroke/TIA Register*
- None
### On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*
- `on_stroketia_reg_pg3_mr_vs1` (SNOMED, 1 codes): Refset: 999005531000230105 | Cluster: STRK_COD
- `on_stroketia_reg_pg3_mr_vs2` (SNOMED, 1 codes): Refset: 999005291000230109 | Cluster: TIA_COD
- `on_stroketia_reg_pg3_mr_vs3` (SNOMED, 4 codes): Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more
- `on_stroketia_reg_pg3_mr_vs4` (SCT_PREP, 6 codes): Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more
- `on_stroketia_reg_pg3_mr_vs5` (SCT Const, 4 codes): Atorvastatin, Simvastatin, Fluvastatin +1 more
- `on_stroketia_reg_pg3_mr_vs6` (SNOMED, 1 codes): Refset: 12464001000001103 | Cluster: STAT_COD
- `on_stroketia_reg_pg3_mr_vs7` (SNOMED, 1 codes): Statin declined | Cluster: STATINDEC_COD