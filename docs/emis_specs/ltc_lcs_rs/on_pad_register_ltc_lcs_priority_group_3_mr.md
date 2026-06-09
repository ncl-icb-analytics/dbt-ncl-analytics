<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1gvxokl1-vpyw-0a-1k5j-1ljfp7f1ihqg
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On PAD Register- LTC LCS Priority Group 3 (MR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On PAD Register- LTC LCS Priority Group 3 (MR)
Parent population: Based on "LTC LCS: PAD Register*" search results

## Parent Chain
- LTC LCS: PAD Register*: Start with currently registered patients. Finally include patients who match PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65).
  Library refs: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65)

## Library Items
- LTC LCS: PAD Register*: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65); wrapper reports: LTC LCS: PAD Register*

## Target Report Logic
Start with based on "ltc lcs: pad register*" search results. Require Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date before 12 months ago. Exclude patients who match Patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 2 (HR); Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months OR Medication Courses [MEDICATION_COURSES] with Atorvastatin, Simvastatin, Fluvastatin +1 more. Finally include patients who do not match Medication Issues [MEDICATION_ISSUES] with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Statin declined then Latest 1 where date > today - 1 year.

Boolean logic:
(Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date before 12 months ago) AND NOT (patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 2 (HR)) AND NOT (Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months OR Medication Courses [MEDICATION_COURSES] with Atorvastatin, Simvastatin, Fluvastatin +1 more) AND NOT (Medication Issues [MEDICATION_ISSUES] with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Statin declined then Latest 1 where date > today - 1 year)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 2 (HR)
- Population ref: On PAD Register- LTC LCS Priority Group 1 (HRC) (224df436-6871-45cc-b733-f063a59ba527)
- Population ref: On PAD Register- LTC LCS Priority Group 2 (HR) (1d652d9b-8185-4e19-8240-fe78ddbad843)

### Rule 2 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more where Date before 12 months ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs1`
  - Filter: Date IN before 12 months ago
    - To: before 12 months ago

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more then Latest 1 where numeric value > 2.5
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs2`
  - Restriction: Latest 1 where numeric value > 2.5
    - Condition: NUMERIC_VALUE IN | > 2.5

### Rule 4 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months OR Medication Courses [MEDICATION_COURSES] with Atorvastatin, Simvastatin, Fluvastatin +1 more
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_pad_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Courses [MEDICATION_COURSES]
  - ValueSets: `on_pad_reg_pg3_mr_vs4`
  - Filter: Course Status (Current, Past etc)
  - Filter: Prescription Type
  - Filter: Drug
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs4`

### Rule 5 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: Medication Issues [MEDICATION_ISSUES] with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Statin declined then Latest 1 where date > today - 1 year
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_pad_reg_pg3_mr_vs5`
  - Filter: Drug
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs5`
  - Filter: Date of Issue
    - To: <=
  - Restriction: Latest 1 where issue date > today - 12 months
    - Condition: ISSUE_DATE IN | > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg3_mr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs5`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_pad_reg_pg3_mr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_pad_reg_pg3_mr_vs6`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date > today - 1 year
    - Condition: DATE IN | > today - 1 year


## ValueSet Friendly Names
### LTC LCS: PAD Register*
- None
### On PAD Register- LTC LCS Priority Group 3 (MR)
- `on_pad_reg_pg3_mr_vs1` (SNOMED, 53 codes): Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +50 more
- `on_pad_reg_pg3_mr_vs2` (SNOMED, 4 codes): Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more
- `on_pad_reg_pg3_mr_vs3` (SCT_PREP, 6 codes): Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more
- `on_pad_reg_pg3_mr_vs4` (SCT Const, 4 codes): Atorvastatin, Simvastatin, Fluvastatin +1 more
- `on_pad_reg_pg3_mr_vs5` (SNOMED, 1 codes): Refset: 12464001000001103 | Cluster: STAT_COD
- `on_pad_reg_pg3_mr_vs6` (SNOMED, 1 codes): Statin declined | Cluster: STATINDEC_COD