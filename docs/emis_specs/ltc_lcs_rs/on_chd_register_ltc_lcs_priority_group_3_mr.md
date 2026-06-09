<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 140bhp90-fwpk-8g-1a5y-120mjq80ukog
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On CHD Register- LTC LCS Priority Group 3 (MR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On CHD Register- LTC LCS Priority Group 3 (MR)
Parent population: Based on "LTC LCS: CHD Register*" search results

## Parent Chain
- LTC LCS: CHD Register*: Start with currently registered patients. Finally include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).
  Library refs: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)

## Library Items
- LTC LCS: CHD Register*: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c); wrapper reports: LTC LCS: CHD Register*

## Target Report Logic
Start with based on "ltc lcs: chd register*" search results. Require Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date before 1 year ago. Exclude patients who match Patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 2 (HR). Finally include patients who match Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Upjohn UK Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more then Latest 1 where numeric value > 2.5.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date before 1 year ago) AND NOT (patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 2 (HR)) AND (Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Upjohn UK Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more then Latest 1 where numeric value > 2.5)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 2 (HR)
- Population ref: On CHD Register- LTC LCS Priority Group 1 (HRC) (6c2418f3-b92c-4d74-8df7-de41376923e3)
- Population ref: On CHD Register- LTC LCS Priority Group 2 (HR) (126ded20-7d2a-47b4-810b-db26e3ece6c9)

### Rule 2 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Refset: 999000771000230107 where Date before 1 year ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_chd_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_chd_reg_pg3_mr_vs1`
  - Filter: Date IN before 1 year ago
    - To: before 1 year ago

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Medication Issues [MEDICATION_ISSUES] with Atorvastatin 80mg tablets, Lipitor 80mg tablets (Upjohn UK Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more then Latest 1 where numeric value > 2.5
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_chd_reg_pg3_mr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_chd_reg_pg3_mr_vs2`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_chd_reg_pg3_mr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_chd_reg_pg3_mr_vs3`
  - Restriction: Latest 1 where numeric value > 2.5
    - Condition: NUMERIC_VALUE IN | > 2.5


## ValueSet Friendly Names
### LTC LCS: CHD Register*
- None
### On CHD Register- LTC LCS Priority Group 3 (MR)
- `on_chd_reg_pg3_mr_vs1` (SNOMED, 1 codes): Refset: 999000771000230107 | Cluster: CHD_COD
- `on_chd_reg_pg3_mr_vs2` (SCT_PREP, 6 codes): Atorvastatin 80mg tablets, Lipitor 80mg tablets (Upjohn UK Ltd), Crestor 20mg tablets (AstraZeneca UK Ltd) +3 more
- `on_chd_reg_pg3_mr_vs3` (SNOMED, 4 codes): Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Serum non HDL (high density lipoprotein) cholesterol level +1 more