<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0g64hz30-4kk5-w0-1kzw-1kk9x3u0m9f5
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On NAFLD Register- LTC LCS Priority Group 3 (MR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On NAFLD Register- LTC LCS Priority Group 3 (MR)
Parent population: Based on "LTC LCS: NAFLD Register v2*" search results

## Parent Chain
- LTC LCS: NAFLD Register v2*: Start with currently registered patients. Finally include patients who match Clinical Codes [EVENTS] with Metabolic dysfunction-associated steatotic liver disease, Metabolic dysfunction-associated steatohepatitis, Metabolic dysfunction-associated steatotic liver.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: nafld register v2*" search results. Finally include patients who match Clinical Codes [EVENTS] with NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score +8 more where Date within the last 3 years then Latest 1 where numeric value > 1.3 and <= 3.25 AND Clinical Codes [EVENTS] with ELF (Enhanced Liver Fibrosis) score, ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score +1 more where Date within the last 3 years then Latest 1 where numeric value >= 9.8.

Boolean logic:
(Clinical Codes [EVENTS] with NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score +8 more where Date within the last 3 years then Latest 1 where numeric value > 1.3 and <= 3.25 AND Clinical Codes [EVENTS] with ELF (Enhanced Liver Fibrosis) score, ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score +1 more where Date within the last 3 years then Latest 1 where numeric value >= 9.8)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score +8 more where Date within the last 3 years then Latest 1 where numeric value > 3.25
- Clinical Codes [EVENTS]
  - ValueSets: `on_nafld_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_nafld_reg_pg3_mr_vs1`
  - Filter: Date IN within the last 3 years
    - From: within the last 3 years
  - Restriction: Latest 1 where numeric value > 3.25
    - Condition: NUMERIC_VALUE IN | > 3.25

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score +8 more where Date within the last 3 years then Latest 1 where numeric value > 1.3 and <= 3.25 AND Clinical Codes [EVENTS] with ELF (Enhanced Liver Fibrosis) score, ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score +1 more where Date within the last 3 years then Latest 1 where numeric value >= 9.8
- Clinical Codes [EVENTS]
  - ValueSets: `on_nafld_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_nafld_reg_pg3_mr_vs1`
  - Filter: Date IN within the last 3 years
    - From: within the last 3 years
  - Restriction: Latest 1 where numeric value > 1.3 and <= 3.25
    - Condition: NUMERIC_VALUE IN | > 1.3 and <= 3.25
- Clinical Codes [EVENTS]
  - ValueSets: `on_nafld_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_nafld_reg_pg3_mr_vs2`
  - Filter: Date IN within the last 3 years
    - From: within the last 3 years
  - Restriction: Latest 1 where numeric value >= 9.8
    - Condition: NUMERIC_VALUE IN | >= 9.8


## ValueSet Friendly Names
### LTC LCS: NAFLD Register v2*
- `nafld_reg_v2_vs1` (SNOMED, 12 codes): Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +9 more
- `nafld_reg_v2_vs2` (SNOMED, 3 codes): Metabolic dysfunction-associated steatotic liver disease, Metabolic dysfunction-associated steatohepatitis, Metabolic dysfunction-associated steatotic liver
### On NAFLD Register- LTC LCS Priority Group 3 (MR)
- `on_nafld_reg_pg3_mr_vs1` (SNOMED, 11 codes): NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score +8 more
- `on_nafld_reg_pg3_mr_vs2` (SNOMED, 4 codes): ELF (Enhanced Liver Fibrosis) score, ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score +1 more