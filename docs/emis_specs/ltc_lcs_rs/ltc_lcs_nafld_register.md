<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0mr4awn0-zgbu-v7-0xey-0sbwohg0uhvx
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: NAFLD Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: NAFLD Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- None

## Target Report Logic
Start with currently registered patients. Finally include patients who match Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more.

Boolean logic:
(Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more
- Clinical Codes [EVENTS]
  - ValueSets: `nafld_reg_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `nafld_reg_vs1`


## ValueSet Friendly Names
### LTC LCS: NAFLD Register*
- `nafld_reg_vs1` (SNOMED, 16 codes): Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more