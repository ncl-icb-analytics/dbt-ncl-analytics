<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 075nh1m1-gfo0-ag-1901-07oktgn1whbk
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: Stroke/TIA Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: Stroke/TIA Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: Stroke/TIA Register*: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42); wrapper reports: LTC LCS: Stroke/TIA Register*

## Target Report Logic
Start with currently registered patients. Finally include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).

Boolean logic:
(Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42))

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42)
- Library item: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42)


## ValueSet Friendly Names
### LTC LCS: Stroke/TIA Register*
- None