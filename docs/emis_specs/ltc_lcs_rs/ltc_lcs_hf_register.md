<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 19y06v11-1zvs-35-13pe-1ffcaft0fry2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: HF Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: HF Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: HF Register*: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23); wrapper reports: LTC LCS: HF Register*

## Target Report Logic
Start with currently registered patients. Finally include patients who match HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23).

Boolean logic:
(HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23))

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23)
- Library item: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23)


## ValueSet Friendly Names
### LTC LCS: HF Register*
- None