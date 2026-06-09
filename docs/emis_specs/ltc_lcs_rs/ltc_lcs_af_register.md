<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0ye6yro1-sx38-k7-1lqr-198r3ds0hddi
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: AF Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: AF Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: AF Register*: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf); wrapper reports: LTC LCS: AF Register*

## Target Report Logic
Start with currently registered patients. Finally include patients who match AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).

Boolean logic:
(AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf))

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf)
- Library item: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf)


## ValueSet Friendly Names
### LTC LCS: AF Register*
- None