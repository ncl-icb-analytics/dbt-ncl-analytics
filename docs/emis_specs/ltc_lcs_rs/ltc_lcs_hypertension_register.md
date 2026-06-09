<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 06z54631-yn3v-ms-12t6-00ywm510fzel
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: Hypertension Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: Hypertension Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).

Boolean logic:
(Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877))

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)
- Library item: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None