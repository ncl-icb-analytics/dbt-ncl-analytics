<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0g6ens51-770y-86-00f7-1so089p1yppq
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: CHD Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: CHD Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: CHD Register*: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c); wrapper reports: LTC LCS: CHD Register*

## Target Report Logic
Start with currently registered patients. Finally include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).

Boolean logic:
(CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c))

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)
- Library item: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)


## ValueSet Friendly Names
### LTC LCS: CHD Register*
- None