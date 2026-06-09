<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1td3uwv0-ehns-an-0jna-1o3slt0115le
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: PAD Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: PAD Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: PAD Register*: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65); wrapper reports: LTC LCS: PAD Register*

## Target Report Logic
Start with currently registered patients. Finally include patients who match PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65).

Boolean logic:
(PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65))

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65)
- Library item: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65)


## ValueSet Friendly Names
### LTC LCS: PAD Register*
- None