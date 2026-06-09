<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1qh132m1-1fi8-om-1qmr-1vyxkuu0dma1
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: GROUP:CYPAST_LR_ONLY

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: [GROUP:CYPAST_LR_ONLY] On Asthma(CYP) Register- LOW RISK
Parent population: Based on "5. LTC LCS: Asthma CYP register ONLY" search results

## Parent Chain
- 5. LTC LCS: Asthma CYP register ONLY: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: Asthma CYP Register ONLY.

## Library Items
- None

## Target Report Logic
Start with based on "5. ltc lcs: asthma cyp register only" search results. Finally include patients who match Patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*.

Boolean logic:
(patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*
- Population ref: On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)* (0fe2f41b-d4ae-4328-b6f0-05baa80989f5)


## ValueSet Friendly Names
### 5. LTC LCS: Asthma CYP register ONLY
- None
### GROUP:CYPAST_LR_ONLY
- None