<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1veu9ss0-71jx-h6-0gtv-00uz2t60ghvy
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: Hypertension or Asthma Master

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: Hypertension or Asthma Master
Parent population: Based on "6. All on Hypertension or Asthma register" search results

## Parent Chain
- 6. All on Hypertension or Asthma register: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*.

## Library Items
- None

## Target Report Logic
Start with based on "6. all on hypertension or asthma register" search results. Finally include patients who do not match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR OR patients included in search GROUP3- MR OR patients included in search GROUP4- LR.

Boolean logic:
NOT (patients included in search GROUP1- HRC OR patients included in search GROUP2- HR OR patients included in search GROUP3- MR OR patients included in search GROUP4- LR)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: patients included in search GROUP1- HRC OR patients included in search GROUP2- HR OR patients included in search GROUP3- MR OR patients included in search GROUP4- LR
- Population ref: GROUP1- HRC (43aa0a5c-01eb-48b8-8e7b-a7afd007648e)
- Population ref: GROUP2- HR (092b82f6-facc-4a38-b5a8-72e356f9a5f1)
- Population ref: GROUP3- MR (fe774472-bb86-4e41-9a15-36f940fe154a)
- Population ref: GROUP4- LR (f91a7389-ea30-4ae7-9699-fbd48b8422d5)


## ValueSet Friendly Names
### 6. All on Hypertension or Asthma register
- None
### Hypertension or Asthma Master
- None