<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1v1ngo81-i94p-2t-1cnc-1e52mti04bpw
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 5. LTC LCS: Asthma CYP register ONLY

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 5. LTC LCS: Asthma CYP register ONLY
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- None

## Target Report Logic
Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: Asthma CYP Register ONLY.

Boolean logic:
(patients included in search LTC LCS: Asthma CYP Register ONLY)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: patients included in search LTC LCS: Asthma CYP Register ONLY
- Population ref: LTC LCS: Asthma CYP Register ONLY (68eed77c-4afe-4639-b625-cb60db3e2c90)


## ValueSet Friendly Names
### 5. LTC LCS: Asthma CYP register ONLY
- None