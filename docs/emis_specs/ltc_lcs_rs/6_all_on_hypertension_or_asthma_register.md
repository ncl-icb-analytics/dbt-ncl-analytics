<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0xz3f6g1-s3bi-x3-03pf-0k3vqus1kb3w
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 6. All on Hypertension or Asthma register

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 6. All on Hypertension or Asthma register
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- None

## Target Report Logic
Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*.

Boolean logic:
(patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*
- Population ref: LTC LCS: Asthma Adult Register* (179ff797-756f-476b-939c-43e0f14b1a1b)
- Population ref: LTC LCS: Asthma CYP Register* (06df4bbf-18e5-453c-9d8c-07712946d38b)
- Population ref: LTC LCS: Hypertension Register* (5b0680ae-1dc6-4536-a136-7ac7f925490a)


## ValueSet Friendly Names
### 6. All on Hypertension or Asthma register
- None