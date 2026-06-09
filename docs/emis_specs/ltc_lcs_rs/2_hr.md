<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1uox5bf1-7a1c-h3-0utt-06q0tcm0nn8t
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 2_HR

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 2_HR
Parent population: Based on "GROUP2- HR" search results

## Parent Chain
- GROUP2- HR: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Exclude patients who match Patients included in search GROUP1- HRC. Finally include patients who match Patients included in search On AF Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On CKD Register- LTC LCS Priority Group 2 (HR) OR patients included in search On CHD Register- LTC LCS Priority Group 2 (HR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 OR patients included in search On COPD Register- LTC LCS Priority Group 2 (HR) OR patients included in search On HF Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On PAD Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- None

## Target Report Logic
Start with based on "group2- hr" search results.

## Detailed Rule Logic
### Patient Details
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Patient Details [PATIENTS]

### Record of Interpreter Information
- Clause type: informational
- Pass: Informational
- Fail: Informational
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Interpreter needed, Interpreter present, Presence of interpreter +13 more
- Clinical Codes [EVENTS]
  - ValueSets: `2hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `2hr_vs1`


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP2- HR
- None
### 2_HR
- `2hr_vs1` (SNOMED, 16 codes): Interpreter needed, Interpreter present, Presence of interpreter +13 more