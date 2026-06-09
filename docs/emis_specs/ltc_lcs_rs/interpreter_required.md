<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 000nz0r0-wbwv-io-1f2e-0pz9p9i1uykk
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: Interpreter Required

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: Interpreter Required
Parent population: Based on "GROUP3- MR" search results

## Parent Chain
- GROUP3- MR: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Exclude patients who match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR. Finally include patients who match Patients included in search On AF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On CKD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On CHD Register- LTC LCS Priority Group 3 (MR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3 OR patients included in search On NAFLD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On COPD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On HF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On PAD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- None

## Target Report Logic
Start with based on "group3- mr" search results. Finally include patients who match Clinical Codes [EVENTS] with British Sign Language interpreter needed.

Boolean logic:
(Clinical Codes [EVENTS] with British Sign Language interpreter needed)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Interpreter not needed, Interpreter needed OR Interpreter needed then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `interpreter_required_vs1`, `interpreter_required_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `interpreter_required_vs1`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with British Sign Language interpreter needed
- Clinical Codes [EVENTS]
  - ValueSets: `interpreter_required_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `interpreter_required_vs3`


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP3- MR
- None
### Interpreter Required
- `interpreter_required_vs1` (SNOMED, 2 codes): Interpreter not needed, Interpreter needed
- `interpreter_required_vs2` (SNOMED, 1 codes): Interpreter needed
- `interpreter_required_vs3` (SNOMED, 1 codes): British Sign Language interpreter needed