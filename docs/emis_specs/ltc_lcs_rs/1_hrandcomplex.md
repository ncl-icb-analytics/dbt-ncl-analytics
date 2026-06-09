<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0cccj680-xl7j-65-0mmu-0jjfl4k0oqgg
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 1_HRandCOMPLEX

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 1_HRandCOMPLEX
Parent population: Based on "GROUP1- HRC" search results

## Parent Chain
- GROUP1- HRC: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Finally include patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- None

## Target Report Logic
Start with based on "group1- hrc" search results.

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
  - ValueSets: `1hrandcomplex_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `1hrandcomplex_vs1`


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP1- HRC
- `high_risk_complexity_vs1` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
- `high_risk_complexity_vs2` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `high_risk_complexity_vs3` (SNOMED, 105 codes): Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +102 more
### 1_HRandCOMPLEX
- `1hrandcomplex_vs1` (SNOMED, 16 codes): Interpreter needed, Interpreter present, Presence of interpreter +13 more