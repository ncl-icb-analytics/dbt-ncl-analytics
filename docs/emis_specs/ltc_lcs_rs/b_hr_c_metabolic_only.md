<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1cim9jp0-54lf-qa-06ri-0w2ccqr0p91k
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: B) HR+C- Metabolic only

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: B) HR+C- Metabolic only
Parent population: Based on "GROUP1- HRC" search results

## Parent Chain
- GROUP1- HRC: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Finally include patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- B) HR+C- Metabolic only: Unknown library item (3de35e4f-7964-4f24-a0b4-fd42930a1dd1)
- B) HR+C- Metabolic only: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c); wrapper reports: LTC LCS: CHD Register*
- B) HR+C- Metabolic only: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65); wrapper reports: LTC LCS: PAD Register*
- B) HR+C- Metabolic only: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42); wrapper reports: LTC LCS: Stroke/TIA Register*
- B) HR+C- Metabolic only: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23); wrapper reports: LTC LCS: HF Register*
- B) HR+C- Metabolic only: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)
- B) HR+C- Metabolic only: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*
- B) HR+C- Metabolic only: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf); wrapper reports: LTC LCS: AF Register*

## Target Report Logic
Start with based on "group1- hrc" search results. Exclude patients who match Patients included in search A) HR+C- Metabolic & Respiratory. Finally include patients who match Clinical Codes [EVENTS] with NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c) OR PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65) OR Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23) OR library item c913f5a7-1256-4de6-871e-23650e72765e OR Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877) OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).

Boolean logic:
NOT (patients included in search A) HR+C- Metabolic & Respiratory) AND (Clinical Codes [EVENTS] with NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c) OR PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65) OR Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23) OR library item c913f5a7-1256-4de6-871e-23650e72765e OR Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877) OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf))

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search A) HR+C- Metabolic & Respiratory
- Population ref: A) HR+C- Metabolic & Respiratory (08834f27-d026-48c5-8943-d10ffddef90d)

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c) OR PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65) OR Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23) OR library item c913f5a7-1256-4de6-871e-23650e72765e OR Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877) OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf)
- Library item: Unknown library item (3de35e4f-7964-4f24-a0b4-fd42930a1dd1)
- Library item: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)
- Library item: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65)
- Library item: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42)
- Library item: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23)
- Library item: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)
- Library item: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)
- Library item: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf)
- Clinical Codes [EVENTS]
  - ValueSets: `b_hrc_metabolic_only_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `b_hrc_metabolic_only_vs1`


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP1- HRC
- `high_risk_complexity_vs1` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
- `high_risk_complexity_vs2` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `high_risk_complexity_vs3` (SNOMED, 105 codes): Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +102 more
### B) HR+C- Metabolic only
- `b_hrc_metabolic_only_vs1` (SNOMED, 2 codes): NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver