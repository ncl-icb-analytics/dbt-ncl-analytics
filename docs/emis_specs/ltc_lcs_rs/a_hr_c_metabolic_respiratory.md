<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 13qjr8z0-9e94-ok-1hro-17g5owd11ozi
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: A) HR+C- Metabolic & Respiratory

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: A) HR+C- Metabolic & Respiratory
Parent population: Based on "GROUP1- HRC" search results

## Parent Chain
- GROUP1- HRC: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Finally include patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- A) HR+C- Metabolic & Respiratory: Unknown library item (3de35e4f-7964-4f24-a0b4-fd42930a1dd1)
- A) HR+C- Metabolic & Respiratory: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c); wrapper reports: LTC LCS: CHD Register*
- A) HR+C- Metabolic & Respiratory: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65); wrapper reports: LTC LCS: PAD Register*
- A) HR+C- Metabolic & Respiratory: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42); wrapper reports: LTC LCS: Stroke/TIA Register*
- A) HR+C- Metabolic & Respiratory: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23); wrapper reports: LTC LCS: HF Register*
- A) HR+C- Metabolic & Respiratory: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)
- A) HR+C- Metabolic & Respiratory: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*
- A) HR+C- Metabolic & Respiratory: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf); wrapper reports: LTC LCS: AF Register*
- A) HR+C- Metabolic & Respiratory: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Target Report Logic
Start with based on "group1- hrc" search results. Require Clinical Codes [EVENTS] with NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c) OR PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65) OR Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23) OR library item c913f5a7-1256-4de6-871e-23650e72765e OR Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877) OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf). Finally include patients who match Library item ee5b135f-b9b2-4ef7-8b51-939a754cf935.

Boolean logic:
(Clinical Codes [EVENTS] with NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c) OR PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65) OR Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23) OR library item c913f5a7-1256-4de6-871e-23650e72765e OR Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877) OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf)) AND (library item ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c) OR PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65) OR Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23) OR library item c913f5a7-1256-4de6-871e-23650e72765e OR Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877) OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf)
- Library item: Unknown library item (3de35e4f-7964-4f24-a0b4-fd42930a1dd1)
- Library item: CHD Register (d730ee6f-1b38-4553-8f8e-7dc8b3042f4c)
- Library item: PAD Register (ffccdb77-bd5e-47fc-add3-d700835ace65)
- Library item: Stroke/TIA Register (d4e6f787-dbce-4f0b-9f3f-498808ebad42)
- Library item: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23)
- Library item: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)
- Library item: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)
- Library item: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf)
- Clinical Codes [EVENTS]
  - ValueSets: `a_hrc_metabolic_respiratory_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `a_hrc_metabolic_respiratory_vs1`

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Asthma resolved, Asthma resolved OR Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co-occurrent and due to allergic asthma, Allergic asthma with status asthmaticus +230 more then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autohaler (3M Health Care Ltd), AeroBec 50 Autohaler (Meda Pharmaceuticals Ltd) +474 more where Date of Issue within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `a_hrc_metabolic_respiratory_vs2`, `a_hrc_metabolic_respiratory_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `a_hrc_metabolic_respiratory_vs2`, `a_hrc_metabolic_respiratory_vs3`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where SNOMED code IN: AST_COD
    - Condition: READCODE IN | AST_COD
    - Condition: DATE IN
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `a_hrc_metabolic_respiratory_vs4`
  - Filter: Drug
    - Filter ValueSets: `a_hrc_metabolic_respiratory_vs4`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months
  - Filter: Date of Issue
    - To: <=

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: library item ee5b135f-b9b2-4ef7-8b51-939a754cf935
- Library item: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)


## ValueSet Friendly Names
### LTC LCS Base*
- None
### LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only
- None
### GROUP1- HRC
- `high_risk_complexity_vs1` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
- `high_risk_complexity_vs2` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `high_risk_complexity_vs3` (SNOMED, 105 codes): Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +102 more
### A) HR+C- Metabolic & Respiratory
- `a_hrc_metabolic_respiratory_vs1` (SNOMED, 2 codes): NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver
- `a_hrc_metabolic_respiratory_vs2` (SNOMED, 2 codes): Asthma resolved, Asthma resolved | Cluster: ASTRES_COD
- `a_hrc_metabolic_respiratory_vs3` (SNOMED, 233 codes): Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co-occurrent and due to allergic asthma, Allergic asthma with status asthmaticus +230 more | Cluster: AST_COD
- `a_hrc_metabolic_respiratory_vs4` (SNOMED, 477 codes): Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autohaler (3M Health Care Ltd), AeroBec 50 Autohaler (Meda Pharmaceuticals Ltd) +474 more | Cluster: ASTTRT_COD