<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0sgtw000-82da-56-0dcx-19qt0tw1rbe6
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: C) HR+C- Respiratory only

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: C) HR+C- Respiratory only
Parent population: Based on "GROUP1- HRC" search results

## Parent Chain
- GROUP1- HRC: Start with based on "ltc lcs moc base excluding cyp only, lr htn only, lr adult asthma only" search results. Finally include patients who match Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
- LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only: Start with based on "ltc lcs base*" search results. Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
- LTC LCS Base*: Start with currently registered patients. Finally include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

## Library Items
- C) HR+C- Respiratory only: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Target Report Logic
Start with based on "group1- hrc" search results. Exclude patients who match Patients included in search A) HR+C- Metabolic & Respiratory. Finally include patients who match Library item ee5b135f-b9b2-4ef7-8b51-939a754cf935.

Boolean logic:
NOT (patients included in search A) HR+C- Metabolic & Respiratory) AND (library item ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search A) HR+C- Metabolic & Respiratory
- Population ref: A) HR+C- Metabolic & Respiratory (08834f27-d026-48c5-8943-d10ffddef90d)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Asthma resolved, Asthma resolved OR Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co-occurrent and due to allergic asthma, Allergic asthma with status asthmaticus +230 more then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autohaler (3M Health Care Ltd), AeroBec 50 Autohaler (Meda Pharmaceuticals Ltd) +474 more where Date of Issue within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `c_hrc_respiratory_only_vs1`, `c_hrc_respiratory_only_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `c_hrc_respiratory_only_vs1`, `c_hrc_respiratory_only_vs2`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where SNOMED code IN: AST_COD
    - Condition: READCODE IN | AST_COD
    - Condition: DATE IN
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `c_hrc_respiratory_only_vs3`
  - Filter: Drug
    - Filter ValueSets: `c_hrc_respiratory_only_vs3`
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
### C) HR+C- Respiratory only
- `c_hrc_respiratory_only_vs1` (SNOMED, 2 codes): Asthma resolved, Asthma resolved | Cluster: ASTRES_COD
- `c_hrc_respiratory_only_vs2` (SNOMED, 233 codes): Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co-occurrent and due to allergic asthma, Allergic asthma with status asthmaticus +230 more | Cluster: AST_COD
- `c_hrc_respiratory_only_vs3` (SNOMED, 477 codes): Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autohaler (3M Health Care Ltd), AeroBec 50 Autohaler (Meda Pharmaceuticals Ltd) +474 more | Cluster: ASTTRT_COD