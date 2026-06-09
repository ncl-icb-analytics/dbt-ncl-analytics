<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1vtx3uz1-4dxy-i6-1c8x-1783r120qy7v
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: NAFLD Register*- EXTRA

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: NAFLD Register*- EXTRA
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- None

## Target Report Logic
Start with currently registered patients. Require Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more. Exclude patients who match Patients included in search LTC LCS: NAFLD Register v2*. Finally include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.

Boolean logic:
(Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more) AND NOT (patients included in search LTC LCS: NAFLD Register v2*) AND NOT (patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more
- Clinical Codes [EVENTS]
  - ValueSets: `nafld_reg_extra_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `nafld_reg_extra_vs1`

### Rule 2 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search LTC LCS: NAFLD Register v2*
- Population ref: LTC LCS: NAFLD Register v2* (1c184f0d-7d44-4af2-b242-0273912d40fe)

### Rule 3 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*
- Population ref: LTC LCS: AF Register* (033eaf88-393d-4931-8c1e-474b7fb99545)
- Population ref: LTC LCS: CKD Register* (513919d2-f8c4-4c34-a91e-b27a222da3a8)
- Population ref: LTC LCS: CHD Register* (416e627c-a7e0-4ea8-a58a-f83f2ba9c709)
- Population ref: LTC LCS: Diabetes Register* (2ca59240-560a-453d-8504-1c55c90846a1)
- Population ref: LTC LCS: Hypertension Register* (5b0680ae-1dc6-4536-a136-7ac7f925490a)
- Population ref: LTC LCS: Asthma Adult Register* (179ff797-756f-476b-939c-43e0f14b1a1b)
- Population ref: LTC LCS: Asthma CYP Register* (06df4bbf-18e5-453c-9d8c-07712946d38b)
- Population ref: LTC LCS: COPD Register* (ff6329a3-93b7-4e8b-b8ec-e4df9897ebd3)
- Population ref: LTC LCS: HF Register* (38a7b284-9308-4a07-8425-1be024d4cf62)
- Population ref: LTC LCS: PAD Register* (71c1f251-4fca-4036-8723-7fc2ea608588)
- Population ref: LTC LCS: Stroke/TIA Register* (e02a6544-3d30-46f5-8124-c9c8561d0b2f)


## ValueSet Friendly Names
### LTC LCS: NAFLD Register*- EXTRA
- `nafld_reg_extra_vs1` (SNOMED, 16 codes): Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +13 more