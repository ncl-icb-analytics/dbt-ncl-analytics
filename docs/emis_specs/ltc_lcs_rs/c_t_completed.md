<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0x5daop1-8abs-hb-0y39-0y3kua10bz09
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# C&T completed

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: NAFLD Register*- EXTRA" (see below). A patient is included when they match Rule 1.

## Who we start with

1. **LTC LCS: NAFLD Register*- EXTRA** — Start with currently registered patients. Require Clinical Codes with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +12 more. Exclude patients who match Patients included in search LTC LCS: NAFLD Register v2*. Finally include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `ct_completed_vs1` (1 code)
  - Where date within the last 1 year

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: NAFLD Register*- EXTRA | `nafld_reg_extra_vs1` |  | SNOMED | 16 | Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alco... | f5b3dad9 |
| C&T completed | `ct_completed_vs1` |  | SNOMED | 1 | Chronic disease initial assessment | 2c55e088 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.