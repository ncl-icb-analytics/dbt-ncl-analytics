<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0odcr3z1-0vdx-le-0hc5-0z60oi002y2y
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# C&T completed FY

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: NAFLD Register*- EXTRA" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **LTC LCS: NAFLD Register*- EXTRA** — Require Clinical Codes with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +12 more. Exclude patients who match Patients included in search LTC LCS: NAFLD Register v2*. Include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
   - Combines: **LTC LCS: NAFLD Register v2***; **LTC LCS: AF Register***; **LTC LCS: CKD Register***; **LTC LCS: CHD Register***; **LTC LCS: Diabetes Register***; **LTC LCS: Hypertension Register***; **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: COPD Register***; **LTC LCS: HF Register***; **LTC LCS: PAD Register***; **LTC LCS: Stroke/TIA Register***
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 1 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `ct_completed_fy_vs1` (1 code)
  - Where date at least 01/04/2024 — `date >= 01/04/2024`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: NAFLD Register*- EXTRA | `nafld_reg_extra_vs1` |  |  | SNOMED | 16 | Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alco... | f5b3dad9 |
| C&T completed FY | `ct_completed_fy_vs1` |  | 1 | SNOMED | 1 | Chronic disease initial assessment | 2c55e088 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.