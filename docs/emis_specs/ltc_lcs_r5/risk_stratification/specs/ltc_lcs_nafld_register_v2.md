<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1iuffjp0-gn7p-nn-05ld-12j6y4w1sgez
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: NAFLD Register v2*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. A patient is included when they match any one of Rules 1-2. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

Currently registered patients.

## Inclusion logic, step by step

### Rule 1 of 2

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `nafld_reg_v2_vs1` (12 codes)

### Rule 2 of 2

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `nafld_reg_v2_vs2` (3 codes)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: NAFLD Register v2* | `nafld_reg_v2_vs1` |  | SNOMED | 12 | Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alco... | 63fd8d6e |
| LTC LCS: NAFLD Register v2* | `nafld_reg_v2_vs2` |  | SNOMED | 3 | Metabolic dysfunction-associated steatotic liver disease, Metabolic dysfuncti... | 43d07f8b |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.