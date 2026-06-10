<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0g64hz30-4kk5-w0-1kzw-1kk9x3u0m9f5
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On NAFLD Register- LTC LCS Priority Group 3 (MR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: NAFLD Register v2*" (see below). A patient is included when they match any one of Rules 1-2.

## Who we start with

1. **LTC LCS: NAFLD Register v2*** — Start with currently registered patients. Include patients who match any of: Clinical Codes with Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alcoholic fatty liver disease +8 more; OR Clinical Codes with Metabolic dysfunction-associated steatotic liver disease, Metabolic dysfunction-associated steatohepatitis, Metabolic dysfunction-associated steatotic liver.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 2

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_nafld_reg_pg3_mr_vs1` (11 codes)
  - Where date within the last 3 years
  - Keep only the latest matching record, and require its numeric value > 3.25

### Rule 2 of 2

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_nafld_reg_pg3_mr_vs1` (11 codes)
  - Where date within the last 3 years
  - Keep only the latest matching record, and require its numeric value > 1.3 and <= 3.25
- **Clinical Codes** (clinical events)
  - Code in: `on_nafld_reg_pg3_mr_vs2` (4 codes)
  - Where date within the last 3 years
  - Keep only the latest matching record, and require its numeric value >= 9.8

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: NAFLD Register v2* | `nafld_reg_v2_vs1` |  | SNOMED | 12 | Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alco... | 63fd8d6e |
| LTC LCS: NAFLD Register v2* | `nafld_reg_v2_vs2` |  | SNOMED | 3 | Metabolic dysfunction-associated steatotic liver disease, Metabolic dysfuncti... | 43d07f8b |
| On NAFLD Register- LTC LCS Priority Group 3 (MR) | `on_nafld_reg_pg3_mr_vs1` |  | SNOMED | 11 | NAFLD (Non-Alcoholic Fatty Liver Disease) fibrosis score, Non-alcoholic fatty... | ca67a54a |
| On NAFLD Register- LTC LCS Priority Group 3 (MR) | `on_nafld_reg_pg3_mr_vs2` |  | SNOMED | 4 | ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score, Ass... | e1c7ed45 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.