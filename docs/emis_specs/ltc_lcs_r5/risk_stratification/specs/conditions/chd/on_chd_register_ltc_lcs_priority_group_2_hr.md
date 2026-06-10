<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0bohg7e1-ihf6-ep-1bek-0cfdbrx0h26c
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CHD Register- LTC LCS Priority Group 2 (HR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CHD Register*" (see below). Patients matching Rules 1-2 are excluded. A patient is included when they match Rule 3. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

1. **LTC LCS: CHD Register*** — Start with currently registered patients. Include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 3

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:
- They appear in the results of the search **On CHD Register- LTC LCS Priority Group 1 (HRC)**

### Rule 2 of 3

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_chd_reg_pg2_hr_vs1` (1 code — cluster CHD_COD)
  - Where date before 1 year ago

### Rule 3 of 3

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_chd_reg_pg2_hr_vs1` (1 code — cluster CHD_COD)
  - Where date within the last 1 year to before 1 month ago

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On CHD Register- LTC LCS Priority Group 2 (HR) | `on_chd_reg_pg2_hr_vs1` | CHD_COD | SNOMED | 1 | Refset: 999000771000230107 | d908caa0 |

## Caveats

- LTC LCS: CHD Register* references the EMIS library item `d730ee6f-1b38-4553-8f8e-7dc8b3042f4c`, whose logic is not included in this XML export. It is likely **CHD Register** (inferred from wrapper report "LTC LCS: CHD Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.