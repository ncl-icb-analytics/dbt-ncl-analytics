<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 19hrq4u1-v9dc-su-1fh8-1u0teja1mk7g
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Hypertension Register- LTC LCS Priority Group 2 (HR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Hypertension Register*" (see below). Patients must match Rule 1 to stay in. Patients matching Rule 2 are excluded. A patient is included when they match Rule 3. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

1. **LTC LCS: Hypertension Register*** — Start with currently registered patients. Include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 3

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_pg2_hr_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_pg2_hr_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months

### Rule 2 of 3

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when:
- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 1 (HRC)**

### Rule 3 of 3

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **Priority Group 2a (ICB)**
- They appear in the results of the search **Priority Group 2b (ICB)**

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On Hypertension Register- LTC LCS Priority Group 2 (HR) | `on_htn_reg_pg2_hr_vs1` | CLINBP_COD | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| On Hypertension Register- LTC LCS Priority Group 2 (HR) | `on_htn_reg_pg2_hr_vs2` | HOMEAMBBP_COD | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |

## Caveats

- LTC LCS: Hypertension Register* references the EMIS library item `a5ff1b4e-f130-4fea-b11c-5b40dc9b0877`, whose logic is not included in this XML export. It is likely **Hypertension Register** (inferred from wrapper report "LTC LCS: Hypertension Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.