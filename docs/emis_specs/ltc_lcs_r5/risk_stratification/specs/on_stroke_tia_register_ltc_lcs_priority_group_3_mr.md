<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1k94gx00-4h27-ze-0487-1ngku9506vvs
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Stroke/TIA Register*" (see below). Patients must match Rule 2 to stay in. Patients matching Rules 1 and 4 are excluded. A patient is included when they match Rule 3. Rule 5 includes only patients who do NOT match it.

## Who we start with

1. **LTC LCS: Stroke/TIA Register*** — Start with currently registered patients. Include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 5

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)***
- They appear in the results of the search **On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)***

### Rule 2 of 5

Patients **must match** this rule to stay in. Those who match continue to Rule 3; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg3_mr_vs1` (1 code — cluster STRK_COD), or `on_stroketia_reg_pg3_mr_vs2` (1 code — cluster TIA_COD)
  - Where date before 1 year ago

### Rule 3 of 5

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg3_mr_vs3` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 2.5

### Rule 4 of 5

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when ANY of the following is true:
- **Medication Issues**
  - Code in: `on_stroketia_reg_pg3_mr_vs4` (6 codes)
  - Where drug code in `on_stroketia_reg_pg3_mr_vs4` (6 codes)
  - Where issue date within the last 6 months
- **Medication Courses**
  - Code in: `on_stroketia_reg_pg3_mr_vs5` (4 codes)
  - Where drug code in `on_stroketia_reg_pg3_mr_vs5` (4 codes)

### Rule 5 of 5

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ANY of the following is true:
- **Medication Issues**
  - Code in: `on_stroketia_reg_pg3_mr_vs6` (1 code — cluster STAT_COD)
  - Where drug code in `on_stroketia_reg_pg3_mr_vs6` (1 code — cluster STAT_COD)
  - Keep only the latest matching record, and require its issue date > today - 12 months
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg3_mr_vs6` (1 code — cluster STAT_COD)
  - Keep only the latest matching record, and require its date > today - 12 months
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg3_mr_vs7` (1 code — cluster STATINDEC_COD)
  - Keep only the latest matching record, and require its date > today - 1 year

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs1` | STRK_COD | SNOMED | 1 | Refset: 999005531000230105 | c8a23b04 |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs2` | TIA_COD | SNOMED | 1 | Refset: 999005291000230109 | babfa5e0 |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs3` |  | SNOMED | 4 | Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Se... | 561cf433 |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs4` |  | SCT_PREP | 6 | Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), ... | dd8f85b7 |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs5` |  | SCT Const | 4 | Atorvastatin, Simvastatin, Fluvastatin +1 more | 61715e8d |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs6` | STAT_COD | SNOMED | 1 | Refset: 12464001000001103 | 2ab5ff0c |
| On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)* | `on_stroketia_reg_pg3_mr_vs7` | STATINDEC_COD | SNOMED | 1 | Statin declined | ac08be53 |

## Caveats

- LTC LCS: Stroke/TIA Register* references the EMIS library item `d4e6f787-dbce-4f0b-9f3f-498808ebad42`, whose logic is not included in this XML export. It is likely **Stroke/TIA Register** (inferred from wrapper report "LTC LCS: Stroke/TIA Register*"), but this is not certain. Verify it in EMIS before implementing.
- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.