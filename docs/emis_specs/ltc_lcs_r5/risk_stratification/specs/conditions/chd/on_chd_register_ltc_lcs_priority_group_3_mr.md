<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 140bhp90-fwpk-8g-1a5y-120mjq80ukog
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CHD Register- LTC LCS Priority Group 3 (MR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CHD Register*" (see below). Patients must match Rule 2 to stay in. Patients matching Rule 1 are excluded. A patient is included when they match Rule 3. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: CHD Register*** — Include patients who match CHD Register (library item d730ee6f-1b38-4553-8f8e-7dc8b3042f4c).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | Continue to Rule 3 | Excluded | Filter — must match |
| 3 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 3 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **On CHD Register- LTC LCS Priority Group 1 (HRC)**
- They appear in the results of the search **On CHD Register- LTC LCS Priority Group 2 (HR)**

### Rule 2 of 3 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 3; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"CHD Register"*
  - Code in: `on_chd_reg_pg3_mr_vs1` (1 code — cluster CHD_COD)
  - Where date before 1 year ago — `date < today - 1 year`

### Rule 3 of 3 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Medication Issues**
  - Code in: `on_chd_reg_pg3_mr_vs2` (6 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_chd_reg_pg3_mr_vs3` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 2.5

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On CHD Register- LTC LCS Priority Group 3 (MR) | `on_chd_reg_pg3_mr_vs1` | CHD_COD | 2 | SNOMED | 1 | Refset: 999000771000230107 | d908caa0 |
| On CHD Register- LTC LCS Priority Group 3 (MR) | `on_chd_reg_pg3_mr_vs2` |  | 3 | SCT_PREP | 6 | Atorvastatin 80mg tablets, Lipitor 80mg tablets (Upjohn UK Ltd), Crestor 20mg... | dd8f85b7 |
| On CHD Register- LTC LCS Priority Group 3 (MR) | `on_chd_reg_pg3_mr_vs3` |  | 3 | SNOMED | 4 | Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Se... | 561cf433 |

## Caveats

- LTC LCS: CHD Register* references the EMIS library item `d730ee6f-1b38-4553-8f8e-7dc8b3042f4c`, whose logic is not included in this XML export. It is likely **CHD Register** (inferred from wrapper report "LTC LCS: CHD Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.