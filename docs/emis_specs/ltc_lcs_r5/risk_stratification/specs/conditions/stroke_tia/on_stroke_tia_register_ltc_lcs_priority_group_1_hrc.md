<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 18mja1v1-10zn-dd-0cdc-0a5u24d1qyue
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Stroke/TIA Register*" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **LTC LCS: Stroke/TIA Register*** — Include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 1 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Stroke / TIA Register"*
  - Code in: `on_stroketia_reg_pg1_hrc_vs1` (1 code — cluster STRK_COD), or `on_stroketia_reg_pg1_hrc_vs2` (1 code — cluster TIA_COD)
  - Where date within the last 30 days — `date > today - 30 days`
  - Where episode type is First or New or Flare Up
- **Criterion B — Clinical Codes** (clinical events)
  *"Stroke / TIA Register"*
  - Code in: `on_stroketia_reg_pg1_hrc_vs1` (1 code — cluster STRK_COD), or `on_stroketia_reg_pg1_hrc_vs2` (1 code — cluster TIA_COD)
  - Where date within the last 30 days — `date > today - 30 days`
  - Where problem significance is Significant

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* | `on_stroketia_reg_pg1_hrc_vs1` | STRK_COD | 1 | SNOMED | 1 | Refset: 999005531000230105 | c8a23b04 |
| On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* | `on_stroketia_reg_pg1_hrc_vs2` | TIA_COD | 1 | SNOMED | 1 | Refset: 999005291000230109 | babfa5e0 |
| On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* | `on_stroketia_reg_pg1_hrc_vs3` |  | 1 | Internal | 3 | First, New, Flare Up | bd7fde07 |
| On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)* | `on_stroketia_reg_pg1_hrc_vs4` |  | 1 | Internal | 1 | Significant | 8de0b3c4 |

## Caveats

- LTC LCS: Stroke/TIA Register* references the EMIS library item `d4e6f787-dbce-4f0b-9f3f-498808ebad42`, whose logic is not included in this XML export. It is likely **Stroke/TIA Register** (inferred from wrapper report "LTC LCS: Stroke/TIA Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.