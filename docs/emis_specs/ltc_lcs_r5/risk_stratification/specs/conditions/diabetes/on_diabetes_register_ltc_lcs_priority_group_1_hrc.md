<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0nwd4ju0-2il8-2f-0xd9-1de46p216pf0
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# on Diabetes Register- LTC LCS Priority Group 1 (HRC)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Diabetes Register*" (see below). Patients must match Rule 5 to stay in. A patient is included when they match any one of Rules 1-4 and 6. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Diabetes Register*** — Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Continue to Rule 2 | Inclusion route |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | Continue to Rule 6 | Excluded | Filter — must match |
| 6 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Latest IFCC HbA1c equals or under 75"*
  - Code in: `on_dm_reg_pg1_hrc_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 90

### Rule 2 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs2` (2 codes)
  - Keep only the latest matching record, and require its numeric value < 15

### Rule 3 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs3` (1 code)
  - Keep only the latest matching record, and require its numeric value > 250

### Rule 4 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs4` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 9.8

### Rule 5 of 6 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 6; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Latest IFCC HbA1c equals or under 75"*
  - Code in: `on_dm_reg_pg1_hrc_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 75

### Rule 6 of 6 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Heart Failure Patients"*
  - Code in: `on_dm_reg_pg1_hrc_vs5` (43 codes — cluster HF_COD)
  - Where episode type is not Review or Ended
- **Criterion B — Medication Issues**
  - Code in: `on_dm_reg_pg1_hrc_vs6` (1 code), or `on_dm_reg_pg1_hrc_vs7` (5 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs1` | IFCCHBAM_COD | 1, 5 | SNOMED | 3 | Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference ... | 95d9e41a |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs2` |  | 2 | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs3` |  | 3 | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs4` |  | 4 | SNOMED | 4 | ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score, Ass... | e1c7ed45 |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs5` | HF_COD | 6 | SNOMED | 43 | Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failur... | 0c87993d |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs6` |  | 6 | Drug Group | 1 | Insulins | 0bd7c4bb |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs7` |  | 6 | SCT Const | 5 | Exenatide, Liraglutide, Lixisenatide +2 more | 6cbac172 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.