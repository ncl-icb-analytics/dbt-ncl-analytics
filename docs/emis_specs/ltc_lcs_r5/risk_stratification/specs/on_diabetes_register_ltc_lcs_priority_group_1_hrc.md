<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0nwd4ju0-2il8-2f-0xd9-1de46p216pf0
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# on Diabetes Register- LTC LCS Priority Group 1 (HRC)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Diabetes Register*" (see below). Patients must match Rule 5 to stay in. A patient is included when they match any one of Rules 1-4 and 6.

## Who we start with

1. **LTC LCS: Diabetes Register*** — Start with currently registered patients. Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 90

### Rule 2 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs2` (2 codes)
  - Keep only the latest matching record, and require its numeric value < 15

### Rule 3 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs3` (1 code)
  - Keep only the latest matching record, and require its numeric value > 250

### Rule 4 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs4` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 9.8

### Rule 5 of 6

Patients **must match** this rule to stay in. Those who match continue to Rule 6; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 75

### Rule 6 of 6

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg1_hrc_vs5` (43 codes — cluster HF_COD)
- **Medication Issues**
  - Code in: `on_dm_reg_pg1_hrc_vs6` (1 code), or `on_dm_reg_pg1_hrc_vs7` (5 codes)
  - Where drug code in `on_dm_reg_pg1_hrc_vs6` (1 code), `on_dm_reg_pg1_hrc_vs7` (5 codes)
  - Where issue date within the last 6 months

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs1` | IFCCHBAM_COD | SNOMED | 3 | Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference ... | 95d9e41a |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs2` |  | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs3` |  | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs4` |  | SNOMED | 4 | ELF (Enhanced Liver Fibrosis) score, Enhanced Liver Fibrosis (ELF) score, Ass... | e1c7ed45 |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs5` | HF_COD | SNOMED | 43 | Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failur... | 0c87993d |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs6` |  | Drug Group | 1 | Insulins | 0bd7c4bb |
| on Diabetes Register- LTC LCS Priority Group 1 (HRC) | `on_dm_reg_pg1_hrc_vs7` |  | SCT Const | 5 | Exenatide, Liraglutide, Lixisenatide +2 more | 6cbac172 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.