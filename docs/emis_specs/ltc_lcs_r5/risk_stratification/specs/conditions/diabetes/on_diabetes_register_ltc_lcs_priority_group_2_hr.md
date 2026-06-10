<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1plpqim0-29ue-v2-1xc3-14d0kwn0psps
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# on Diabetes Register- LTC LCS Priority Group 2 (HR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Diabetes Register*" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-7. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Diabetes Register*** — Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Continue to Rule 6 | Inclusion route |
| 6 | **Included** | Continue to Rule 7 | Inclusion route |
| 7 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 7 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:

- They appear in the results of the search **on Diabetes Register- LTC LCS Priority Group 1 (HRC)**

### Rule 2 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Latest IFCC HbA1c equals or under 75"*
  - Code in: `on_dm_reg_pg2_hr_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 75 and <= 90

### Rule 3 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg2_hr_vs2` (33 codes)
  - Where date within the last 3 years — `date >= today - 3 years`

### Rule 4 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"CHD Register"*
  - Code in: `on_dm_reg_pg2_hr_vs3` (444 codes — cluster CHD_COD)
  - Where episode type is not Review or Ended
  - Keep only the earliest matching record, and require its date >= today - 1 year

### Rule 5 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Stroke / TIA Register"*
  - Code in: `on_dm_reg_pg2_hr_vs4` (271 codes — cluster STRK_COD), or `on_dm_reg_pg2_hr_vs5` (38 codes — cluster TIA_COD)
  - Where episode type is not Review or Ended
  - Keep only the earliest matching record, and require its date >= today - 1 year

### Rule 6 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 7.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg2_hr_vs6` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 15 and < 29

### Rule 7 of 7 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_pg2_hr_vs7` (1 code)
  - Keep only the latest matching record, and require its numeric value > 70

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs1` | IFCCHBAM_COD | 2 | SNOMED | 3 | Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference ... | 95d9e41a |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs2` |  | 3 | SNOMED | 33 | O/E - Right diabetic foot - ulcerated, O/E - Left diabetic foot - ulcerated, ... | 62608eed |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs3` | CHD_COD | 4 | SNOMED | 444 | Mural thrombus of right ventricle following acute myocardial infarction, Post... | e4c73e0d |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs4` | STRK_COD | 5 | SNOMED | 271 | Thrombosis of left middle cerebral artery, Left middle cerebral artery thromb... | bb3a48ed |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs5` | TIA_COD | 5 | SNOMED | 38 | Transient cerebral ischemia, Anterior circulation transient ischaemic attack,... | 8b1f1274 |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs6` |  | 6 | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| on Diabetes Register- LTC LCS Priority Group 2 (HR) | `on_dm_reg_pg2_hr_vs7` |  | 7 | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.