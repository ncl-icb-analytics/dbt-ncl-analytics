<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0v8wf381-yh31-o5-1bu5-0eu2jd91lu1u
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# on Diabetes Register- LTC LCS Priority Group 3A (MRa)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Diabetes Register*" (see below). Patients must match Rule 2 to stay in. Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 3-6. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Diabetes Register*** — Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | Continue to Rule 3 | Excluded | Filter — must match |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Continue to Rule 6 | Inclusion route |
| 6 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 6 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **on Diabetes Register- LTC LCS Priority Group 1 (HRC)**
- They appear in the results of the search **on Diabetes Register- LTC LCS Priority Group 2 (HR)**

### Rule 2 of 6 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 3; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Latest IFCC HbA1c equals or under 75"*
  - Code in: `on_dm_reg_priority_group_3a_mra_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 58 and <= 75

### Rule 3 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"CHD Register"*
  - Code in: `on_dm_reg_priority_group_3a_mra_vs2` (542 codes — cluster CHD_COD)
  - Where date within the last 12 months — `date > today - 12 months`
- **Criterion B — Clinical Codes** (clinical events)
  *"Stroke / TIA Register"*
  - Code in: `on_dm_reg_priority_group_3a_mra_vs3` (348 codes — cluster STRK_COD), or `on_dm_reg_priority_group_3a_mra_vs4` (42 codes — cluster TIA_COD)
  - Where date within the last 12 months — `date > today - 12 months`

### Rule 4 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3a_mra_vs5` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 30 and < 44

### Rule 5 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3a_mra_vs6` (1 code)
  - Keep only the latest matching record, and require its numeric value > 30

### Rule 6 of 6 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading done in last 12 months"*
  - Code in: `on_dm_reg_priority_group_3a_mra_vs7` (190 codes — cluster BP_COD)
  - Keep only the latest 1000 matching records
  - **Linked record A.1** — join: same date as record A
    *"Latest Systolic BP equal or less than 150"*
    - Code in: `on_dm_reg_priority_group_3a_mra_vs8` (43 codes — cluster Systolic Blood Pressure)
    - Keep only the latest 1000 matching records
    - **Linked record A.1.1** — join: same date as record A.1
      *"Latest Diastolic BP equal or less than 90"*
      - Code in: `on_dm_reg_priority_group_3a_mra_vs9` (43 codes)
      - Keep only the latest matching record, and require its numeric value >= 140
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading done in last 12 months"*
  - Code in: `on_dm_reg_priority_group_3a_mra_vs7` (190 codes — cluster BP_COD)
  - Keep only the latest 1000 matching records
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic BP equal or less than 150"*
    - Code in: `on_dm_reg_priority_group_3a_mra_vs8` (43 codes — cluster Systolic Blood Pressure)
    - Keep only the latest 1000 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic BP equal or less than 90"*
      - Code in: `on_dm_reg_priority_group_3a_mra_vs10` (46 codes — cluster Diastolic Blood Pressure)
      - Keep only the latest matching record, and require its numeric value >= 90

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs1` | IFCCHBAM_COD | 2 | SNOMED | 3 | Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference ... | 95d9e41a |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs10` | Diastolic Blood Pressure | 6 | SNOMED | 46 | Average diastolic blood pressure, Average night interval diastolic blood pres... | 3f1e99d8 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs2` | CHD_COD | 3 | SNOMED | 542 | Mural thrombus of right ventricle following acute myocardial infarction, Post... | e6c095a3 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs3` | STRK_COD | 3 | SNOMED | 348 | Thrombosis of left middle cerebral artery, Left middle cerebral artery thromb... | 82f1a338 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs4` | TIA_COD | 3 | SNOMED | 42 | Transient cerebral ischemia, Anterior circulation transient ischaemic attack,... | 84cf9628 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs5` |  | 4 | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs6` |  | 5 | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs7` | BP_COD | 6 | SNOMED | 190 | O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measure... | 44f87e74 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs8` | Systolic Blood Pressure | 6 | SNOMED | 43 | Average systolic blood pressure, Average night interval systolic blood pressu... | 8da5fff1 |
| on Diabetes Register- LTC LCS Priority Group 3A (MRa) | `on_dm_reg_priority_group_3a_mra_vs9` |  | 6 | SNOMED | 43 | Average systolic blood pressure, Average night interval systolic blood pressu... | 7a882230 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.