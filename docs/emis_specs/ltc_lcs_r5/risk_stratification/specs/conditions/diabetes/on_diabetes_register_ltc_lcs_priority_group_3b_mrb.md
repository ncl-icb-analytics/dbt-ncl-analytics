<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1sqm7ar1-48iw-4o-1a31-0obqzp51fiox
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# on Diabetes Register- LTC LCS Priority Group 3B (MRb)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Diabetes Register*" (see below). Patients must match Rule 3 to stay in. Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2 and 4-7. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Diabetes Register*** — Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | Continue to Rule 4 | Excluded | Filter — must match |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Continue to Rule 6 | Inclusion route |
| 6 | **Included** | Continue to Rule 7 | Inclusion route |
| 7 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 7 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **on Diabetes Register- LTC LCS Priority Group 1 (HRC)**
- They appear in the results of the search **on Diabetes Register- LTC LCS Priority Group 2 (HR)**
- They appear in the results of the search **on Diabetes Register- LTC LCS Priority Group 3A (MRa)**

### Rule 2 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Latest IFCC HbA1c equals or under 75"*
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 58 and <= 75

### Rule 3 of 7 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 4; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Latest IFCC HbA1c equals or under 75"*
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs1` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 48 and <= 58

### Rule 4 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs2` (2 codes)
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs3` (20 codes)
- **Criterion C — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs4` (5 codes)
  - Keep only the latest matching record, and require its numeric value > 35
- **Criterion D — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs5` (4 codes)
- **Criterion E — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs6` (6 codes)
- **Criterion F — Clinical Codes** (clinical events)
  *"Heart Failure Patients"*
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs7` (43 codes — cluster HF_COD)
  - Where episode type is not Review or Ended
- **Criterion G — Medication Issues**
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs8` (1 code), or `on_dm_reg_priority_group_3b_mrb_vs9` (5 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion H — Clinical Codes** (clinical events)
  *"PAD Register"*
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs10` (84 codes)

### Rule 5 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs11` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 45 and < 49

### Rule 6 of 7 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 7.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs12` (1 code)
  - Keep only the latest matching record, and require its numeric value >= 3 and <= 30

### Rule 7 of 7 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading done in last 12 months"*
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs13` (190 codes — cluster BP_COD)
  - Keep only the latest 1000 matching records
  - **Linked record A.1** — join: same date as record A
    *"Latest Systolic BP equal or less than 150"*
    - Code in: `on_dm_reg_priority_group_3b_mrb_vs14` (43 codes — cluster Systolic Blood Pressure)
    - Keep only the latest 1000 matching records
    - **Linked record A.1.1** — join: same date as record A.1
      *"Latest Diastolic BP equal or less than 90"*
      - Code in: `on_dm_reg_priority_group_3b_mrb_vs15` (43 codes)
      - Keep only the latest matching record, and require its numeric value >= 140
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading done in last 12 months"*
  - Code in: `on_dm_reg_priority_group_3b_mrb_vs13` (190 codes — cluster BP_COD)
  - Keep only the latest 1000 matching records
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic BP equal or less than 150"*
    - Code in: `on_dm_reg_priority_group_3b_mrb_vs14` (43 codes — cluster Systolic Blood Pressure)
    - Keep only the latest 1000 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic BP equal or less than 90"*
      - Code in: `on_dm_reg_priority_group_3b_mrb_vs16` (46 codes — cluster Diastolic Blood Pressure)
      - Keep only the latest matching record, and require its numeric value >= 90

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs1` | IFCCHBAM_COD | 2, 3 | SNOMED | 3 | Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference ... | 95d9e41a |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs10` |  | 4 | SNOMED | 84 | Claudication, Bilateral lower limb atherosclerosis pain at rest co-occurrent ... | c0c6b538 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs11` |  | 5 | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs12` |  | 6 | SNOMED | 1 | Urine albumin:creatinine ratio | 71e284a5 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs13` | BP_COD | 7 | SNOMED | 190 | O/E - Systolic BP reading, O/E - Diastolic BP reading, Blood pressure measure... | 44f87e74 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs14` | Systolic Blood Pressure | 7 | SNOMED | 43 | Average systolic blood pressure, Average night interval systolic blood pressu... | 8da5fff1 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs15` |  | 7 | SNOMED | 43 | Average systolic blood pressure, Average night interval systolic blood pressu... | 7a882230 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs16` | Diastolic Blood Pressure | 7 | SNOMED | 46 | Average diastolic blood pressure, Average night interval diastolic blood pres... | 3f1e99d8 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs2` |  | 4 | SNOMED | 2 | Erectile dysfunction, C/O erectile dysfunction | 9dcb714a |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs3` |  | 4 | SNOMED | 20 | Background diabetic retinopathy, Diabetic retinopathy, O/E - left eye backgro... | 6a54a4cd |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs4` |  | 4 | SNOMED | 5 | Body mass index, BMI - Body mass index, Weight: body mass +1 more | 435b40ad |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs5` |  | 4 | SNOMED | 4 | Diabetic neuropathy, Acute painful diabetic neuropathy, Chronic painful diabe... | 5a5b9110 |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs6` |  | 4 | SNOMED | 6 | O/E - Right diabetic foot at moderate risk, O/E - Left diabetic foot at moder... | 88f0f7dc |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs7` | HF_COD | 4 | SNOMED | 43 | Biventricular failure, Cardiac insufficiency, CCF - Congestive cardiac failur... | 0c87993d |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs8` |  | 4 | Drug Group | 1 | Insulins | 0bd7c4bb |
| on Diabetes Register- LTC LCS Priority Group 3B (MRb) | `on_dm_reg_priority_group_3b_mrb_vs9` |  | 4 | SCT Const | 5 | Exenatide, Liraglutide, Lixisenatide +2 more | 6cbac172 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.