<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0c60eyb1-7xyv-8n-1caw-0vehb9q1woir
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Hypertension Register*" (see below). Patients must match Rules 1 and 3 to stay in. Patients matching Rules 2 and 4 are excluded. A patient is included when they do NOT match Rule 5. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Hypertension Register*** — Include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Continue to Rule 2 | Excluded | Filter — must match |
| 2 | Excluded | Continue to Rule 3 | Exclusion |
| 3 | Continue to Rule 4 | Excluded | Filter — must match |
| 4 | Excluded | Continue to Rule 5 | Exclusion |
| 5 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 5 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3a_mra_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months — `date > today - 12 months`

### Rule 2 of 5 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3**
- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 2 (HR) v3**

### Rule 3 of 5 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 4; those who do not are excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"CHD Register"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs3` (444 codes — cluster CHD_COD)
  - Where episode type is not Review or Ended
- **Criterion B — Clinical Codes** (clinical events)
  *"Stroke / TIA Register"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs4` (271 codes — cluster STRK_COD), or `on_htn_reg_priority_group_3a_mra_v3_vs5` (38 codes — cluster TIA_COD)
  - Where episode type is not Review or Ended
- **Criterion C — Clinical Codes** (clinical events)
  *"PAD Register"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs6` (32 codes — cluster PAD_COD)
  - Where episode type is not Review or Ended
- **Criterion D — Clinical Codes** (clinical events)
  *"Latest Occurrence of CKD Stage 3 to 5"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs7` (106 codes — cluster CKD_COD)
- **Criterion E — Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs8` (1 code)
  - Keep only the latest matching record, and require its numeric value < 60
- **Criterion F — Clinical Codes** (clinical events)
  *"Earliest Occurrence of Diabetes"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs9` (527 codes — cluster DM_COD)
- **Criterion G — Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs10` (78 codes)

### Rule 4 of 5 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3a_mra_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months — `date > today - 12 months`
  - Keep only the latest 100 matching records
  - **Linked record A.1** — join: same date as record A
    *"Latest Systolic HOMEBP/BPEXHOME"*
    - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs11` (49 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record A.1.1** — join: same date as record A.1
      *"Latest Diastolic HOMEBP/BPEXHOME"*
      - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs12` (45 codes — cluster Diastolic Blood Pressure), or `on_htn_reg_priority_group_3a_mra_v3_vs1` (1 code — cluster CLINBP_COD)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs1` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic BPEXHOME equal or less than 140"*
    - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (36 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic BPEXHOME equal or less than 90"*
      - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs14` (32 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic BPEXHOME equal or less than 150"*
        - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 140

### Rule 5 of 5 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at home in last 12 months"*
  - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"Blood Pressure reading excluding home done in last 12 months"*
      - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs1` (1 code — cluster CLINBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record A.1.1** — join: same date as record A.1
        *"Latest Systolic BPEXHOME equal or less than 140"*
        - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record A.1.1.1** — join: same date as record A.1.1
          *"Latest Diastolic BPEXHOME equal or less than 90"*
          - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs14` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **A.2 — Clinical Codes** (clinical events)
      *"Blood Pressure reading done at Home in last 12 months"*
      - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record A.2.1** — join: same date as record A.2
        *"Latest Systolic HOMEBP equal or less than 135"*
        - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record A.2.1.1** — join: same date as record A.2.1
          *"Latest Diastolic HOMEBP equal or less than 85"*
          - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs15` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - **Linked record A.3** — join: same date as record A
    *"Blood Pressure reading done at Home in last 12 months"*
    - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
    - Keep only the latest 100 matching records
    - **Linked record A.3.1** — join: same date as record A.3
      *"Latest Systolic HOMEBP equal or less than 135"*
      - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record
      - **Linked record A.3.1.1** — join: same date as record A.3.1
        *"Latest Diastolic HOMEBP equal or less than 85"*
        - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs15` (13 codes — cluster Diastolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value > 0
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at Home in last 12 months"*
  - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic HOMEBP equal or less than 145"*
    - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (13 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic HOMEBP equal or less than 85"*
      - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs15` (13 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic HOMEBP equal or less than 145"*
        - Code in: `on_htn_reg_priority_group_3a_mra_v3_vs13` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 135

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs1` | CLINBP_COD | 1, 4, 5 | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs10` |  | 3 | SNOMED | 78 | Black African, Black Caribbean, Black Black - other +73 more | 88a4167f |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs11` | Systolic Blood Pressure | 4 | SNOMED | 49 | Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic bloo... | dbe8bf65 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs12` | Diastolic Blood Pressure | 4 | SNOMED | 45 | Minimum diastolic blood pressure, Minimum day interval diastolic blood pressu... | 94656e9b |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs13` | Systolic Blood Pressure | 4, 5 | SNOMED | 36 | Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood ... | 5b356e22 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs14` | Diastolic Blood Pressure | 4, 5 | SNOMED | 32 | Increased diastolic arterial pressure, High diastolic arterial pressure, Incr... | 2c57ad8d |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs15` | Diastolic Blood Pressure | 5 | SNOMED | 13 | Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, A... | 5f525c4f |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs2` | HOMEAMBBP_COD | 1, 4, 5 | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs3` | CHD_COD | 3 | SNOMED | 444 | Mural thrombus of right ventricle following acute myocardial infarction, Post... | e4c73e0d |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs4` | STRK_COD | 3 | SNOMED | 271 | Thrombosis of left middle cerebral artery, Left middle cerebral artery thromb... | bb3a48ed |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs5` | TIA_COD | 3 | SNOMED | 38 | Transient cerebral ischemia, Anterior circulation transient ischaemic attack,... | 8b1f1274 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs6` | PAD_COD | 3 | SNOMED | 32 | Claudication, Charcot  s syndrome, IC - Intermittent claudication +27 more | e5d7772c |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs7` | CKD_COD | 3 | SNOMED | 106 | Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occ... | 68fffd80 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs8` |  | 3 | SNOMED | 1 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 20855d05 |
| On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 | `on_htn_reg_priority_group_3a_mra_v3_vs9` | DM_COD | 3 | SNOMED | 527 | Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosi... | ea63b842 |

## Caveats

- LTC LCS: Hypertension Register* references the EMIS library item `a5ff1b4e-f130-4fea-b11c-5b40dc9b0877`, whose logic is not included in this XML export. It is likely **Hypertension Register** (inferred from wrapper report "LTC LCS: Hypertension Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.