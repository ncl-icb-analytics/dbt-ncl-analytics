<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1297zit1-t9ds-4t-0mu7-0k83oab0j1xy
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On PAD Register- LTC LCS Priority Group 2 (HR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: PAD Register*" (see below). Patients must match Rule 3 to stay in. Patients matching Rules 1, 4-5 and 7 are excluded. A patient is included when they match Rule 2. A patient is included unless they match every one of Rules 6 and 8 — failing any one of them includes the patient. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: PAD Register*** — Include patients who match PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | Continue to Rule 4 | Excluded | Filter — must match |
| 4 | Excluded | Continue to Rule 5 | Exclusion |
| 5 | Excluded | Continue to Rule 6 | Exclusion |
| 6 | Continue to Rule 7 | **Included** | Inclusion route (on no match) |
| 7 | Excluded | Continue to Rule 8 | Exclusion |
| 8 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 8 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:

- They appear in the results of the search **On PAD Register- LTC LCS Priority Group 1 (HRC)**

### Rule 2 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"PAD Register"*
  - Code in: `on_pad_reg_pg2_hr_vs1` (53 codes)
  - Where date within the last 365 days to before 90 days ago — `date >= today - 365 days AND date < today - 90 days`

### Rule 3 of 8 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 4; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"PAD Register"*
  - Code in: `on_pad_reg_pg2_hr_vs1` (53 codes)
  - Where date before 12 months ago — `date < today - 12 months`

### Rule 4 of 8 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD), or `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months — `date > today - 12 months`
  - Keep only the latest 100 matching records
  - **Linked record A.1** — join: same date as record A
    *"Latest Systolic HOMEBP/BPEXHOME"*
    - Code in: `on_pad_reg_pg2_hr_vs4` (49 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record A.1.1** — join: same date as record A.1
      *"Latest Diastolic HOMEBP/BPEXHOME"*
      - Code in: `on_pad_reg_pg2_hr_vs5` (45 codes — cluster Diastolic Blood Pressure), or `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic BPEXHOME equal or less than 140"*
    - Code in: `on_pad_reg_pg2_hr_vs6` (36 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic BPEXHOME equal or less than 90"*
      - Code in: `on_pad_reg_pg2_hr_vs7` (32 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic BPEXHOME equal or less than 140"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 140

### Rule 5 of 8 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 6.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at home in last 12 months"*
  - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"Blood Pressure reading excluding home done in last 12 months"*
      - Code in: `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record A.1.1** — join: same date as record A.1
        *"Latest Systolic BPEXHOME equal or less than 140"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record A.1.1.1** — join: same date as record A.1.1
          *"Latest Diastolic BPEXHOME equal or less than 90"*
          - Code in: `on_pad_reg_pg2_hr_vs7` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **A.2 — Clinical Codes** (clinical events)
      *"Blood Pressure reading done at Home in last 12 months"*
      - Code in: `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record A.2.1** — join: same date as record A.2
        *"Latest Systolic HOMEBP equal or less than 135"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record A.2.1.1** — join: same date as record A.2.1
          *"Latest Diastolic HOMEBP equal or less than 85"*
          - Code in: `on_pad_reg_pg2_hr_vs8` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - **Linked record A.3** — join: same date as record A
    *"Blood Pressure reading done at Home in last 12 months"*
    - Code in: `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
    - Keep only the latest 100 matching records
    - **Linked record A.3.1** — join: same date as record A.3
      *"Latest Systolic HOMEBP equal or less than 135"*
      - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record
      - **Linked record A.3.1.1** — join: same date as record A.3.1
        *"Latest Diastolic HOMEBP equal or less than 85"*
        - Code in: `on_pad_reg_pg2_hr_vs8` (13 codes — cluster Diastolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value > 0
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at Home in last 12 months"*
  - Code in: `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic HOMEBP equal or less than 145"*
    - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic HOMEBP equal or less than 85"*
      - Code in: `on_pad_reg_pg2_hr_vs8` (13 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic HOMEBP equal or less than 145"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 135

### Rule 6 of 8 — Inclusion route (on no match)

If a patient does **not** match this rule they are **included** and no further rules are checked. If they do match, continue to Rule 7.

A patient matches this rule when:

- **Criterion A — Patient Details**
  - Where age more than 80 years old — `age > 80 years`

### Rule 7 of 8 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 8.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD), or `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months — `date > today - 12 months`
  - Keep only the latest 100 matching records
  - **Linked record A.1** — join: same date as record A
    *"Latest Systolic HOMEBP/BPEXHOME"*
    - Code in: `on_pad_reg_pg2_hr_vs4` (49 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record A.1.1** — join: same date as record A.1
      *"Latest Diastolic HOMEBP/BPEXHOME"*
      - Code in: `on_pad_reg_pg2_hr_vs5` (45 codes — cluster Diastolic Blood Pressure), or `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic BPEXHOME equal or less than 140"*
    - Code in: `on_pad_reg_pg2_hr_vs6` (36 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic BPEXHOME equal or less than 90"*
      - Code in: `on_pad_reg_pg2_hr_vs7` (32 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic BPEXHOME equal or less than 140"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 150

### Rule 8 of 8 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Patient Details** — must NOT exist
  *"Patient Age are less than 80"*
  - Where age under 80 years old — `age < 80 years`
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at home in last 12 months"*
  - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **B.1 — Clinical Codes** (clinical events)
      *"Blood Pressure reading excluding home done in last 12 months"*
      - Code in: `on_pad_reg_pg2_hr_vs2` (1 code — cluster CLINBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record B.1.1** — join: same date as record B.1
        *"Latest Systolic BPEXHOME equal or less than 140"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record B.1.1.1** — join: same date as record B.1.1
          *"Latest Diastolic BPEXHOME equal or less than 90"*
          - Code in: `on_pad_reg_pg2_hr_vs7` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **B.2 — Clinical Codes** (clinical events)
      *"Blood Pressure reading done at Home in last 12 months"*
      - Code in: `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record B.2.1** — join: same date as record B.2
        *"Latest Systolic HOMEBP equal or less than 135"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record B.2.1.1** — join: same date as record B.2.1
          *"Latest Diastolic HOMEBP equal or less than 85"*
          - Code in: `on_pad_reg_pg2_hr_vs8` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - **Linked record B.3** — join: same date as record B
    *"Blood Pressure reading done at Home in last 12 months"*
    - Code in: `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
    - Keep only the latest 100 matching records
    - **Linked record B.3.1** — join: same date as record B.3
      *"Latest Systolic HOMEBP equal or less than 135"*
      - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record
      - **Linked record B.3.1.1** — join: same date as record B.3.1
        *"Latest Diastolic HOMEBP equal or less than 85"*
        - Code in: `on_pad_reg_pg2_hr_vs8` (13 codes — cluster Diastolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value > 0
- **Criterion C — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at Home in last 12 months"*
  - Code in: `on_pad_reg_pg2_hr_vs3` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record C.1** — join: same date as record C
    *"Latest Systolic HOMEBP equal or less than 145"*
    - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record C.1.1** — join: same date as record C.1
      *"Latest Diastolic HOMEBP equal or less than 85"*
      - Code in: `on_pad_reg_pg2_hr_vs8` (13 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
      - **Linked record C.1.1.1** — join: same date as record C.1.1
        *"Latest Systolic HOMEBP equal or less than 145"*
        - Code in: `on_pad_reg_pg2_hr_vs6` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 145

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs1` |  | 2, 3 | SNOMED | 53 | Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +... | 36d83560 |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs2` | CLINBP_COD | 4, 5, 7, 8 | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs3` | HOMEAMBBP_COD | 4, 5, 7, 8 | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs4` | Systolic Blood Pressure | 4, 7 | SNOMED | 49 | Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic bloo... | dbe8bf65 |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs5` | Diastolic Blood Pressure | 4, 7 | SNOMED | 45 | Minimum diastolic blood pressure, Minimum day interval diastolic blood pressu... | 94656e9b |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs6` | Systolic Blood Pressure | 4, 5, 7, 8 | SNOMED | 36 | Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood ... | 5b356e22 |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs7` | Diastolic Blood Pressure | 4, 5, 7, 8 | SNOMED | 32 | Increased diastolic arterial pressure, High diastolic arterial pressure, Incr... | 2c57ad8d |
| On PAD Register- LTC LCS Priority Group 2 (HR) | `on_pad_reg_pg2_hr_vs8` | Diastolic Blood Pressure | 5, 8 | SNOMED | 13 | Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, A... | 5f525c4f |

## Caveats

- LTC LCS: PAD Register* references the EMIS library item `ffccdb77-bd5e-47fc-add3-d700835ace65`, whose logic is not included in this XML export. It is likely **PAD Register** (inferred from wrapper report "LTC LCS: PAD Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.