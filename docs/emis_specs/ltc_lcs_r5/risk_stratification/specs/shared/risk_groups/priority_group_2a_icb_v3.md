<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1cpo3cm1-nr8q-e5-156m-1xiycss0m127
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Priority Group 2a (ICB) v3

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Hypertension Register*" (see below). Patients must match Rule 1 to stay in. Patients matching Rules 2-3 are excluded. A patient is included when they do NOT match Rule 4. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Hypertension Register*** — Include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Continue to Rule 2 | Excluded | Filter — must match |
| 2 | Excluded | Continue to Rule 3 | Exclusion |
| 3 | Excluded | Continue to Rule 4 | Exclusion |
| 4 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 4 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `priority_group_2a_icb_v3_vs1` (1 code — cluster CLINBP_COD), or `priority_group_2a_icb_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months — `date > today - 12 months`

### Rule 2 of 4 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when:

- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3**

### Rule 3 of 4 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 4.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `priority_group_2a_icb_v3_vs1` (1 code — cluster CLINBP_COD), or `priority_group_2a_icb_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months — `date > today - 12 months`
  - Keep only the latest 100 matching records
  - **Linked record A.1** — join: same date as record A
    *"Latest Systolic HOMEBP/BPEXHOME"*
    - Code in: `priority_group_2a_icb_v3_vs3` (49 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record A.1.1** — join: same date as record A.1
      *"Latest Diastolic HOMEBP/BPEXHOME"*
      - Code in: `priority_group_2a_icb_v3_vs4` (45 codes — cluster Diastolic Blood Pressure), or `priority_group_2a_icb_v3_vs1` (1 code — cluster CLINBP_COD)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading excluding home done in last 12 months"*
  - Code in: `priority_group_2a_icb_v3_vs1` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic BPEXHOME equal or less than 140"*
    - Code in: `priority_group_2a_icb_v3_vs5` (36 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic BPEXHOME equal or less than 90"*
      - Code in: `priority_group_2a_icb_v3_vs6` (32 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 100
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic BPEXHOME equal or less than 150"*
        - Code in: `priority_group_2a_icb_v3_vs5` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 160

### Rule 4 of 4 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at home in last 12 months"*
  - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"Blood Pressure reading excluding home done in last 12 months"*
      - Code in: `priority_group_2a_icb_v3_vs1` (1 code — cluster CLINBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record A.1.1** — join: same date as record A.1
        *"Latest Systolic BPEXHOME equal or less than 140"*
        - Code in: `priority_group_2a_icb_v3_vs5` (36 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record A.1.1.1** — join: same date as record A.1.1
          *"Latest Diastolic BPEXHOME equal or less than 90"*
          - Code in: `priority_group_2a_icb_v3_vs6` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - Requires this group — **ALL (AND)** of the following:
    - **A.2 — Clinical Codes** (clinical events)
      *"Blood Pressure reading done at Home in last 12 months"*
      - Code in: `priority_group_2a_icb_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - **Linked record A.2.1** — join: same date as record A.2
        *"Latest Systolic HOMEBP equal or less than 135"*
        - Code in: `priority_group_2a_icb_v3_vs5` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest 100 matching records
        - **Linked record A.2.1.1** — join: same date as record A.2.1
          *"Latest Diastolic HOMEBP equal or less than 85"*
          - Code in: `priority_group_2a_icb_v3_vs7` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
  - **Linked record A.3** — join: same date as record A
    *"Blood Pressure reading done at Home in last 12 months"*
    - Code in: `priority_group_2a_icb_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
    - Keep only the latest 100 matching records
    - **Linked record A.3.1** — join: same date as record A.3
      *"Latest Systolic HOMEBP equal or less than 135"*
      - Code in: `priority_group_2a_icb_v3_vs5` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record
      - **Linked record A.3.1.1** — join: same date as record A.3.1
        *"Latest Diastolic HOMEBP equal or less than 85"*
        - Code in: `priority_group_2a_icb_v3_vs7` (13 codes — cluster Diastolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value > 0
- **Criterion B — Clinical Codes** (clinical events)
  *"Blood Pressure reading done at Home in last 12 months"*
  - Code in: `priority_group_2a_icb_v3_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - **Linked record B.1** — join: same date as record B
    *"Latest Systolic HOMEBP equal or less than 145"*
    - Code in: `priority_group_2a_icb_v3_vs5` (13 codes — cluster Systolic Blood Pressure)
    - Where numeric value > 0
    - Keep only the latest 100 matching records
    - **Linked record B.1.1** — join: same date as record B.1
      *"Latest Diastolic HOMEBP equal or less than 85"*
      - Code in: `priority_group_2a_icb_v3_vs7` (13 codes — cluster Diastolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest matching record, and require its numeric value >= 1 and <= 95
      - **Linked record B.1.1.1** — join: same date as record B.1.1
        *"Latest Systolic HOMEBP equal or less than 145"*
        - Code in: `priority_group_2a_icb_v3_vs5` (13 codes — cluster Systolic Blood Pressure)
        - Where numeric value > 0
        - Keep only the latest matching record, and require its numeric value >= 1 and <= 150

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs1` | CLINBP_COD | 1, 3, 4 | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs2` | HOMEAMBBP_COD | 1, 3, 4 | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs3` | Systolic Blood Pressure | 3 | SNOMED | 49 | Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic bloo... | dbe8bf65 |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs4` | Diastolic Blood Pressure | 3 | SNOMED | 45 | Minimum diastolic blood pressure, Minimum day interval diastolic blood pressu... | 94656e9b |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs5` | Systolic Blood Pressure | 3, 4 | SNOMED | 36 | Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood ... | 5b356e22 |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs6` | Diastolic Blood Pressure | 3, 4 | SNOMED | 32 | Increased diastolic arterial pressure, High diastolic arterial pressure, Incr... | 2c57ad8d |
| Priority Group 2a (ICB) v3 | `priority_group_2a_icb_v3_vs7` | Diastolic Blood Pressure | 4 | SNOMED | 13 | Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, A... | 5f525c4f |

## Caveats

- LTC LCS: Hypertension Register* references the EMIS library item `a5ff1b4e-f130-4fea-b11c-5b40dc9b0877`, whose logic is not included in this XML export. It is likely **Hypertension Register** (inferred from wrapper report "LTC LCS: Hypertension Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.