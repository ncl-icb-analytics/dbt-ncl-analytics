<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0qghi8r0-lwhd-96-033h-070tzqb11cj5
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Stroke/TIA Register*" (see below). Patients must match Rule 3 to stay in. Patients matching Rules 1, 4-5 and 7 are excluded. A patient is included when they match Rule 2. A patient is included unless they match every one of Rules 6 and 8 — failing any one of them includes the patient. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

1. **LTC LCS: Stroke/TIA Register*** — Start with currently registered patients. Include patients who match Stroke/TIA Register (library item d4e6f787-dbce-4f0b-9f3f-498808ebad42).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 8

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:
- They appear in the results of the search **On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)***

### Rule 2 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs1` (1 code — cluster STRK_COD), or `on_stroketia_reg_pg2_hr_vs2` (1 code — cluster TIA_COD)
  - Where date within the last 365 days to before 30 days ago
  - Where episode type is First or New or Flare Up
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs1` (1 code — cluster STRK_COD), or `on_stroketia_reg_pg2_hr_vs2` (1 code — cluster TIA_COD)
  - Where date within the last 365 days to before 30 days ago
  - Where problem significance is Significant

### Rule 3 of 8

Patients **must match** this rule to stay in. Those who match continue to Rule 4; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs1` (1 code — cluster STRK_COD), or `on_stroketia_reg_pg2_hr_vs2` (1 code — cluster TIA_COD)
  - Where date before 1 year ago

### Rule 4 of 8

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD), or `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months
  - Keep only the latest 100 matching records
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs7` (49 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs8` (45 codes — cluster Diastolic Blood Pressure), or `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs9` (36 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs10` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_stroketia_reg_pg2_hr_vs9` (36 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 140

### Rule 5 of 8

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 6.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD), or `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest matching record
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs9` (13 codes — cluster Systolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_stroketia_reg_pg2_hr_vs11` (13 codes — cluster Diastolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 0
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs9` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs11` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_stroketia_reg_pg2_hr_vs9` (13 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 135

### Rule 6 of 8

If a patient does **not** match this rule they are **included** and no further rules are checked. If they do match, continue to Rule 7.

A patient matches this rule when:
- **Patient Details**
  - Where age more than 80 years old

### Rule 7 of 8

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 8.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD), or `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months
  - Keep only the latest 100 matching records
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs7` (49 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs8` (45 codes — cluster Diastolic Blood Pressure), or `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs9` (36 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs10` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_stroketia_reg_pg2_hr_vs9` (36 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 150

### Rule 8 of 8

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs5` (1 code — cluster CLINBP_COD), or `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest matching record
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs9` (13 codes — cluster Systolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_stroketia_reg_pg2_hr_vs11` (13 codes — cluster Diastolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 0
- **Clinical Codes** (clinical events)
  - Code in: `on_stroketia_reg_pg2_hr_vs6` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_stroketia_reg_pg2_hr_vs9` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_stroketia_reg_pg2_hr_vs11` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_stroketia_reg_pg2_hr_vs9` (13 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 145

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs1` | STRK_COD | SNOMED | 1 | Refset: 999005531000230105 | c8a23b04 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs10` | Diastolic Blood Pressure | SNOMED | 32 | Increased diastolic arterial pressure, High diastolic arterial pressure, Incr... | 2c57ad8d |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs11` | Diastolic Blood Pressure | SNOMED | 13 | Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, A... | 5f525c4f |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs2` | TIA_COD | SNOMED | 1 | Refset: 999005291000230109 | babfa5e0 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs3` |  | Internal | 3 | First, New, Flare Up | bd7fde07 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs4` |  | Internal | 1 | Significant | 8de0b3c4 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs5` | CLINBP_COD | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs6` | HOMEAMBBP_COD | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs7` | Systolic Blood Pressure | SNOMED | 49 | Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic bloo... | dbe8bf65 |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs8` | Diastolic Blood Pressure | SNOMED | 45 | Minimum diastolic blood pressure, Minimum day interval diastolic blood pressu... | 94656e9b |
| On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)* | `on_stroketia_reg_pg2_hr_vs9` | Systolic Blood Pressure | SNOMED | 36 | Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood ... | 5b356e22 |

## Caveats

- LTC LCS: Stroke/TIA Register* references the EMIS library item `d4e6f787-dbce-4f0b-9f3f-498808ebad42`, whose logic is not included in this XML export. It is likely **Stroke/TIA Register** (inferred from wrapper report "LTC LCS: Stroke/TIA Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.