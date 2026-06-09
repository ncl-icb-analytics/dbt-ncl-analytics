<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 03t5g221-8mul-2j-0h23-1nco5yj0yqh2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Hypertension Register- LTC LCS Priority Group 3B (MRb)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Hypertension Register*" (see below). Patients must match Rules 1 and 5 to stay in. Patients matching Rules 2-4 and 6 are excluded. Rule 7 includes only patients who do NOT match it.

## Who we start with

1. **LTC LCS: Hypertension Register*** — Start with currently registered patients. Include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 7

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months

### Rule 2 of 7

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 1 (HRC)**
- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 2 (HR)**
- They appear in the results of the search **On Hypertension Register- LTC LCS Priority Group 3A (MRa)**

### Rule 3 of 7

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 4.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months
  - Keep only the latest 100 matching records
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs3` (49 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs4` (45 codes — cluster Diastolic Blood Pressure), or `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (36 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs6` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (36 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 140

### Rule 4 of 7

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest matching record
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (13 codes — cluster Systolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_htn_reg_priority_group_3b_mrb_vs7` (13 codes — cluster Diastolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 0
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs7` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (13 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 135

### Rule 5 of 7

Patients **must match** this rule to stay in. Those who match continue to Rule 6; those who do not are excluded.

A patient matches this rule when:
- **Patient Details**
  - Where age more than 80 years old

### Rule 6 of 7

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 7.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months
  - Keep only the latest 100 matching records
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs3` (49 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs4` (45 codes — cluster Diastolic Blood Pressure), or `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (36 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs6` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 90
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (36 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 150

### Rule 7 of 7

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs1` (1 code — cluster CLINBP_COD), or `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest matching record
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (13 codes — cluster Systolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_htn_reg_priority_group_3b_mrb_vs7` (13 codes — cluster Diastolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 0
- **Clinical Codes** (clinical events)
  - Code in: `on_htn_reg_priority_group_3b_mrb_vs2` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_htn_reg_priority_group_3b_mrb_vs7` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1 and <= 85
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_htn_reg_priority_group_3b_mrb_vs5` (13 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1 and <= 145

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs1` | CLINBP_COD | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs2` | HOMEAMBBP_COD | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs3` | Systolic Blood Pressure | SNOMED | 49 | Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic bloo... | dbe8bf65 |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs4` | Diastolic Blood Pressure | SNOMED | 45 | Minimum diastolic blood pressure, Minimum day interval diastolic blood pressu... | 94656e9b |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs5` | Systolic Blood Pressure | SNOMED | 36 | Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood ... | 5b356e22 |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs6` | Diastolic Blood Pressure | SNOMED | 32 | Increased diastolic arterial pressure, High diastolic arterial pressure, Incr... | 2c57ad8d |
| On Hypertension Register- LTC LCS Priority Group 3B (MRb) | `on_htn_reg_priority_group_3b_mrb_vs7` | Diastolic Blood Pressure | SNOMED | 13 | Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, A... | 5f525c4f |

## Caveats

- LTC LCS: Hypertension Register* references the EMIS library item `a5ff1b4e-f130-4fea-b11c-5b40dc9b0877`, whose logic is not included in this XML export. It is likely **Hypertension Register** (inferred from wrapper report "LTC LCS: Hypertension Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.