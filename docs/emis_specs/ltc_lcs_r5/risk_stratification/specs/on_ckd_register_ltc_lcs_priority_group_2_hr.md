<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0d56nam0-ztr1-g1-0ph1-06thze01idhr
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On CKD Register- LTC LCS Priority Group 2 (HR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: CKD Register*" (see below). Patients must match Rule 6 to stay in. Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-5 and 7-10.

## Who we start with

1. **LTC LCS: CKD Register*** — Start with currently registered patients. Require Patient Details where Age at least 18 years old. Include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 10

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:
- They appear in the results of the search **On CKD Register- LTC LCS Priority Group 1(HRC)**

### Rule 2 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 45 and <= 59
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 30

### Rule 3 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 30 and <= 44
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 3

### Rule 4 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs1` (2 codes)
  - Keep only the latest matching record, and require its numeric value >= 15 and <= 29

### Rule 5 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value >= 70 and < 250

### Rule 6 of 10

Patients **must match** this rule to stay in. Those who match continue to Rule 7; those who do not are excluded.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_ckd_reg_pg2_hr_vs3` (1 code)
  - Where drug code in `on_ckd_reg_pg2_hr_vs3` (1 code)
  - Where issue date within the last 3 months

### Rule 7 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 8.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD), or `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months
  - Keep only the latest 100 matching records
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs6` (49 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs7` (45 codes — cluster Diastolic Blood Pressure), or `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs8` (36 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs9` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value > 90
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_ckd_reg_pg2_hr_vs8` (36 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1

### Rule 8 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 9.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD), or `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
  - Where date within the last 12 months
  - Keep only the latest 100 matching records
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs6` (49 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs7` (45 codes — cluster Diastolic Blood Pressure), or `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its code to be in: CLINBP_COD
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs8` (36 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs9` (32 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_ckd_reg_pg2_hr_vs8` (36 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 150

### Rule 9 of 10

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 10.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD), or `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest matching record
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs8` (13 codes — cluster Systolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_ckd_reg_pg2_hr_vs10` (13 codes — cluster Diastolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 0
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs8` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs10` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value > 90
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_ckd_reg_pg2_hr_vs8` (13 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value >= 1

### Rule 10 of 10

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs4` (1 code — cluster CLINBP_COD), or `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest matching record
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs8` (13 codes — cluster Systolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_ckd_reg_pg2_hr_vs10` (13 codes — cluster Diastolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 0
- **Clinical Codes** (clinical events)
  - Code in: `on_ckd_reg_pg2_hr_vs5` (5 codes — cluster HOMEAMBBP_COD)
  - Keep only the latest 100 matching records, and require its date > today - 12 months
  - Must also have a linked record (Linked on DATE):
    - **Clinical Codes** (clinical events)
      - Code in: `on_ckd_reg_pg2_hr_vs8` (13 codes — cluster Systolic Blood Pressure)
      - Where numeric value > 0
      - Keep only the latest 100 matching records
      - Must also have a linked record (Linked on DATE):
        - **Clinical Codes** (clinical events)
          - Code in: `on_ckd_reg_pg2_hr_vs10` (13 codes — cluster Diastolic Blood Pressure)
          - Where numeric value > 0
          - Keep only the latest matching record, and require its numeric value >= 1
          - Must also have a linked record (Linked on DATE):
            - **Clinical Codes** (clinical events)
              - Code in: `on_ckd_reg_pg2_hr_vs8` (13 codes — cluster Systolic Blood Pressure)
              - Where numeric value > 0
              - Keep only the latest matching record, and require its numeric value > 150

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs1` |  | SNOMED | 2 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 45ee7150 |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs10` | Diastolic Blood Pressure | SNOMED | 13 | Minimum diastolic blood pressure, Average 24 hour diastolic blood pressure, A... | 5f525c4f |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs2` |  | SNOMED | 4 | Urine albumin:creatinine ratio, Albumin/creatinine ratio in urine, Urine micr... | d501652f |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs3` |  | Drug Group | 1 | Antihypertensive Drugs | 68b47862 |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs4` | CLINBP_COD | SNOMED | 1 | Refset: 999036281000230108 | f806e309 |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs5` | HOMEAMBBP_COD | SNOMED | 5 | 24 hour blood pressure, Average blood pressure, 24 hr blood pressure monitori... | 0daae157 |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs6` | Systolic Blood Pressure | SNOMED | 49 | Minimum systolic blood pressure, Systemic blood pressure, SBP - Systemic bloo... | dbe8bf65 |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs7` | Diastolic Blood Pressure | SNOMED | 45 | Minimum diastolic blood pressure, Minimum day interval diastolic blood pressu... | 94656e9b |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs8` | Systolic Blood Pressure | SNOMED | 36 | Systemic blood pressure, SBP - Systemic blood pressure, Lying systolic blood ... | 5b356e22 |
| On CKD Register- LTC LCS Priority Group 2 (HR) | `on_ckd_reg_pg2_hr_vs9` | Diastolic Blood Pressure | SNOMED | 32 | Increased diastolic arterial pressure, High diastolic arterial pressure, Incr... | 2c57ad8d |

## Caveats

- LTC LCS: CKD Register* references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.