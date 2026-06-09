<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0bkid961-q1iy-w4-0kge-144fcwf1s3r5
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: COPD Register*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. A patient is included when they match any one of Rules 1-6.

## Who we start with

Currently registered patients.

## Inclusion logic, step by step

### Rule 1 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:
- They match the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935` (see Caveats)

### Rule 2 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD), or `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Where date before 1 year ago
  - Keep only the earliest matching record, and require its code to be in: COPD_COD

### Rule 3 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD), or `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Where date within the last 1 year
  - Must also have a linked record (its date at least 93 days before and at most 186 days after the date of the record above):
    - **Clinical Codes** (clinical events)
      - Code in: `copd_reg_vs3` (1 code — cluster FEV1FVC_COD)
      - Where numeric value < 0.7
      - Keep only the earliest matching record, and require its numeric value < 0.7
- **Clinical Codes** (clinical events)
  - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD), or `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Where date within the last 1 year
  - Must also have a linked record (its date at least 93 days before and at most 186 days after the date of the record above):
    - **Clinical Codes** (clinical events)
      - Code in: `copd_reg_vs4` (1 code — cluster FEV1FVCL70_COD), or `copd_reg_vs5` (1 code)
      - Keep only the earliest matching record

### Rule 4 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD), or `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Where date within the last 1 year
  - Keep only the earliest matching record, and require its code to be in: COPD_COD
- **Patient Details**
  - Where registration date within the last 12 months
  - Must also have a linked record (its date at least 93 days before and at most 186 days after the registration date of the record above):
    - **Clinical Codes** (clinical events)
      - Code in: `copd_reg_vs3` (1 code — cluster FEV1FVC_COD)
      - Where numeric value < 0.7
      - Keep only the earliest matching record

### Rule 5 of 6

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when ALL of the following are true:
- **Clinical Codes** (clinical events)
  - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD), or `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Where date within the last 1 year
  - Keep only the earliest matching record, and require its code to be in: COPD_COD
- **Patient Details**
  - Where registration date within the last 12 months
  - Must also have a linked record (its date at least 93 days before and at most 186 days after the registration date of the record above):
    - **Clinical Codes** (clinical events)
      - Code in: `copd_reg_vs4` (1 code — cluster FEV1FVCL70_COD)
      - Keep only the earliest matching record

### Rule 6 of 6

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD), or `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Where date within the last 1 year
  - Keep only the earliest matching record, and require its code to be in: COPD_COD

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: COPD Register* | `copd_reg_vs1` | COPD_COD | SNOMED | 1 | Refset: 999011571000230107 | 95fd66a2 |
| LTC LCS: COPD Register* | `copd_reg_vs2` | COPDRES_COD | SNOMED | 1 | Refset: 999009131000230100 | c378d825 |
| LTC LCS: COPD Register* | `copd_reg_vs3` | FEV1FVC_COD | SNOMED | 1 | Refset: 999020251000230104 | 858d7625 |
| LTC LCS: COPD Register* | `copd_reg_vs4` | FEV1FVCL70_COD | SNOMED | 1 | Refset: 999020291000230109 | faf72240 |
| LTC LCS: COPD Register* | `copd_reg_vs5` |  | SNOMED | 1 | UK NHS primary care data extraction - General practice data extraction - FEV1... | 0a5a44d1 |

## Caveats

- This search references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.