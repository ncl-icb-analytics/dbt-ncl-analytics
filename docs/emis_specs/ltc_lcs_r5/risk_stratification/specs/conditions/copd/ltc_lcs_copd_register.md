<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1bhleef1-gshl-r2-0vff-0cyfdm51kuy0
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: COPD Register*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. A patient is included when they match any one of Rules 1-6. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

Currently registered patients.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Continue to Rule 2 | Inclusion route |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Continue to Rule 6 | Inclusion route |
| 6 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:

- They match the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935` (see Caveats)

### Rule 2 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Where date before 1 year ago — `date < today - 1 year`
  - Keep only the earliest matching record, and require its code to be in: COPD_COD
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"First COPD diagnosis"*
      - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
      - Keep only the earliest matching record
    - **A.2 — Clinical Codes** (clinical events) — must NOT exist
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Requires this group — **ALL (AND)** of the following:
    - **A.3 — Clinical Codes** (clinical events)
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
      - Keep only the latest matching record
      - **Linked record A.3.1** — join: its date after the date of record A.3
        *"First COPD diagnosis"*
        - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
        - Keep only the earliest matching record

### Rule 3 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Where date within the last 1 year — `date >= today - 1 year`
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"First COPD diagnosis"*
      - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
      - Keep only the earliest matching record
    - **A.2 — Clinical Codes** (clinical events) — must NOT exist
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Requires this group — **ALL (AND)** of the following:
    - **A.3 — Clinical Codes** (clinical events)
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
      - Keep only the latest matching record
      - **Linked record A.3.1** — join: its date after the date of record A.3
        *"First COPD diagnosis"*
        - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
        - Keep only the earliest matching record
  - **Linked record A.4** — join: its date at least 93 days before and at most 186 days after the date of record A
    *"First FEV1/FVC ratio with value less than 0.7"*
    - Code in: `copd_reg_vs3` (1 code — cluster FEV1FVC_COD)
    - Where numeric value < 0.7
    - Keep only the earliest matching record, and require its numeric value < 0.7
- **Criterion B — Clinical Codes** (clinical events)
  - Where date within the last 1 year — `date >= today - 1 year`
  - Requires this group — **ALL (AND)** of the following:
    - **B.1 — Clinical Codes** (clinical events)
      *"First COPD diagnosis"*
      - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
      - Keep only the earliest matching record
    - **B.2 — Clinical Codes** (clinical events) — must NOT exist
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Requires this group — **ALL (AND)** of the following:
    - **B.3 — Clinical Codes** (clinical events)
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
      - Keep only the latest matching record
      - **Linked record B.3.1** — join: its date after the date of record B.3
        *"First COPD diagnosis"*
        - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
        - Keep only the earliest matching record
  - **Linked record B.4** — join: its date at least 93 days before and at most 186 days after the date of record B
    *"First FEV1/FVC ratio of less than 0.7"*
    - Code in: `copd_reg_vs4` (1 code — cluster FEV1FVCL70_COD), or `copd_reg_vs5` (1 code)
    - Keep only the earliest matching record

### Rule 4 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Where date within the last 1 year — `date >= today - 1 year`
  - Keep only the earliest matching record, and require its code to be in: COPD_COD
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"First COPD diagnosis"*
      - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
      - Keep only the earliest matching record
    - **A.2 — Clinical Codes** (clinical events) — must NOT exist
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Requires this group — **ALL (AND)** of the following:
    - **A.3 — Clinical Codes** (clinical events)
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
      - Keep only the latest matching record
      - **Linked record A.3.1** — join: its date after the date of record A.3
        *"First COPD diagnosis"*
        - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
        - Keep only the earliest matching record
- **Criterion B — Patient Details**
  *"Patients Registered in the last 12 months"*
  - Where registration date within the last 12 months — `registration date > today - 12 months`
  - **Linked record B.1** — join: its date at least 93 days before and at most 186 days after the registration date of record B
    *"First FEV1/FVC ratio with value less than 0.7"*
    - Code in: `copd_reg_vs3` (1 code — cluster FEV1FVC_COD)
    - Where numeric value < 0.7
    - Keep only the earliest matching record

### Rule 5 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Where date within the last 1 year — `date >= today - 1 year`
  - Keep only the earliest matching record, and require its code to be in: COPD_COD
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"First COPD diagnosis"*
      - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
      - Keep only the earliest matching record
    - **A.2 — Clinical Codes** (clinical events) — must NOT exist
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Requires this group — **ALL (AND)** of the following:
    - **A.3 — Clinical Codes** (clinical events)
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
      - Keep only the latest matching record
      - **Linked record A.3.1** — join: its date after the date of record A.3
        *"First COPD diagnosis"*
        - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
        - Keep only the earliest matching record
- **Criterion B — Patient Details**
  *"Patients Registered in the last 12 months"*
  - Where registration date within the last 12 months — `registration date > today - 12 months`
  - **Linked record B.1** — join: its date at least 93 days before and at most 186 days after the registration date of record B
    *"First FEV1/FVC ratio of less than 0.7"*
    - Code in: `copd_reg_vs4` (1 code — cluster FEV1FVCL70_COD)
    - Keep only the earliest matching record

### Rule 6 of 6 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Where date within the last 1 year — `date >= today - 1 year`
  - Keep only the earliest matching record, and require its code to be in: COPD_COD
  - Requires this group — **ALL (AND)** of the following:
    - **A.1 — Clinical Codes** (clinical events)
      *"First COPD diagnosis"*
      - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
      - Keep only the earliest matching record
    - **A.2 — Clinical Codes** (clinical events) — must NOT exist
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
  - Requires this group — **ALL (AND)** of the following:
    - **A.3 — Clinical Codes** (clinical events)
      *"Occurrence of COPD resolved"*
      - Code in: `copd_reg_vs2` (1 code — cluster COPDRES_COD)
      - Keep only the latest matching record
      - **Linked record A.3.1** — join: its date after the date of record A.3
        *"First COPD diagnosis"*
        - Code in: `copd_reg_vs1` (1 code — cluster COPD_COD)
        - Keep only the earliest matching record

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: COPD Register* | `copd_reg_vs1` | COPD_COD | 2, 3, 4, 5, 6 | SNOMED | 1 | Refset: 999011571000230107 | 95fd66a2 |
| LTC LCS: COPD Register* | `copd_reg_vs2` | COPDRES_COD | 2, 3, 4, 5, 6 | SNOMED | 1 | Refset: 999009131000230100 | c378d825 |
| LTC LCS: COPD Register* | `copd_reg_vs3` | FEV1FVC_COD | 3, 4 | SNOMED | 1 | Refset: 999020251000230104 | 858d7625 |
| LTC LCS: COPD Register* | `copd_reg_vs4` | FEV1FVCL70_COD | 3, 5 | SNOMED | 1 | Refset: 999020291000230109 | faf72240 |
| LTC LCS: COPD Register* | `copd_reg_vs5` |  | 3 | SNOMED | 1 | UK NHS primary care data extraction - General practice data extraction - FEV1... | 0a5a44d1 |

## Caveats

- This search references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.