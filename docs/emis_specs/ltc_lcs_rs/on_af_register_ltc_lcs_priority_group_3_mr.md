<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0n0fqrz1-ku4w-y7-1j09-0jh5ui70x0ja
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On AF Register- LTC LCS Priority Group 3 (MR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: AF Register*" (see below). Patients must match Rule 2 to stay in. Patients matching Rules 1 and 10 are excluded. A patient is included when they match any one of Rules 3-9 and 11-14.

## Who we start with

1. **LTC LCS: AF Register*** — Start with currently registered patients. Include patients who match AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 14

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:
- They appear in the results of the search **On AF Register- LTC LCS Priority Group 2 (HR)***

### Rule 2 of 14

Patients **must match** this rule to stay in. Those who match continue to Rule 3; those who do not are excluded.

A patient matches this rule when ANY of the following is true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs1` (1 code — cluster Warfarin)
  - Where drug code in `on_af_reg_pg3_mr_vs1` (1 code — cluster Warfarin)
  - Where issue date within the last 6 months
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs2` (6 codes)
  - Where drug code in `on_af_reg_pg3_mr_vs2` (6 codes)
  - Where issue date within the last 6 months
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs4` (1 code)
  - Where date within the last 6 months
- **Patient Details**
  - Where age more than 65 years old
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs5` (8 codes)
  - Keep only the latest matching record, and require its numeric value >= 40 and < 60
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs6` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 50
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs7` (6 codes), or `on_af_reg_pg3_mr_vs8` (1 code)
  - Keep only the latest matching record
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs9` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 6 and < 7 and date > today - 6 months
- **Clinical codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs10` (1 code — cluster CHADVASC_COD)
  - Keep only the latest matching record, and require its numeric value >= 1

### Rule 3 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs1` (1 code — cluster Warfarin)
  - Where drug code in `on_af_reg_pg3_mr_vs1` (1 code — cluster Warfarin)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs11` (1 code)
  - Keep only the latest matching record, and require its date < today - 15 months

### Rule 4 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs11` (1 code)
  - Keep only the latest matching record, and require its date < today - 15 months

### Rule 5 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs5` (8 codes)
  - Keep only the latest matching record, and require its numeric value >= 40 and < 60

### Rule 6 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 7.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs6` (3 codes)
  - Keep only the latest matching record, and require its numeric value > 50 and <= 60

### Rule 7 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 8.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Patient Details**
  - Where age at least 75 years old

### Rule 8 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 9.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs7` (6 codes), or `on_af_reg_pg3_mr_vs8` (1 code)
  - Keep only the latest matching record

### Rule 9 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 10.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs9` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 6 and < 7 and date > today - 6 months

### Rule 10 of 14

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 11.

A patient matches this rule when ANY of the following is true:
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs1` (1 code — cluster Warfarin)
  - Where drug code in `on_af_reg_pg3_mr_vs1` (1 code — cluster Warfarin)
  - Where issue date within the last 6 months
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs2` (6 codes)
  - Where drug code in `on_af_reg_pg3_mr_vs2` (6 codes)
  - Where issue date within the last 6 months
- **Medication Issues**
  - Code in: `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where drug code in `on_af_reg_pg3_mr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs4` (1 code)
  - Where date within the last 6 months

### Rule 11 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 12.

A patient matches this rule when ALL of the following are true:
- **Clinical codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs10` (1 code — cluster CHADVASC_COD)
  - Keep only the latest matching record, and require its numeric value >= 1
- **Patient Details**
  - Code in: `on_af_reg_pg3_mr_vs12` (1 code)
  - Where sex in `on_af_reg_pg3_mr_vs12` (1 code)

### Rule 12 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 13.

A patient matches this rule when ALL of the following are true:
- **Clinical codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs10` (1 code — cluster CHADVASC_COD)
  - Keep only the latest matching record, and require its numeric value >= 2
- **Patient Details**
  - Code in: `on_af_reg_pg3_mr_vs13` (1 code)
  - Where sex in `on_af_reg_pg3_mr_vs13` (1 code)

### Rule 13 of 14

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 14.

A patient matches this rule when ALL of the following are true:
- **Clinical codes** (clinical events)
  - Code in: `on_af_reg_pg3_mr_vs10` (1 code — cluster CHADVASC_COD)
  - Keep only the latest matching record, and require its date < today - 2 years
- **Patient Details**
  - Where age more than 65 years old

### Rule 14 of 14

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ALL of the following are true:
- **Clinical codes** (clinical events) — patient must NOT have a matching record
  - Code in: `on_af_reg_pg3_mr_vs10` (1 code — cluster CHADVASC_COD)
- **Patient Details**
  - Where age more than 65 years old

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs1` | Warfarin | Drug Group | 1 | Oral Anticoagulants | 3627615a |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs10` | CHADVASC_COD | SNOMED | 1 | Refset: 999011331000230100 | 6dd017b8 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs11` |  | SNOMED | 1 | Serum creatinine level | 67b00c07 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs12` |  | Internal | 1 | Male | 08f27188 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs13` |  | Internal | 1 | Female | f67ab10a |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs2` |  | SCT Const | 6 | Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more | 05e37db6 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs3` | DIRECTORANTICOAGDRUG_COD | SNOMED | 40 | Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsu... | a8a26d95 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs4` |  | SNOMED | 1 | Anticoagulant prescribed by third party | b532dd16 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs5` |  | SNOMED | 8 | Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated crea... | 610c0721 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs6` |  | SNOMED | 3 | Body weight, O/E - weight | 7f3acf99 |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs7` |  | SNOMED | 6 | Rockwood Clinical Frailty Scale level 1 - very fit, Rockwood Clinical Frailty... | ffc1706e |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs8` |  | SNOMED | 1 | Rockwood Clinical Frailty Scale level 6 - moderately frail | 59e5803d |
| On AF Register- LTC LCS Priority Group 3 (MR)* | `on_af_reg_pg3_mr_vs9` |  | SNOMED | 3 | Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Asses... | 734a600b |

## Caveats

- LTC LCS: AF Register* references the EMIS library item `e6742de9-2073-4a23-8c94-e05f668eaabf`, whose logic is not included in this XML export. It is likely **AF Register** (inferred from wrapper report "LTC LCS: AF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.