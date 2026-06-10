<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0ya2yux0-jt2t-pc-06f3-0ybvpy61sush
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On AF Register- LTC LCS Priority Group 2 (HR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: AF Register*" (see below). Patients must match Rules 1 and 4 to stay in. A patient is included when they match any one of Rules 2-3 and 5. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: AF Register*** — Include patients who match AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Continue to Rule 2 | Excluded | Filter — must match |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | Continue to Rule 5 | Excluded | Filter — must match |
| 5 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 5 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Medication Issues**
  *"Medication Issues of Warfarin in last 6 months"*
  - Code in: `on_af_reg_pg2_hr_vs1` (1 code — cluster Warfarin)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion B — Medication Issues**
  - Code in: `on_af_reg_pg2_hr_vs2` (6 codes)
  - Where issue date within the last 6 months — `issue date > today - 6 months`
- **Criterion C — Medication Issues**
  *"Medication Issues of Direct oral anticoagulant in last 6 months"*
  - Code in: `on_af_reg_pg2_hr_vs3` (40 codes — cluster DIRECTORANTICOAGDRUG_COD)
  - Where issue date within the last 6 months — `issue date > today - 6 months`
- **Criterion D — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs4` (1 code)
  - Where date within the last 6 months — `date >= today - 6 months`

### Rule 2 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Medication Issues**
  *"Medication Issues of Aspirin, Warfarin, Dipyridamole or Clopidogrel in last 12 months"*
  - Code in: `on_af_reg_pg2_hr_vs5` (7 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion B — Clinical Codes** (clinical events)
  *"Read Codes recording Aspirin Use in last 12 months"*
  - Code in: `on_af_reg_pg2_hr_vs6` (12 codes)
  - Where date within the last 6 months — `date >= today - 6 months`

### Rule 3 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs7` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 3
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs8` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 4

### Rule 4 of 5 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 5; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Medication Issues**
  - Code in: `on_af_reg_pg2_hr_vs2` (6 codes)
  - Where issue date within the last 6 months — `issue date > today - 6 months`

### Rule 5 of 5 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs9` (8 codes)
  - Keep only the latest matching record, and require its numeric value < 40
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs10` (3 codes)
  - Keep only the latest matching record, and require its numeric value < 50
- **Criterion C — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs11` (3 codes)
- **Criterion D — Clinical Codes** (clinical events)
  - Code in: `on_af_reg_pg2_hr_vs12` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 7

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs1` | Warfarin | 1 | Drug Group | 1 | Oral Anticoagulants | 3627615a |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs10` |  | 5 | SNOMED | 3 | Body weight, O/E - weight | 7f3acf99 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs11` |  | 5 | SNOMED | 3 | Rockwood Clinical Frailty Scale level 7 - severely frail, Rockwood Clinical F... | df624195 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs12` |  | 5 | SNOMED | 3 | Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Asses... | 734a600b |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs2` |  | 1, 4 | SCT Const | 6 | Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more | 05e37db6 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs3` | DIRECTORANTICOAGDRUG_COD | 1 | SNOMED | 40 | Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsu... | a8a26d95 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs4` |  | 1 | SNOMED | 1 | Anticoagulant prescribed by third party | b532dd16 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs5` |  | 2 | SCT Const | 7 | Aspirin, Clopidogrel, Clopidogrel Hydrogen Sulfate +4 more | 5dc10500 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs6` |  | 2 | SNOMED | 12 | Aspirin prophylaxis - IHD, Aspirin prophylaxis for ischaemic heart disease, A... | 5161429e |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs7` |  | 3 | SNOMED | 3 | HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleedin... | 60b9f18e |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs8` |  | 3 | SNOMED | 3 | ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillat... | 53c83a20 |
| On AF Register- LTC LCS Priority Group 2 (HR)* | `on_af_reg_pg2_hr_vs9` |  | 5 | SNOMED | 8 | Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated crea... | 610c0721 |

## Caveats

- LTC LCS: AF Register* references the EMIS library item `e6742de9-2073-4a23-8c94-e05f668eaabf`, whose logic is not included in this XML export. It is likely **AF Register** (inferred from wrapper report "LTC LCS: AF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.