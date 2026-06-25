# ICB_CF_AF_61

Report title: [ICB_CF_AF_61] AF Case Finding: Eligible Population- On AF Medication
Folder: 1) Casefinding R2 > [CF-AF] AF Case finding
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_AF_61_base_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_AF_61_BASE** — Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23). Include patients who match Medication Courses with Oral Anticoagulants OR Digoxin, Flecainide Acetate, Propafenone Hydrochloride OR Medication Courses with Cardiac Glycosides OR Medication Issues with Anticoagulants And Protamine where Date of Issue within the last 6 months.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_AF_61_base_woEX** — Exclude patients who match Clinical Codes with Antiphospholipid syndrome, Deep venous thrombosis, Advised risk of deep vein thrombosis in air travel +219 more; Clinical Codes with Atrial fibrillation, ECG: atrial fibrillation, Atrial flutter +5 more; Clinical Codes with Long COVID-19. Include patients who do not match Clinical Codes with Hypoplastic left heart syndrome.
4. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `af_case_finding_eligible_population_on_af_medication_vs1` (2 codes)
  - Where date within the last 3 years — `date > today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_AF_61_BASE | `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs1` |  |  | Drug Group | 1 | Oral Anticoagulants | 3627615a |
| ICB_CF_AF_61_BASE | `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs2` |  |  | SCT Const | 3 | Digoxin, Flecainide Acetate, Propafenone Hydrochloride | 89c96068 |
| ICB_CF_AF_61_BASE | `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs3` |  |  | Drug Group | 1 | Cardiac Glycosides | 3d34ea44 |
| ICB_CF_AF_61_BASE | `patients_on_digoxin_flecainide_propafenone_or_anticoag_vs4` |  |  | Drug Group | 1 | Anticoagulants And Protamine | 38916e02 |
| ICB_CF_AF_61_base_woEX | `af_case_finding_eligible_population_on_af_medication_vs1` |  |  | SNOMED | 223 | Antiphospholipid syndrome, Deep venous thrombosis, Advised risk of deep vein ... | 1dd431f7 |
| ICB_CF_AF_61_base_woEX | `af_case_finding_eligible_population_on_af_medication_vs2` |  |  | SNOMED | 8 | Atrial fibrillation, ECG: atrial fibrillation, Atrial flutter +5 more | 6ae95ae6 |
| ICB_CF_AF_61_base_woEX | `af_case_finding_eligible_population_on_af_medication_vs3` |  |  | SNOMED | 1 | Long COVID-19 | 48df1e81 |
| ICB_CF_AF_61_base_woEX | `af_case_finding_eligible_population_on_af_medication_vs4` |  |  | SNOMED | 1 | Hypoplastic left heart syndrome | 86856ab5 |
| ICB_CF_AF_61 | `af_case_finding_eligible_population_on_af_medication_vs1` |  | 1 | SNOMED | 2 | Atrial fibrillation excluded, Atrial fibrillation confirmed | ea9dcb07 |

## Caveats

- ICB_CF_AF_61_BASE references the EMIS library item `e6742de9-2073-4a23-8c94-e05f668eaabf`, whose logic is not included in this XML export. It is likely **AF Register** (inferred from wrapper report "LTC LCS: AF Register*"), but this is not certain. Verify it in EMIS before implementing.
- ICB_CF_AF_61_BASE references the EMIS library item `79888a16-aa09-4ef4-ba5e-a3be8e1daf23`, whose logic is not included in this XML export. It is likely **HF Register** (inferred from wrapper report "LTC LCS: HF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.