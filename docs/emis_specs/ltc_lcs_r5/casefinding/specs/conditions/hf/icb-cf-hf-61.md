# ICB_CF_HF_61

Report title: [ICB_CF_HF_61] Heart Failure Case finding - Eligible Patients
Folder: 1) Casefinding R2 > [CF-HF] Heart Failure
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_HF_61_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_HF_61_BASE** — Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf) OR HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23). Include patients who do not match Clinical Codes with HFrEF - heart failure with reduced ejection fraction, Congestive cardiac failure, CCFDN - congenital cataracts, facial dysmorphism and neuropathy +4 more.
   - Combines: **ICS_METABOLIC_LTC**
3. **ICB_CF_HF_61_woEX** — Require Medication Issues with Spironolactone where Date of Issue within the last 3 months. Include patients who match any of: Medication Issues with Sacubitril, Valsartan OR Entresto OR Ivabradine, Eplerenone where Date of Issue within the last 3 months; OR Medication Issues with Dapagliflozin, Dapagliflozin Propanediol Monohydrate, Digoxin +1 more where Date of Issue within the last 3 months; OR Clinical Codes with N terminal pro-brain natriuretic peptide level, Plasma pro-brain natriuretic peptide level, Serum pro-BNP peptide level then Latest 1 where numeric value > 400; OR Clinical Codes with N terminal pro-brain natriuretic peptide level, Plasma pro-brain natriuretic peptide level, Serum pro-BNP peptide level where Date within the last 2 years then Latest 1 where numeric value > 2000; OR Clinical Codes with Cardiomyopathy, Heart failure excluded, Hypertrophic obstructive cardiomyopathy +15 more OR Heart failure excluded then Latest 1 AND Clinical Codes with Difficulty breathing, Difficulty taking deep breaths, Difficulty controlling breathing +8 more OR Heart failure excluded then Latest 1; OR Medication Issues with Digoxin where Date of Issue within the last 3 months. Include patients who do not match Patient Details where Age under 40 years old AND Patient Details with Female where Gender = Female AND Clinical Codes with Hirsutism, Hirsutism - hypertrichosis, O/E - hirsutism +3 more.
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
  - Code in: `hf_case_finding_eligible_patients_vs1` (1 code)
  - Where date within the last 3 years — `date >= today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_HF_61_BASE | `eligible_for_hf_casefinding_vs1` |  |  | SNOMED | 7 | HFrEF - heart failure with reduced ejection fraction, Congestive cardiac fail... | d31d4cb5 |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs1` |  |  | SCT Const | 2 | Sacubitril, Valsartan | 91ae3dcc |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs2` |  |  | Brand | 1 | Entresto | 3a7bd75c |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs3` |  |  | SCT Const | 2 | Ivabradine, Eplerenone | 08b52629 |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs4` |  |  | SCT Const | 4 | Dapagliflozin, Dapagliflozin Propanediol Monohydrate, Digoxin +1 more | 3348d79f |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs5` |  |  | SNOMED | 3 | N terminal pro-brain natriuretic peptide level, Plasma pro-brain natriuretic ... | d42c9760 |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs6` |  |  | SNOMED | 7 | Cardiological referral, Private referral to cardiologist, Referral to cardiol... | 483c4597 |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs7` |  |  | SNOMED | 1 | Heart failure excluded | 15a4071a |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs8` |  |  | SNOMED | 18 | Cardiomyopathy, Heart failure excluded, Hypertrophic obstructive cardiomyopat... | ad073823 |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs9` |  |  | SNOMED | 12 | Difficulty breathing, Difficulty taking deep breaths, Difficulty controlling ... | 53ee1a6f |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs10` |  |  | SCT Const | 1 | Digoxin | 051d8c9a |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs11` |  |  | SCT Const | 1 | Spironolactone | 8603cf04 |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs12` |  |  | Internal | 1 | Female | f67ab10a |
| ICB_CF_HF_61_woEX | `hf_case_finding_eligible_patients_vs13` |  |  | SNOMED | 6 | Hirsutism, Hirsutism - hypertrichosis, O/E - hirsutism +3 more | a9063f70 |
| ICB_CF_HF_61 | `hf_case_finding_eligible_patients_vs1` |  | 1 | SNOMED | 1 | Heart failure excluded | 15a4071a |

## Caveats

- ICB_CF_HF_61_BASE references the EMIS library item `e6742de9-2073-4a23-8c94-e05f668eaabf`, whose logic is not included in this XML export. It is likely **AF Register** (inferred from wrapper report "LTC LCS: AF Register*"), but this is not certain. Verify it in EMIS before implementing.
- ICB_CF_HF_61_BASE references the EMIS library item `79888a16-aa09-4ef4-ba5e-a3be8e1daf23`, whose logic is not included in this XML export. It is likely **HF Register** (inferred from wrapper report "LTC LCS: HF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.