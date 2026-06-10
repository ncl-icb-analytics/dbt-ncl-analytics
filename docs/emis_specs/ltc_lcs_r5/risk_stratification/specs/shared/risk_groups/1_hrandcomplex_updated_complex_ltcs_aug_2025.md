<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 09cbutw0-tssy-k0-1bq6-013s9x903ee6
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP1- HRC" (see below). This report has no filtering rules of its own — it reports on its starting population.

## Start population

1. Currently registered patients
2. **LTC LCS Base*** — Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
   - Combines: **LTC LCS: AF Register***; **LTC LCS: CKD Register***; **LTC LCS: CHD Register***; **LTC LCS: Diabetes Register***; **LTC LCS: Hypertension Register***; **LTC LCS: NAFLD Register v2***; **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: COPD Register***; **LTC LCS: HF Register***; **LTC LCS: PAD Register***; **LTC LCS: Stroke/TIA Register***
3. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
   - Combines: **LTC LCS: Asthma CYP Register ONLY**; **On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only)**; **On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only)**
4. **GROUP1- HRC** — Include patients who match any of: Clinical Codes with Refset: 999003371000230102 OR Refset: 999004691000230108 OR Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +79 more then Latest 1; OR Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
   - Combines: **On CKD Register- LTC LCS Priority Group 1(HRC)**; **On CHD Register- LTC LCS Priority Group 1 (HRC)**; **on Diabetes Register- LTC LCS Priority Group 1 (HRC)**; **On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3**; **On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)***; **On COPD Register- LTC LCS Priority Group 1 (HRC)**; **On PAD Register- LTC LCS Priority Group 1 (HRC)**; **On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)***
5. **This search** — applies the rules below.

## Rule details

No rules — all patients from the starting population are included.

## Report output

These define what the report shows for each patient, not who is included.

### Patient Details

Shows: NHS Number, Usual GP's Organisation Code, Age, Date of Birth, Gender, Ethnic Origin, Townsend Score, Lower Layer Area (2001), Middle Layer Area (2001), Lower Layer Area (2011), Middle Layer Area (2011)
No filtering criteria; outputs standard columns.

### Record of Interpreter Information

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs1` (16 codes)
  - Keep only the latest matching record

### Homelessness

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs2` (3 codes), or `1hrandcomplex_updated_complex_ltcs_aug_2025_vs3` (2 codes)
  - Keep only the latest matching record

### Housebound

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs4` (2 codes), or `1hrandcomplex_updated_complex_ltcs_aug_2025_vs5` (1 code)
  - Keep only the latest matching record

### Care Home resident

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs6` (3 codes), or `1hrandcomplex_updated_complex_ltcs_aug_2025_vs7` (2 codes)
  - Keep only the latest matching record

### Cardiovascular disease

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Atrial Fibrillation

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Hypertension

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Hyperlipidaemia

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs8` (1 code)
  - Keep only the latest matching record

### Diabetes

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Chronic Kidney Disease

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### NA Fatty Liver Disease

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs9` (16 codes)
  - Keep only the latest matching record

### COPD

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Asthma

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Serious MI

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs10` (2 codes), or `1hrandcomplex_updated_complex_ltcs_aug_2025_vs11` (1 code)
  - Keep only the latest matching record

### Learning Disability

- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Frailty

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs12` (6 codes)
  - Keep only the latest matching record

### Smoking Status

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs13` (27 codes)
  - Keep only the latest matching record

### BMI

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs14` (16 codes)
  - Keep only the latest matching record

### O/E BP Reading

Shows: Date, Code Term, Value, Secondary Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs15` (1 code)
  - Keep only the latest matching record

### HbA1c

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs16` (3 codes)
  - Keep only the latest matching record

### eGFR

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs17` (4 codes)
  - Keep only the latest matching record

### UACR

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs18` (6 codes)
  - Keep only the latest matching record

### TC

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs19` (6 codes)
  - Keep only the latest matching record

### LDL

Shows: Date, Code Term, Value
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs20` (7 codes)
  - Keep only the latest matching record

### MoC- Check & Test Appointment on

Shows: Date
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs21` (1 code)
  - Keep only the latest matching record

### MoC- Follow-Up Appointment on

Shows: Date
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_updated_complex_ltcs_aug_2025_vs22` (1 code)
  - Keep only the latest matching record

### Rpt count

- **Criterion A — Medication Courses**
  - Where prescription type is Automatic or Repeat or Repeat Dispensed
  - Where course status is Current

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GROUP1- HRC | `high_risk_complexity_vs1` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| GROUP1- HRC | `high_risk_complexity_vs2` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| GROUP1- HRC | `high_risk_complexity_vs3` |  |  | SNOMED | 105 | Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabete... | 10923643 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs1` |  | Record of Interpreter Information | SNOMED | 16 | Interpreter needed, Interpreter present, Presence of interpreter +13 more | d9a780b0 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs10` |  | Serious MI | SNOMED | 2 | On severe mental illness register, Removed from severe mental illness register | d864ef35 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs11` |  | Serious MI | SNOMED | 1 | On severe mental illness register | d2e1bc0b |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs12` |  | Frailty | SNOMED | 6 | Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moder... | fa6dc79f |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs13` |  | Smoking Status | SNOMED | 27 | Ex-smoker, Cigarette smoker, Current smoker +24 more | 9ce972e3 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs14` |  | BMI | SNOMED | 16 | Body mass index, Body mass index 30+ - obesity, Body mass index 25-29 - overw... | b9cc9742 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs15` |  | O/E BP Reading | SNOMED | 1 | O/E - blood pressure reading | 91cb1fb0 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs16` |  | HbA1c | SNOMED | 3 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 95d9e41a |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs17` |  | eGFR | SNOMED | 4 | eGFR (estimated glomerular filtration rate) using creatinine Chronic Kidney D... | ab88cefc |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs18` |  | UACR | SNOMED | 6 | Urine albumin:creatinine ratio, Urine albumin/creatinine ratio measurement, U... | 26bdbd23 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs19` |  | TC | SNOMED | 6 | Serum total cholesterol level, Total cholesterol level, Serum fasting total c... | 0be07f86 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs2` |  | Homelessness | SNOMED | 3 | Homeless, Homeless enhanced services administration, No longer homeless | bd017638 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs20` |  | LDL | SNOMED | 7 | Serum low density lipoprotein cholesterol level, Calculated LDL (low density ... | c5fe5ee3 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs21` |  | MoC- Check & Test Appointment on | SNOMED | 1 | Chronic disease initial assessment | 2c55e088 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs22` |  | MoC- Follow-Up Appointment on | SNOMED | 1 | Review of Personalised Care and Support Plan | 5bfd7289 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs23` |  | Rpt count | Internal | 3 | Automatic, Repeat, Repeat Dispensed | 23417f0f |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs24` |  | Rpt count | Internal | 1 | Current | 6b23c0d5 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs3` |  | Homelessness | SNOMED | 2 | Homeless, Homeless enhanced services administration | c853fe7f |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs4` |  | Housebound | SNOMED | 2 | Housebound, No longer housebound | 7bcc1e58 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs5` |  | Housebound | SNOMED | 1 | Housebound | fb66502d |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs6` |  | Care Home resident | SNOMED | 3 | Lives in care home, Provision of general practitioner intermediate care, Prev... | 23643eb1 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs7` |  | Care Home resident | SNOMED | 2 | Lives in care home, Provision of general practitioner intermediate care | 8aafdf14 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs8` |  | Hyperlipidaemia | SNOMED | 1 | Hyperlipidaemia | b03bd7c6 |
| 1_HRandCOMPLEX - updated : Complex LTCs- Aug 2025 | `1hrandcomplex_updated_complex_ltcs_aug_2025_vs9` |  | NA Fatty Liver Disease | SNOMED | 16 | NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver di... | c4faf05c |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.