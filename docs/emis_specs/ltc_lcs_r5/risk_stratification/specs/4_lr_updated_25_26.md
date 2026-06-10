<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0lg80x20-wl7t-oo-11aj-0hmhga40blzw
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 4_LR updated 25-26

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP4- LR" (see below). This report has no filtering rules of its own — it reports on its starting population.

## Who we start with

1. **LTC LCS Base*** — Start with currently registered patients. Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
2. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Start with the patients found by "LTC LCS Base*". Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
3. **GROUP4- LR** — Start with the patients found by "LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only". Finally include patients who do not match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR OR patients included in search GROUP3- MR.
4. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

No rules — all patients from the starting population are included.

## Report output

These define what the report shows for each patient, not who is included.

### Patient Details

### Record of Interpreter Information
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs1` (16 codes)
  - Keep only the latest matching record

### Homeless
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs2` (3 codes), or `4lr_updated_25_26_vs3` (2 codes)
  - Keep only the latest matching record

### Housebound
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs4` (2 codes), or `4lr_updated_25_26_vs5` (1 code)
  - Keep only the latest matching record

### Care Home resident
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs6` (3 codes), or `4lr_updated_25_26_vs7` (2 codes)
  - Keep only the latest matching record

### Cardiovascular disease
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Atrial Fibrillation
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Hypertension
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Hyperlipidaemia
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs8` (1 code)
  - Keep only the latest matching record

### Diabetes
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Chronic Kidney Disease
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### NA Fatty Liver Disease
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs9` (12 codes)
  - Keep only the latest matching record

### COPD
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Asthma
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Serious MI
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs10` (2 codes), or `4lr_updated_25_26_vs11` (1 code)
  - Keep only the latest matching record

### Learning Disability
- **Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Frailty
- **Clinical Codes** (clinical events)
  - Code in: `4lr_updated_25_26_vs12` (6 codes)
  - Keep only the latest matching record

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs1` |  | SNOMED | 16 | Interpreter needed, Interpreter present, Presence of interpreter +13 more | d9a780b0 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs10` |  | SNOMED | 2 | On severe mental illness register, Removed from severe mental illness register | d864ef35 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs11` |  | SNOMED | 1 | On severe mental illness register | d2e1bc0b |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs12` |  | SNOMED | 6 | Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moder... | fa6dc79f |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs2` |  | SNOMED | 3 | Homeless, Homeless enhanced services administration, No longer homeless | bd017638 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs3` |  | SNOMED | 2 | Homeless, Homeless enhanced services administration | c853fe7f |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs4` |  | SNOMED | 2 | Housebound, No longer housebound | 7bcc1e58 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs5` |  | SNOMED | 1 | Housebound | fb66502d |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs6` |  | SNOMED | 3 | Lives in care home, Provision of general practitioner intermediate care, Prev... | 23643eb1 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs7` |  | SNOMED | 2 | Lives in care home, Provision of general practitioner intermediate care | 8aafdf14 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs8` |  | SNOMED | 1 | Hyperlipidaemia | b03bd7c6 |
| 4_LR updated 25-26 | `4lr_updated_25_26_vs9` |  | SNOMED | 12 | NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver di... | 63fd8d6e |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.