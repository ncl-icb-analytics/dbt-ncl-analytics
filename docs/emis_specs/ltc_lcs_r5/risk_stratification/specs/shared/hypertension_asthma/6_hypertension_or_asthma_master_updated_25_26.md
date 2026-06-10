<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 19f4w5z0-pzul-74-04u4-1a54mdr0uwi8
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 6_ Hypertension or Asthma Master updated 25-26

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "Hypertension or Asthma Master" (see below). This report has no filtering rules of its own — it reports on its starting population.

## Start population

1. Currently registered patients
2. **6. All on Hypertension or Asthma register** — Include patients who match Patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*.
   - Combines: **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: Hypertension Register***
3. **Hypertension or Asthma Master** — All on Hypertension or Asthma register". Include patients who do not match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR OR patients included in search GROUP3- MR OR patients included in search GROUP4- LR.
   - Combines: **GROUP1- HRC**; **GROUP2- HR**; **GROUP3- MR**; **GROUP4- LR**
4. **This search** — applies the rules below.

## Rule details

No rules — all patients from the starting population are included.

## Report output

These define what the report shows for each patient, not who is included.

### Patient Details

Shows: NHS Number, Usual GP's Organisation Code, Ethnic Origin
No filtering criteria; outputs standard columns.

### Record of Interpreter Information

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs1` (16 codes)
  - Keep only the latest matching record

### Homeless

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs2` (3 codes), or `6_htn_or_asthma_master_updated_25_26_vs3` (2 codes)
  - Keep only the latest matching record

### Housebound

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs4` (2 codes), or `6_htn_or_asthma_master_updated_25_26_vs5` (1 code)
  - Keep only the latest matching record

### Care Home resident

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs6` (3 codes), or `6_htn_or_asthma_master_updated_25_26_vs7` (2 codes)
  - Keep only the latest matching record

### Cardiovascular disease

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Atrial Fibrillation

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Hypertension

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Hyperlipidaemia

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs8` (1 code)
  - Keep only the latest matching record

### Diabetes

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Chronic Kidney Disease

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### NA Fatty Liver Disease

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs9` (12 codes)
  - Keep only the latest matching record

### COPD

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Asthma

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Serious MI

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs10` (2 codes), or `6_htn_or_asthma_master_updated_25_26_vs11` (1 code)
  - Keep only the latest matching record

### Learning Disability

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Keep only the latest matching record

### Frailty

Shows: Code Term
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `6_htn_or_asthma_master_updated_25_26_vs12` (6 codes)
  - Keep only the latest matching record

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs1` |  | Record of Interpreter Information | SNOMED | 16 | Interpreter needed, Interpreter present, Presence of interpreter +13 more | d9a780b0 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs10` |  | Serious MI | SNOMED | 2 | On severe mental illness register, Removed from severe mental illness register | d864ef35 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs11` |  | Serious MI | SNOMED | 1 | On severe mental illness register | d2e1bc0b |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs12` |  | Frailty | SNOMED | 6 | Frailty, On frailty register, Rockwood Clinical Frailty Scale level 6 - moder... | fa6dc79f |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs2` |  | Homeless | SNOMED | 3 | Homeless, Homeless enhanced services administration, No longer homeless | bd017638 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs3` |  | Homeless | SNOMED | 2 | Homeless, Homeless enhanced services administration | c853fe7f |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs4` |  | Housebound | SNOMED | 2 | Housebound, No longer housebound | 7bcc1e58 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs5` |  | Housebound | SNOMED | 1 | Housebound | fb66502d |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs6` |  | Care Home resident | SNOMED | 3 | Lives in care home, Provision of general practitioner intermediate care, Prev... | 23643eb1 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs7` |  | Care Home resident | SNOMED | 2 | Lives in care home, Provision of general practitioner intermediate care | 8aafdf14 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs8` |  | Hyperlipidaemia | SNOMED | 1 | Hyperlipidaemia | b03bd7c6 |
| 6_ Hypertension or Asthma Master updated 25-26 | `6_htn_or_asthma_master_updated_25_26_vs9` |  | NA Fatty Liver Disease | SNOMED | 12 | NAFLD - nonalcoholic fatty liver disease, NAFLD - Nonalcoholic fatty liver di... | 63fd8d6e |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.