<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0ne8vpf1-ed5f-oc-13mb-1apy5371lgjd
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 3_MR

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP3- MR" (see below). This report has no filtering rules of its own — it reports on its starting population.

## Who we start with

1. **LTC LCS Base*** — Start with currently registered patients. Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
2. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Start with the patients found by "LTC LCS Base*". Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
3. **GROUP3- MR** — Start with the patients found by "LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only". Exclude patients who match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR. Include patients who match Patients included in search On AF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On CKD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On CHD Register- LTC LCS Priority Group 3 (MR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3 OR patients included in search On NAFLD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On COPD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On HF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On PAD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*.
4. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

No rules — all patients from the starting population are included.

## Report output

These define what the report shows for each patient, not who is included.

### Patient Details

### Record of Interpreter Information
- **Clinical Codes** (clinical events)
  - Code in: `3mr_vs1` (16 codes)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| 3_MR | `3mr_vs1` |  | SNOMED | 16 | Interpreter needed, Interpreter present, Presence of interpreter +13 more | d9a780b0 |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.