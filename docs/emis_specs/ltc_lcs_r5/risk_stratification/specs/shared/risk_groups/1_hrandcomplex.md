<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0cccj680-xl7j-65-0mmu-0jjfl4k0oqgg
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 1_HRandCOMPLEX

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

Shows: NHS Number, Usual GP's Organisation Code
No filtering criteria; outputs standard columns.

### Record of Interpreter Information

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `1hrandcomplex_vs1` (16 codes)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GROUP1- HRC | `high_risk_complexity_vs1` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| GROUP1- HRC | `high_risk_complexity_vs2` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| GROUP1- HRC | `high_risk_complexity_vs3` |  |  | SNOMED | 105 | Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabete... | 10923643 |
| 1_HRandCOMPLEX | `1hrandcomplex_vs1` |  | Record of Interpreter Information | SNOMED | 16 | Interpreter needed, Interpreter present, Presence of interpreter +13 more | d9a780b0 |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.