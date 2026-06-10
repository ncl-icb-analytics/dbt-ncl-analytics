<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 01hvbh50-fefz-nn-1ez0-1gt9qqc0l8fm
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# GROUP2- HR

Report title: [GROUP2- HR] LTC LCS HIGH RISK*- M4:JUL
Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP2- HR" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **LTC LCS Base*** — Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
   - Combines: **LTC LCS: AF Register***; **LTC LCS: CKD Register***; **LTC LCS: CHD Register***; **LTC LCS: Diabetes Register***; **LTC LCS: Hypertension Register***; **LTC LCS: NAFLD Register v2***; **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: COPD Register***; **LTC LCS: HF Register***; **LTC LCS: PAD Register***; **LTC LCS: Stroke/TIA Register***
3. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
   - Combines: **LTC LCS: Asthma CYP Register ONLY**; **On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only)**; **On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only)**
4. **GROUP2- HR** — Exclude patients who match Patients included in search GROUP1- HRC. Include patients who match Patients included in search On AF Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On CKD Register- LTC LCS Priority Group 2 (HR) OR patients included in search On CHD Register- LTC LCS Priority Group 2 (HR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 OR patients included in search On COPD Register- LTC LCS Priority Group 2 (HR) OR patients included in search On HF Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On PAD Register- LTC LCS Priority Group 2 (HR) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*.
   - Combines: **GROUP1- HRC**; **On AF Register- LTC LCS Priority Group 2 (HR)***; **On CKD Register- LTC LCS Priority Group 2 (HR)**; **On CHD Register- LTC LCS Priority Group 2 (HR)**; **on Diabetes Register- LTC LCS Priority Group 2 (HR)**; **On Hypertension Register- LTC LCS Priority Group 2 (HR) v3**; **On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)***; **On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2**; **On COPD Register- LTC LCS Priority Group 2 (HR)**; **On HF Register- LTC LCS Priority Group 2 (HR)***; **On PAD Register- LTC LCS Priority Group 2 (HR)**; **On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)***
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 1 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Patient Details**
  - Where date of birth 7 MONTH ABSOLUTE

## Code lists used

None.

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.