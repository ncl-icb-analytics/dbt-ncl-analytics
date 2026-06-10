<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 000nz0r0-wbwv-io-1f2e-0pz9p9i1uykk
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Interpreter Required

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP3- MR" (see below). A patient is included when they match any one of Rules 1-2. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS Base*** — Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
   - Combines: **LTC LCS: AF Register***; **LTC LCS: CKD Register***; **LTC LCS: CHD Register***; **LTC LCS: Diabetes Register***; **LTC LCS: Hypertension Register***; **LTC LCS: NAFLD Register v2***; **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: COPD Register***; **LTC LCS: HF Register***; **LTC LCS: PAD Register***; **LTC LCS: Stroke/TIA Register***
3. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
   - Combines: **LTC LCS: Asthma CYP Register ONLY**; **On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only)**; **On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only)**
4. **GROUP3- MR** — Exclude patients who match Patients included in search GROUP1- HRC OR patients included in search GROUP2- HR. Include patients who match Patients included in search On AF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On CKD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On CHD Register- LTC LCS Priority Group 3 (MR) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3 OR patients included in search On NAFLD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On COPD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On HF Register- LTC LCS Priority Group 3 (MR)* OR patients included in search On PAD Register- LTC LCS Priority Group 3 (MR) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*.
   - Combines: **GROUP1- HRC**; **GROUP2- HR**; **On AF Register- LTC LCS Priority Group 3 (MR)***; **On CKD Register- LTC LCS Priority Group 3 (MR)**; **On CHD Register- LTC LCS Priority Group 3 (MR)**; **on Diabetes Register- LTC LCS Priority Group 3A (MRa)**; **on Diabetes Register- LTC LCS Priority Group 3B (MRb)**; **On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3**; **On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3**; **On NAFLD Register- LTC LCS Priority Group 3 (MR)**; **On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)***; **On COPD Register- LTC LCS Priority Group 3 (MR)**; **On HF Register- LTC LCS Priority Group 3 (MR)***; **On PAD Register- LTC LCS Priority Group 3 (MR)**; **On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)***
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Continue to Rule 2 | Inclusion route |
| 2 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 2 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `interpreter_required_vs1` (2 codes), or `interpreter_required_vs2` (1 code)
  - Keep only the latest matching record

### Rule 2 of 2 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `interpreter_required_vs3` (1 code)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Interpreter Required | `interpreter_required_vs1` |  | 1 | SNOMED | 2 | Interpreter not needed, Interpreter needed | 32adafca |
| Interpreter Required | `interpreter_required_vs2` |  | 1 | SNOMED | 1 | Interpreter needed | 40c5d030 |
| Interpreter Required | `interpreter_required_vs3` |  | 2 | SNOMED | 1 | British Sign Language interpreter needed | 8acbbf70 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.