<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0sgtw000-82da-56-0dcx-19qt0tw1rbe6
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# C) HR+C- Respiratory only

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP1- HRC" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-3. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS Base*** — Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
   - Combines: **LTC LCS: AF Register***; **LTC LCS: CKD Register***; **LTC LCS: CHD Register***; **LTC LCS: Diabetes Register***; **LTC LCS: Hypertension Register***; **LTC LCS: NAFLD Register v2***; **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: COPD Register***; **LTC LCS: HF Register***; **LTC LCS: PAD Register***; **LTC LCS: Stroke/TIA Register***
3. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
   - Combines: **LTC LCS: Asthma CYP Register ONLY**; **On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only)**; **On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only)**
4. **GROUP1- HRC** — Include patients who match any of: Clinical Codes with Refset: 999003371000230102 OR Refset: 999004691000230108 OR Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabetes mellitus with ulcer +79 more then Latest 1; OR Patients included in search On CKD Register- LTC LCS Priority Group 1(HRC) OR patients included in search On CHD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On PAD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*.
   - Combines: **On CKD Register- LTC LCS Priority Group 1(HRC)**; **On CHD Register- LTC LCS Priority Group 1 (HRC)**; **on Diabetes Register- LTC LCS Priority Group 1 (HRC)**; **On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3**; **On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)***; **On COPD Register- LTC LCS Priority Group 1 (HRC)**; **On PAD Register- LTC LCS Priority Group 1 (HRC)**; **On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)***
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 3 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:

- They appear in the results of the search **A) HR+C- Metabolic & Respiratory**

### Rule 2 of 3 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Patients Currently Diagnosed as Asthmatic"*
  - Code in: `c_hrc_respiratory_only_vs1` (2 codes — cluster ASTRES_COD), or `c_hrc_respiratory_only_vs2` (233 codes — cluster AST_COD)
  - Keep only the latest matching record, and require its code to be in: AST_COD
- **Criterion B — Medication Issues**
  *"Patients Taking Asthma Treatment in last 12 months"*
  - Code in: `c_hrc_respiratory_only_vs3` (477 codes — cluster ASTTRT_COD)
  - Where issue date within the last 12 months — `issue date > today - 12 months`

### Rule 3 of 3 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- They match the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935` (see Caveats)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GROUP1- HRC | `high_risk_complexity_vs1` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| GROUP1- HRC | `high_risk_complexity_vs2` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| GROUP1- HRC | `high_risk_complexity_vs3` |  |  | SNOMED | 105 | Type 1 diabetes mellitus, Type I diabetes mellitus with ulcer, Type 1 diabete... | 10923643 |
| C) HR+C- Respiratory only | `c_hrc_respiratory_only_vs1` | ASTRES_COD | 2 | SNOMED | 2 | Asthma resolved | 7cf102c3 |
| C) HR+C- Respiratory only | `c_hrc_respiratory_only_vs2` | AST_COD | 2 | SNOMED | 233 | Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co... | 612ae5b1 |
| C) HR+C- Respiratory only | `c_hrc_respiratory_only_vs3` | ASTTRT_COD | 2 | SNOMED | 477 | Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autoha... | 0e7af9f6 |

## Caveats

- This search references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.