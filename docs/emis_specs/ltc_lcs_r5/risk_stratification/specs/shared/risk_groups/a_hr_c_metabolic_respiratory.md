<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 13qjr8z0-9e94-ok-1hro-17g5owd11ozi
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# A) HR+C- Metabolic & Respiratory

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "GROUP1- HRC" (see below). Patients must match Rule 1 to stay in. A patient is included when they match any one of Rules 2-3. Rules run in order; each patient stops at the first rule that includes or excludes them.

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
| 1 | Continue to Rule 2 | Excluded | Filter — must match |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 3 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- They match the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1` (see Caveats)
- They match the EMIS library item **CHD Register** (see Caveats)
- They match the EMIS library item **PAD Register** (see Caveats)
- They match the EMIS library item **Stroke/TIA Register** (see Caveats)
- They match the EMIS library item **HF Register** (see Caveats)
- They match the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e` (see Caveats)
- They match the EMIS library item **Hypertension Register** (see Caveats)
- They match the EMIS library item **AF Register** (see Caveats)
- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `a_hrc_metabolic_respiratory_vs1` (2 codes)

### Rule 2 of 3 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Patients Currently Diagnosed as Asthmatic"*
  - Code in: `a_hrc_metabolic_respiratory_vs2` (2 codes — cluster ASTRES_COD), or `a_hrc_metabolic_respiratory_vs3` (233 codes — cluster AST_COD)
  - Keep only the latest matching record, and require its code to be in: AST_COD
- **Criterion B — Medication Issues**
  *"Patients Taking Asthma Treatment in last 12 months"*
  - Code in: `a_hrc_metabolic_respiratory_vs4` (477 codes — cluster ASTTRT_COD)
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
| A) HR+C- Metabolic & Respiratory | `a_hrc_metabolic_respiratory_vs1` |  | 1 | SNOMED | 2 | NAFLD - Nonalcoholic fatty liver disease, Non-alcoholic fatty liver | 41d2e52e |
| A) HR+C- Metabolic & Respiratory | `a_hrc_metabolic_respiratory_vs2` | ASTRES_COD | 2 | SNOMED | 2 | Asthma resolved | 7cf102c3 |
| A) HR+C- Metabolic & Respiratory | `a_hrc_metabolic_respiratory_vs3` | AST_COD | 2 | SNOMED | 233 | Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co... | 612ae5b1 |
| A) HR+C- Metabolic & Respiratory | `a_hrc_metabolic_respiratory_vs4` | ASTTRT_COD | 2 | SNOMED | 477 | Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autoha... | 0e7af9f6 |

## Caveats

- This search references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This search references the EMIS library item `d730ee6f-1b38-4553-8f8e-7dc8b3042f4c`, whose logic is not included in this XML export. It is likely **CHD Register** (inferred from wrapper report "LTC LCS: CHD Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `ffccdb77-bd5e-47fc-add3-d700835ace65`, whose logic is not included in this XML export. It is likely **PAD Register** (inferred from wrapper report "LTC LCS: PAD Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `d4e6f787-dbce-4f0b-9f3f-498808ebad42`, whose logic is not included in this XML export. It is likely **Stroke/TIA Register** (inferred from wrapper report "LTC LCS: Stroke/TIA Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `79888a16-aa09-4ef4-ba5e-a3be8e1daf23`, whose logic is not included in this XML export. It is likely **HF Register** (inferred from wrapper report "LTC LCS: HF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `c913f5a7-1256-4de6-871e-23650e72765e`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This search references the EMIS library item `a5ff1b4e-f130-4fea-b11c-5b40dc9b0877`, whose logic is not included in this XML export. It is likely **Hypertension Register** (inferred from wrapper report "LTC LCS: Hypertension Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `e6742de9-2073-4a23-8c94-e05f668eaabf`, whose logic is not included in this XML export. It is likely **AF Register** (inferred from wrapper report "LTC LCS: AF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.