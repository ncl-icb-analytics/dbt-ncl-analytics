<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 058ijs21-6wve-bw-020g-0q44mtw0wq6x
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On COPD Register- LTC LCS Priority Group 3 (MR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: COPD Register*" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-6. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: COPD Register*** — Include patients who match any of: Library item ee5b135f-b9b2-4ef7-8b51-939a754cf935; OR Clinical Codes with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date before 1 year ago then Earliest 1 where SNOMED code IN: COPD_COD with nested group (Clinical Codes with Refset: 999011571000230107 then Earliest 1 AND Clinical Codes NOT with Refset: 999009131000230100) with nested group (Clinical Codes with Refset: 999009131000230100 then Latest 1); OR Clinical Codes with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year with nested group (Clinical Codes with Refset: 999011571000230107 then Earliest 1 AND Clinical Codes NOT with Refset: 999009131000230100) with nested group (Clinical Codes with Refset: 999009131000230100 then Latest 1) OR Clinical Codes with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year with nested group (Clinical Codes with Refset: 999011571000230107 then Earliest 1 AND Clinical Codes NOT with Refset: 999009131000230100) with nested group (Clinical Codes with Refset: 999009131000230100 then Latest 1); OR Clinical Codes with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD with nested group (Clinical Codes with Refset: 999011571000230107 then Earliest 1 AND Clinical Codes NOT with Refset: 999009131000230100) with nested group (Clinical Codes with Refset: 999009131000230100 then Latest 1) AND Patient Details where Registration Date within the last 12 months; OR Clinical Codes with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD with nested group (Clinical Codes with Refset: 999011571000230107 then Earliest 1 AND Clinical Codes NOT with Refset: 999009131000230100) with nested group (Clinical Codes with Refset: 999009131000230100 then Latest 1) AND Patient Details where Registration Date within the last 12 months; OR Clinical Codes with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD with nested group (Clinical Codes with Refset: 999011571000230107 then Earliest 1 AND Clinical Codes NOT with Refset: 999009131000230100) with nested group (Clinical Codes with Refset: 999009131000230100 then Latest 1).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Continue to Rule 6 | Inclusion route |
| 6 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 6 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **On COPD Register- LTC LCS Priority Group 1 (HRC)**
- They appear in the results of the search **On COPD Register- LTC LCS Priority Group 2 (HR)**

### Rule 2 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_copd_reg_pg3_mr_vs1` (1 code)
  - Keep only the latest matching record, and require its numeric value >= 50 and < 80

### Rule 3 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"MRC Breathlessness in past 12 months"*
  - Code in: `on_copd_reg_pg3_mr_vs2` (9 codes)

### Rule 4 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_copd_reg_pg3_mr_vs3` (1 code)
  - Where date within the last 12 months — `date >= today - 12 months`
- **Criterion B — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs4` (9 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`
- **Criterion C — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs5` (3 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`
- **Criterion D — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs6` (2 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`

### Rule 5 of 6 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:

- **Criterion A — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs7` (2 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`

### Rule 6 of 6 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs8` (6 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion B — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs9` (1 code), or `on_copd_reg_pg3_mr_vs10` (4 codes), or `on_copd_reg_pg3_mr_vs11` (3 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion C — Medication Issues**
  - Code in: `on_copd_reg_pg3_mr_vs12` (6 codes), or `on_copd_reg_pg3_mr_vs13` (21 codes), or `on_copd_reg_pg3_mr_vs14` (1 code), or `on_copd_reg_pg3_mr_vs15` (25 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: COPD Register* | `copd_reg_vs1` | COPD_COD |  | SNOMED | 1 | Refset: 999011571000230107 | 95fd66a2 |
| LTC LCS: COPD Register* | `copd_reg_vs2` | COPDRES_COD |  | SNOMED | 1 | Refset: 999009131000230100 | c378d825 |
| LTC LCS: COPD Register* | `copd_reg_vs3` | FEV1FVC_COD |  | SNOMED | 1 | Refset: 999020251000230104 | 858d7625 |
| LTC LCS: COPD Register* | `copd_reg_vs4` | FEV1FVCL70_COD |  | SNOMED | 1 | Refset: 999020291000230109 | faf72240 |
| LTC LCS: COPD Register* | `copd_reg_vs5` |  |  | SNOMED | 1 | UK NHS primary care data extraction - General practice data extraction - FEV1... | 0a5a44d1 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs1` |  | 2 | SNOMED | 1 | Percent predicted FEV1 | 626f71bc |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs10` |  | 6 | SCT_PREP | 4 | Glycopyrronium bromide 55microgram inhalation powder capsules with device, Se... | 3eb04eed |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs11` |  | 6 | SCT Const | 3 | Tiotropium bromide monohydrate, Umeclidinium Bromide, Ipratropium Bromide | a51ecb26 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs12` |  | 6 | SCT Const | 6 | Bambuterol Hydrochloride, Formoterol Fumarate, Indacaterol +3 more | b007bfaf |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs13` |  | 6 | SCT_PREP | 21 | Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inh... | de9c095f |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs14` |  | 6 | SCT Const | 1 | Fluticasone Furoate | 0af2168e |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs15` |  | 6 | SCT_PREP | 25 | Aerivio Spiromax 50micrograms/dose / 500micrograms/dose dry powder inhaler (T... | 34fefbc2 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs2` |  | 3 | SNOMED | 9 | Medical Research Council (MRC) Breathlessness Scale: grade 2, Medical Researc... | e8058397 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs3` |  | 4 | SNOMED | 1 | 1 COPD exacerbation in past year | 6b17f8e2 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs4` |  | 4 | SCT Const | 9 | Amoxicillin, Amoxicillin Trihydrate, Doxycycline +6 more | b191f1eb |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs5` |  | 4 | SCT Const | 3 | Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate | 153a3cb0 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs6` |  | 4 | SCT Const | 2 | Azithromycin | c5da1ae5 |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs7` |  | 5 | Brand | 2 | Trimbow, Trelegy Ellipta | b8d2997b |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs8` |  | 6 | SCT Const | 6 | Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more | a2d6b25c |
| On COPD Register- LTC LCS Priority Group 3 (MR) | `on_copd_reg_pg3_mr_vs9` |  | 6 | SCT Const | 1 | Aclidinium Bromide | bdebe649 |

## Caveats

- LTC LCS: COPD Register* references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.