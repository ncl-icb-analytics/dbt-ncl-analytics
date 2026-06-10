<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0t7a40d0-33c6-bg-1fzx-052017d0cnen
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On COPD Register- LTC LCS Priority Group 2 (HR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: COPD Register*" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-5. Rules run in order; each patient stops at the first rule that includes or excludes them.

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
| 5 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 5 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:

- They appear in the results of the search **On COPD Register- LTC LCS Priority Group 1 (HRC)**

### Rule 2 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_copd_reg_pg2_hr_vs1` (1 code)
  - Keep only the latest matching record, and require its numeric value < 50

### Rule 3 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_copd_reg_pg2_hr_vs2` (1 code)
  - Where date within the last 5 years — `date >= today - 5 years`

### Rule 4 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"MRC Breathlessness in past 12 months"*
  - Code in: `on_copd_reg_pg2_hr_vs3` (3 codes)

### Rule 5 of 5 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_copd_reg_pg2_hr_vs4` (3 codes)
  - Where date within the last 12 months — `date >= today - 12 months`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: COPD Register* | `copd_reg_vs1` | COPD_COD |  | SNOMED | 1 | Refset: 999011571000230107 | 95fd66a2 |
| LTC LCS: COPD Register* | `copd_reg_vs2` | COPDRES_COD |  | SNOMED | 1 | Refset: 999009131000230100 | c378d825 |
| LTC LCS: COPD Register* | `copd_reg_vs3` | FEV1FVC_COD |  | SNOMED | 1 | Refset: 999020251000230104 | 858d7625 |
| LTC LCS: COPD Register* | `copd_reg_vs4` | FEV1FVCL70_COD |  | SNOMED | 1 | Refset: 999020291000230109 | faf72240 |
| LTC LCS: COPD Register* | `copd_reg_vs5` |  |  | SNOMED | 1 | UK NHS primary care data extraction - General practice data extraction - FEV1... | 0a5a44d1 |
| On COPD Register- LTC LCS Priority Group 2 (HR) | `on_copd_reg_pg2_hr_vs1` |  | 2 | SNOMED | 1 | Percent predicted FEV1 | 626f71bc |
| On COPD Register- LTC LCS Priority Group 2 (HR) | `on_copd_reg_pg2_hr_vs2` |  | 3 | SNOMED | 1 | Chronic cor pulmonale | 4e35cb88 |
| On COPD Register- LTC LCS Priority Group 2 (HR) | `on_copd_reg_pg2_hr_vs3` |  | 4 | SNOMED | 3 | Medical Research Council (MRC) Breathlessness Scale: grade 4, Medical Researc... | ea13ad7c |
| On COPD Register- LTC LCS Priority Group 2 (HR) | `on_copd_reg_pg2_hr_vs4` |  | 5 | SNOMED | 3 | 2 COPD exacerbations in past year, 3+ COPD exacerbations in past year, Acute ... | d533f11c |

## Caveats

- LTC LCS: COPD Register* references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.