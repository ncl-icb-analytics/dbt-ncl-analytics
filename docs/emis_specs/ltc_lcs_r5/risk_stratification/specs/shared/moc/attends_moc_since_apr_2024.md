<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1ceo9k51-k9y8-87-1hpw-02qbpqq1bwy2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Attends MOC since Apr 2024

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Diabetes Register*" (see below). A patient is included when they match any one of Rules 1-3. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Diabetes Register*** — Require Patient Details where Age at least 17 years old. Include patients who match Clinical Codes with Refset: 999004691000230108 then Latest 1.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Continue to Rule 2 | Inclusion route |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 3 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `attends_moc_since_apr_2024_vs1` (1 code)
  - Where date at least 01/04/2024 — `date >= 01/04/2024`

### Rule 2 of 3 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `attends_moc_since_apr_2024_vs2` (1 code)
  - Where date at least 01/04/2024 — `date >= 01/04/2024`
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `attends_moc_since_apr_2024_vs3` (1 code)
  - Where date at least 01/04/2024 — `date >= 01/04/2024`

### Rule 3 of 3 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `attends_moc_since_apr_2024_vs4` (1 code)
  - Where date at least 01/04/2024 — `date >= 01/04/2024`
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `attends_moc_since_apr_2024_vs5` (1 code)
  - Where date at least 01/04/2024 — `date >= 01/04/2024`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Diabetes Register* | `dm_reg_vs1` | DM_COD |  | SNOMED | 1 | Refset: 999004691000230108 | 2b147092 |
| LTC LCS: Diabetes Register* | `dm_reg_vs2` | DMRES_COD |  | SNOMED | 1 | Refset: 999003371000230102 | ce2851bb |
| Attends MOC since Apr 2024 | `attends_moc_since_apr_2024_vs1` |  | 1 | SNOMED | 1 | Chronic disease initial assessment | 2c55e088 |
| Attends MOC since Apr 2024 | `attends_moc_since_apr_2024_vs2` |  | 2 | SNOMED | 1 | Chronic disease management annual review completed | f10b2cf8 |
| Attends MOC since Apr 2024 | `attends_moc_since_apr_2024_vs3` |  | 2 | SNOMED | 1 | Long term condition summary sent to patient | 3dad0ac0 |
| Attends MOC since Apr 2024 | `attends_moc_since_apr_2024_vs4` |  | 3 | SNOMED | 1 | Personalised Care and Support Plan agreed | 952e7cf1 |
| Attends MOC since Apr 2024 | `attends_moc_since_apr_2024_vs5` |  | 3 | SNOMED | 1 | Review of Personalised Care and Support Plan | 5bfd7289 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.