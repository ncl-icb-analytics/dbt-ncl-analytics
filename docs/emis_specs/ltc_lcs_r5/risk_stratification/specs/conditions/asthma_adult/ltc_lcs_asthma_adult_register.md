<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1yepjte1-05pk-n5-1kuf-1ta9xv30ouzp
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: Asthma Adult Register*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. Patients must match Rule 1 to stay in. A patient is included when they match Rule 2. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

Currently registered patients.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Continue to Rule 2 | Excluded | Filter — must match |
| 2 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 2 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Patient Details**
  - Where age at least 18 years old — `age >= 18 years`

### Rule 2 of 2 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  *"Patients Currently Diagnosed as Asthmatic"*
  - Code in: `asthma_adult_reg_vs1` (1 code — cluster ASTRES_COD), or `asthma_adult_reg_vs2` (1 code — cluster AST_COD)
  - Keep only the latest matching record, and require its code to be in: AST_COD
- **Criterion B — Medication Issues**
  *"Patients Taking Asthma Treatment in last 12 months"*
  - Code in: `asthma_adult_reg_vs3` (521 codes — cluster ASTTRT_COD)
  - Where issue date within the last 12 months — `issue date > today - 12 months`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs1` | ASTRES_COD | 2 | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs2` | AST_COD | 2 | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs3` | ASTTRT_COD | 2 | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.