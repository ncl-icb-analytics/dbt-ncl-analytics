<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0yxb6pz0-tz9k-i8-12n7-1k97r7v03v4i
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: Asthma CYP Register ONLY

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Asthma CYP Register*" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **LTC LCS: Asthma CYP Register*** — Require Patient Details where Age under 18 years old. Include patients who match Clinical Codes with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **LTC LCS: AF Register***
- They appear in the results of the search **LTC LCS: CKD Register***
- They appear in the results of the search **LTC LCS: CHD Register***
- They appear in the results of the search **LTC LCS: Diabetes Register***
- They appear in the results of the search **LTC LCS: Hypertension Register***
- They appear in the results of the search **LTC LCS: NAFLD Register v2***
- They appear in the results of the search **LTC LCS: Asthma Adult Register***
- They appear in the results of the search **LTC LCS: COPD Register***
- They appear in the results of the search **LTC LCS: HF Register***
- They appear in the results of the search **LTC LCS: PAD Register***
- They appear in the results of the search **LTC LCS: Stroke/TIA Register***

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs1` | ASTRES_COD |  | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs2` | AST_COD |  | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs3` | ASTTRT_COD |  | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.