<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1m0bjxd0-dsp8-w3-02o0-1fuvsr51rwjo
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Asthma Adult Register*" (see below). A patient is included when they match Rule 1.

## Who we start with

1. **LTC LCS: Asthma Adult Register*** — Start with currently registered patients. Require Patient Details where Age at least 18 years old. Include patients who match Clinical Codes with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_adult_reg_pg1_hrc_vs1` (4 codes)
  - Where issue date within the last 12 months

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs1` | ASTRES_COD | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs2` | AST_COD | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs3` | ASTTRT_COD | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |
| On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* | `on_asthma_adult_reg_pg1_hrc_vs1` |  | SCT Const | 4 | Omalizumab, Mepolizumab, Reslizumab +1 more | b42586ad |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.