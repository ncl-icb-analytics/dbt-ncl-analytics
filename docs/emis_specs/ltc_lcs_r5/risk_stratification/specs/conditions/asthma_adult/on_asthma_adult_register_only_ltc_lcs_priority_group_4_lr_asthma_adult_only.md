<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1u1mp5o1-68zd-b1-1qh6-01m7oyq1rk2k
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025
Description: On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only)

## What this search does

Start with the patients found by "On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)*" (see below). A patient is included when they do NOT match Rule 1.

## Who we start with

1. **LTC LCS: Asthma Adult Register*** — Start with currently registered patients. Require Patient Details where Age at least 18 years old. Include patients who match Clinical Codes with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.
2. **On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)*** — Start with the patients found by "LTC LCS: Asthma Adult Register*". Exclude patients who match Patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)*. Include patients who match any of: Medication Issues with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 6 months; OR Medication Issues with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 12 months.
3. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **LTC LCS: AF Register***
- They appear in the results of the search **LTC LCS: CKD Register***
- They appear in the results of the search **LTC LCS: CHD Register***
- They appear in the results of the search **LTC LCS: Diabetes Register***
- They appear in the results of the search **LTC LCS: Hypertension Register***
- They appear in the results of the search **LTC LCS: NAFLD Register v2***
- They appear in the results of the search **LTC LCS: Asthma CYP Register***
- They appear in the results of the search **LTC LCS: COPD Register***
- They appear in the results of the search **LTC LCS: HF Register***
- They appear in the results of the search **LTC LCS: PAD Register***
- They appear in the results of the search **LTC LCS: Stroke/TIA Register***

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs1` | ASTRES_COD | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs2` | AST_COD | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs3` | ASTTRT_COD | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |
| On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)* | `on_asthma_adult_reg_pg4_lr_vs1` |  | SCT Const | 6 | Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more | a2d6b25c |
| On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)* | `on_asthma_adult_reg_pg4_lr_vs2` |  | SCT Const | 3 | Salbutamol, Salbutamol Cr, Terbutaline Sulfate | 5c851fb5 |

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.