<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 17wu99q0-hff3-3m-06aa-0ljbicw0ming
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Asthma Adult Register*" (see below). Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-7. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

1. **LTC LCS: Asthma Adult Register*** — Start with currently registered patients. Require Patient Details where Age at least 18 years old. Include patients who match Clinical Codes with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 7

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)***
- They appear in the results of the search **On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)***

### Rule 2 of 7

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_asthma_adult_reg_pg3_mr_vs1` (1 code)
  - Where date within the last 12 months

### Rule 3 of 7

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_adult_reg_pg3_mr_vs2` (3 codes)
  - Where issue date within the last 12 months

### Rule 4 of 7

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_adult_reg_pg3_mr_vs3` (9 codes)
  - Where issue date within the last 12 months

### Rule 5 of 7

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_adult_reg_pg3_mr_vs4` (3 codes)
  - Where issue date within the last 12 months

### Rule 6 of 7

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 7.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_asthma_adult_reg_pg3_mr_vs5` (1 code), or `on_asthma_adult_reg_pg3_mr_vs6` (28 codes)
  - Where issue date within the last 6 months
- **Medication Issues** — patient must NOT have a matching record
  - Code in: `on_asthma_adult_reg_pg3_mr_vs7` (6 codes)
  - Where issue date within the last 6 months

### Rule 7 of 7

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ALL of the following are true:
- **Medication Issues**
  - Code in: `on_asthma_adult_reg_pg3_mr_vs4` (3 codes)
  - Where issue date within the last 12 months
- **Medication Issues** — patient must NOT have a matching record
  - Code in: `on_asthma_adult_reg_pg3_mr_vs7` (6 codes)
  - Where issue date within the last 6 months

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs1` | ASTRES_COD | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs2` | AST_COD | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma Adult Register* | `asthma_adult_reg_vs3` | ASTTRT_COD | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs1` |  | SNOMED | 1 | Acute exacerbation of asthma | 600d69b8 |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs2` |  | SCT Const | 3 | Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate | 153a3cb0 |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs3` |  | SCT Const | 9 | Amoxicillin, Amoxicillin Trihydrate, Doxycycline +6 more | b191f1eb |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs4` |  | SCT Const | 3 | Salbutamol, Salbutamol Cr, Terbutaline Sulfate | 5c851fb5 |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs5` |  | SCT Const | 1 | Bambuterol Hydrochloride | 42efbe32 |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs6` |  | SCT_PREP | 28 | Atimos Modulite 12micrograms/dose inhaler (Chiesi Ltd), Foradil 12microgram i... | f9bf039c |
| On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* | `on_asthma_adult_reg_pg3_mr_vs7` |  | SCT Const | 6 | Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more | a2d6b25c |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.