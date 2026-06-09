<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 054wuhu1-dkj7-zf-0565-1laymow1fnl8
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Asthma CYP Register*" (see below). A patient is included when they match any one of Rules 1-8.

## Who we start with

1. **LTC LCS: Asthma CYP Register*** — Start with currently registered patients. Require Patient Details where Age under 18 years old. Include patients who match Clinical Codes with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs1` (4 codes), or `on_asthma_cyp_reg_pg2_hr_v2_vs2` (1 code)
  - Keep only the latest matching record
- **Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs3` (2 codes), or `on_asthma_cyp_reg_pg2_hr_v2_vs4` (1 code)
  - Keep only the latest matching record

### Rule 2 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs5` (3 codes)
  - Where date within the last 12 months

### Rule 3 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs6` (39 codes)
  - Where date within the last 12 months

### Rule 4 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs7` (3 codes)
  - Where drug code in `on_asthma_cyp_reg_pg2_hr_v2_vs7` (3 codes)
  - Where issue date within the last 12 months

### Rule 5 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs8` (5 codes)
  - Where drug code in `on_asthma_cyp_reg_pg2_hr_v2_vs8` (5 codes)
  - Where issue date within the last 12 months

### Rule 6 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 7.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs9` (3 codes)
  - Where drug code in `on_asthma_cyp_reg_pg2_hr_v2_vs9` (3 codes)
  - Where issue date within the last 3 months

### Rule 7 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 8.

A patient matches this rule when:
- **Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs10` (1 code), or `on_asthma_cyp_reg_pg2_hr_v2_vs11` (28 codes)
  - Where issue date within the last 12 months
  - Where drug code in `on_asthma_cyp_reg_pg2_hr_v2_vs10` (1 code), `on_asthma_cyp_reg_pg2_hr_v2_vs11` (28 codes)

### Rule 8 of 8

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ALL of the following are true:
- **Medication Issues** — patient must NOT have a matching record
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs12` (6 codes)
  - Where issue date within the last 12 months
  - Where drug code in `on_asthma_cyp_reg_pg2_hr_v2_vs12` (6 codes)
- **Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_vs9` (3 codes)
  - Where drug code in `on_asthma_cyp_reg_pg2_hr_v2_vs9` (3 codes)
  - Where issue date within the last 3 months

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs1` | ASTRES_COD | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs2` | AST_COD | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs3` | ASTTRT_COD | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs1` |  | SNOMED | 4 | Child on protection register, Child removed from protection register, Child n... | 6528bece |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs10` |  | SCT Const | 1 | Bambuterol Hydrochloride | 42efbe32 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs11` |  | SCT_PREP | 28 | Atimos Modulite 12micrograms/dose inhaler (Chiesi Ltd), Foradil 12microgram i... | f9bf039c |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs12` |  | SCT Const | 6 | Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more | a2d6b25c |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs2` |  | SNOMED | 1 | Child on protection register | d9a584fb |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs3` |  | SNOMED | 2 | Child in need, Child no longer in need | 8332ea34 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs4` |  | SNOMED | 1 | Child in need | 17e62b70 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs5` |  | SNOMED | 3 | Emergency hospital admission for asthma, Emergency asthma admission since las... | 7a5550bd |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs6` |  | SNOMED | 39 | Acute exacerbation of asthma, Acute severe exacerbation of asthma, Exacerbati... | ff2958ce |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs7` |  | SCT Const | 3 | Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate | 153a3cb0 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs8` |  | SCT Const | 5 | Theophylline, Theophylline Hydrate, Theophylline S/R +2 more | 08d2e32b |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 | `on_asthma_cyp_reg_pg2_hr_v2_vs9` |  | SCT Const | 3 | Salbutamol, Salbutamol Cr, Terbutaline Sulfate | 5c851fb5 |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.