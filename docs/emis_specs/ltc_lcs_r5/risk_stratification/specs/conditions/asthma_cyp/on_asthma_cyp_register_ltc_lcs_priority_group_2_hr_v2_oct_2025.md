<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 027k9f11-s62w-hm-1rfs-1w8tbzi0zq1g
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: Asthma CYP Register ONLY" (see below). A patient is included when they match any one of Rules 1-8. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: Asthma CYP Register*** — Require Patient Details where Age under 18 years old. Include patients who match Clinical Codes with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.
3. **LTC LCS: Asthma CYP Register ONLY** — Include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
   - Combines: **LTC LCS: AF Register***; **LTC LCS: CKD Register***; **LTC LCS: CHD Register***; **LTC LCS: Diabetes Register***; **LTC LCS: Hypertension Register***; **LTC LCS: NAFLD Register v2***; **LTC LCS: Asthma Adult Register***; **LTC LCS: COPD Register***; **LTC LCS: HF Register***; **LTC LCS: PAD Register***; **LTC LCS: Stroke/TIA Register***
4. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Continue to Rule 2 | Inclusion route |
| 2 | **Included** | Continue to Rule 3 | Inclusion route |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | **Included** | Continue to Rule 5 | Inclusion route |
| 5 | **Included** | Continue to Rule 6 | Inclusion route |
| 6 | **Included** | Continue to Rule 7 | Inclusion route |
| 7 | **Included** | Continue to Rule 8 | Inclusion route |
| 8 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs1` (4 codes), or `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs2` (1 code)
  - Keep only the latest matching record
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs3` (2 codes), or `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs4` (1 code)
  - Keep only the latest matching record

### Rule 2 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs5` (3 codes)
  - Where date within the last 12 months — `date >= today - 12 months`

### Rule 3 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs6` (39 codes)
  - Where date within the last 12 months — `date >= today - 12 months`

### Rule 4 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:

- **Criterion A — Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs7` (3 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`

### Rule 5 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 6.

A patient matches this rule when:

- **Criterion A — Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs8` (5 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`

### Rule 6 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 7.

A patient matches this rule when:

- **Criterion A — Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9` (3 codes)
  - Where issue date within the last 3 months — `issue date >= today - 3 months`

### Rule 7 of 8 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 8.

A patient matches this rule when:

- **Criterion A — Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs10` (1 code), or `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs11` (28 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`

### Rule 8 of 8 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when **ALL (AND)** of the following are true:

- **Criterion A — Medication Issues** — must NOT exist
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs12` (6 codes)
  - Where issue date within the last 12 months — `issue date >= today - 12 months`
- **Criterion B — Medication Issues**
  - Code in: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9` (3 codes)
  - Where issue date within the last 3 months — `issue date >= today - 3 months`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs1` | ASTRES_COD |  | SNOMED | 1 | Refset: 999010051000230100 | 0cecb4fb |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs2` | AST_COD |  | SNOMED | 1 | Refset: 999012891000230104 | b6f202b4 |
| LTC LCS: Asthma CYP Register* | `asthma_cyp_reg_vs3` | ASTTRT_COD |  | SNOMED | 521 | Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals L... | d78c07e6 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs1` |  | 1 | SNOMED | 4 | Child on protection register, Child removed from protection register, Child n... | 6528bece |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs10` |  | 7 | SCT Const | 1 | Bambuterol Hydrochloride | 42efbe32 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs11` |  | 7 | SCT_PREP | 28 | Atimos Modulite 12micrograms/dose inhaler (Chiesi Ltd), Foradil 12microgram i... | f9bf039c |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs12` |  | 8 | SCT Const | 6 | Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more | a2d6b25c |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs2` |  | 1 | SNOMED | 1 | Child on protection register | d9a584fb |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs3` |  | 1 | SNOMED | 2 | Child in need, Child no longer in need | 8332ea34 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs4` |  | 1 | SNOMED | 1 | Child in need | 17e62b70 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs5` |  | 2 | SNOMED | 3 | Emergency hospital admission for asthma, Emergency asthma admission since las... | 7a5550bd |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs6` |  | 3 | SNOMED | 39 | Acute exacerbation of asthma, Acute severe exacerbation of asthma, Exacerbati... | ff2958ce |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs7` |  | 4 | SCT Const | 3 | Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate | 153a3cb0 |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs8` |  | 5 | SCT Const | 5 | Theophylline, Theophylline Hydrate, Theophylline S/R +2 more | 08d2e32b |
| On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025 | `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9` |  | 6, 8 | SCT Const | 3 | Salbutamol, Salbutamol Cr, Terbutaline Sulfate | 5c851fb5 |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.