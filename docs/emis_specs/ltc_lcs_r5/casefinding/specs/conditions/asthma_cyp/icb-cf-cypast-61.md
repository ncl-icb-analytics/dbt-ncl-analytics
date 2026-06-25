# ICB_CF_CYPAST_61

Report title: [ICB_CF_CYPAST_61] Asthma CaseFinding: Eligible patients
Folder: 1) Casefinding R2 > [CF-CYPAST] possible diagnosis of asthma (CYP)
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CYPAST_61_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CYPAST_61_BASE** — Require Patient Details where Age at least 18 months old to at most 17 years old. Exclude patients who match Clinical Codes with Asthma resolved OR Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co-occurrent and due to allergic asthma, Allergic asthma with status asthmaticus +213 more then Latest 1 where SNOMED code IN: AST_COD. Include patients who do not match Patients included in search ICS_RESP_LTC OR library item ee5b135f-b9b2-4ef7-8b51-939a754cf935.
   - Combines: **ICS_RESP_LTC**
3. **ICB_CF_CYPAST_61_woEX** — Include patients who match any of: Medication Issues with Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autohaler (3M Health Care Ltd), AeroBec 50 Autohaler (Meda Pharmaceuticals Ltd) +466 more OR Bronchodilators, Corticosteroids For Inhalation, Drugs For Prophylaxis Of Asthma +5 more where Date of Issue within the last 12 months to under now; OR Medication Issues with Prednisolone OR Prednesol Soluble tablets 5 mg, Prednisolone 5mg soluble tablets OR Prednisolone Steaglate, Prednisolone Sodium Phosphate where Date of Issue within the last 1 year; OR Medication Issues with Montelukast Sodium where Date of Issue within the last 1 year; OR Clinical Codes with Suspected asthma where Date within the last 1 year; OR Clinical Codes with Viral induced wheeze where Date within the last 1 year.
4. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `asthma_casefinding_eligible_patients_vs1` (1 code)
  - Where date within the last 3 years — `date > today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CYPAST_61_BASE | `asthma_casefinding_vs1` | ASTRES_COD |  | SNOMED | 2 | Asthma resolved | 7cf102c3 |
| ICB_CF_CYPAST_61_BASE | `asthma_casefinding_vs2` | AST_COD |  | SNOMED | 226 | Moderate acute exacerbation of asthma, Acute severe exacerbation of asthma co... | 59ff1c69 |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs1` |  |  | SCT_PREP | 469 | Accolate 20mg tablets (AstraZeneca UK Ltd), Aerolin 100micrograms/dose Autoha... | 52319d3b |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs2` |  |  | Drug Group | 8 | Bronchodilators, Corticosteroids For Inhalation, Drugs For Prophylaxis Of Ast... | 15dc634b |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs3` |  |  | SCT Const | 1 | Prednisolone | 22c74448 |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs4` |  |  | SCT_PREP | 2 | Prednesol Soluble tablets 5 mg, Prednisolone 5mg soluble tablets | fc45e152 |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs5` |  |  | SCT Const | 2 | Prednisolone Steaglate, Prednisolone Sodium Phosphate | 932bd31d |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs6` |  |  | SCT Const | 1 | Montelukast Sodium | 919ed7f2 |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs7` |  |  | SNOMED | 1 | Suspected asthma | 0f1207d6 |
| ICB_CF_CYPAST_61_woEX | `asthma_casefinding_eligible_patients_vs8` |  |  | SNOMED | 1 | Viral induced wheeze | 72debb07 |
| ICB_CF_CYPAST_61 | `asthma_casefinding_eligible_patients_vs1` |  | 1 | SNOMED | 1 | Respiratory disease screening | 95d46c1f |

## Caveats

- ICB_CF_CYPAST_61_BASE references the EMIS library item `ee5b135f-b9b2-4ef7-8b51-939a754cf935`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.