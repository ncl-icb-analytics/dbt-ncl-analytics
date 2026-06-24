# ICB_CF_CVD_NHSHC_CHOL_61_2

Report title: [ICB_CF_CVD_NHSHC_CHOL_61_2] Requires Cholesterol check
Folder: 1) Casefinding R2 > [CF-CVD] High risk CVD
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CVD_NHSHC_61_2D" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CVD_61_BASE** — Require Patient Details where Age at least 40 years old to under 84 years old. Include patients who do not match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR Medication Issues with Atorvastatin 10mg tablets, Atorvastatin 20mg tablets, Atorvastatin 40mg tablets +88 more then Latest 1 where issue date > today - 12 months OR Clinical Codes with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes with Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adverse reaction +45 more OR Clinical Codes with Statins contraindicated, Statin not indicated, Statin not tolerated OR Clinical Codes with Statin declined then Latest 1 where date > today - 5 years OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item ea06414e-6bec-4593-837f-5b854c54a8c7.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_CVD_61_woEX** — Include patients who match Clinical Codes with Refset: 999011011000230107 then Latest 1 where numeric value >= 20.
4. **ICB_CF_CVD_61** — Include patients who do not match Clinical Codes with Cardiovascular disease high risk review declined where Date within the last 3 years.
5. **ICB_CF_CVD_NHSHC_61_2D** — Include patients who do not match Patients included in search NHSHC_ELIGIBLE.
   - Combines: **NHSHC_ELIGIBLE**
6. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `requires_cholesterol_check_vs1` (1 code)
  - Where date within the last 3 years — `date >= today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CVD_61_BASE | `people_with_aged_40_84_with_qrisk_recorded_vs1` |  |  | SCT_PREP | 91 | Atorvastatin 10mg tablets, Atorvastatin 20mg tablets, Atorvastatin 40mg table... | fb703d29 |
| ICB_CF_CVD_61_BASE | `people_with_aged_40_84_with_qrisk_recorded_vs2` | STAT_COD |  | SNOMED | 1 | Refset: 12464001000001103 | 2ab5ff0c |
| ICB_CF_CVD_61_BASE | `people_with_aged_40_84_with_qrisk_recorded_vs3` |  |  | SNOMED | 52 | Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adver... | b06e6d97 |
| ICB_CF_CVD_61_BASE | `people_with_aged_40_84_with_qrisk_recorded_vs4` | TXSTAT_COD |  | SNOMED | 3 | Statins contraindicated, Statin not indicated, Statin not tolerated | e99e9880 |
| ICB_CF_CVD_61_BASE | `people_with_aged_40_84_with_qrisk_recorded_vs5` | STATINDEC_COD |  | SNOMED | 1 | Statin declined | ac08be53 |
| ICB_CF_CVD_61_woEX | `people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1` | CVDASS2_COD |  | SNOMED | 1 | Refset: 999011011000230107 | 7b7df77c |
| ICB_CF_CVD_61 | `people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1` |  |  | SNOMED | 1 | Cardiovascular disease high risk review declined | c0e1227a |
| ICB_CF_CVD_NHSHC_CHOL_61_2 | `requires_cholesterol_check_vs1` |  | 1 | SNOMED | 1 | Serum cholesterol level | 474af1a3 |

## Caveats

- ICB_CF_CVD_61_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CVD_61_BASE references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.