# ICB_CF_CVD_66

Report title: [ICB_CF_CVD_66] Aged 75-84 years who are not on a Statin and no QRisk in L5Y
Folder: 1) Casefinding R2 > [CF-CVD] High risk CVD
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CVD_66_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CVD_66_BASE** — Require Patient Details where Age at least 75 years old to under 84 years old. Include patients who do not match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR Medication Issues with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes with Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adverse reaction +45 more OR Clinical Codes with Statins contraindicated, Statin not indicated, Statin not tolerated OR Clinical Codes with Statin declined then Latest 1 where date > today - 5 years OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item ea06414e-6bec-4593-837f-5b854c54a8c7.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_CVD_66_woEX** — Exclude patients who match Patients included in search ICB_CF_CVD_61_woEX OR patients included in search ICB_CF_CVD_62_woEX OR patients included in search ICB_CF_CVD_63_woEX OR patients included in search ICB_CF_CVD_64_woEX OR patients included in search ICB_CF_CVD_65_woEX. Include patients who do not match Clinical Codes with Refset: 999011011000230107 where Date within the last 5 years.
   - Combines: **ICB_CF_CVD_61_woEX**; **ICB_CF_CVD_62_woEX**; **ICB_CF_CVD_63_woEX**; **ICB_CF_CVD_64_woEX**; **ICB_CF_CVD_65_woEX**
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
  - Code in: `aged_75_84_years_who_are_not_on_a_statin_and_no_qrisk_in_l5y_vs1` (1 code)
  - Where date within the last 3 years — `date > today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CVD_66_BASE | `people_with_aged_70_84_with_qrisk_recorded_vs1` | STAT_COD |  | SNOMED | 1 | Refset: 12464001000001103 | 2ab5ff0c |
| ICB_CF_CVD_66_BASE | `people_with_aged_70_84_with_qrisk_recorded_vs2` |  |  | SNOMED | 52 | Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adver... | b06e6d97 |
| ICB_CF_CVD_66_BASE | `people_with_aged_70_84_with_qrisk_recorded_vs3` | TXSTAT_COD |  | SNOMED | 3 | Statins contraindicated, Statin not indicated, Statin not tolerated | e99e9880 |
| ICB_CF_CVD_66_BASE | `people_with_aged_70_84_with_qrisk_recorded_vs4` | STATINDEC_COD |  | SNOMED | 1 | Statin declined | ac08be53 |
| ICB_CF_CVD_66_woEX | `aged_75_84_years_who_are_not_on_a_statin_and_no_qriskin_l5y_vs1` | CVDASS2_COD |  | SNOMED | 1 | Refset: 999011011000230107 | 7b7df77c |
| ICB_CF_CVD_66 | `aged_75_84_years_who_are_not_on_a_statin_and_no_qrisk_in_l5y_vs1` |  | 1 | SNOMED | 1 | Cardiovascular disease high risk review declined | c0e1227a |

## Caveats

- ICB_CF_CVD_66_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CVD_66_BASE references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.