# ICB_CF_CVD_NHSHC_64_1D

Report title: [ICB_CF_CVD_NHSHC_64_1D] NHS health check Eligible
Folder: 1) Casefinding R2 > [CF-CVD] High risk CVD
Source: NCL LTC LCS R5.0 updated: 27112025
Description: If eligible for NHS Health Check, offer to complete

## What this search does

Start with the patients found by "ICB_CF_CVD_64" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CVD_63_BASE** — Require Patient Details where Age at least 40 years old to under 84 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR Clinical Codes with Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adverse reaction +45 more OR Clinical Codes with Statins contraindicated, Statin not indicated, Statin not tolerated OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item ea06414e-6bec-4593-837f-5b854c54a8c7. Include patients who match Clinical Codes with Refset: 999011011000230107 then Latest 1 where numeric value >= 10.
   - Combines: **ICS_METABOLIC_LTC**
3. **ICB_CF_CVD_64_woEX** — Exclude patients who match Patients included in search ICB_CF_CVD_61_woEX OR patients included in search ICB_CF_CVD_62_woEX OR patients included in search ICB_CF_CVD_63_woEX. Include patients who do not match Medication Issues with Refset: 12464001000001103 then Latest 1 where issue date > today - 12 months OR Clinical Codes with Refset: 12464001000001103 then Latest 1 where date > today - 12 months OR Clinical Codes with Statin declined then Latest 1 where date > today - 5 years.
   - Combines: **ICB_CF_CVD_61_woEX**; **ICB_CF_CVD_62_woEX**; **ICB_CF_CVD_63_woEX**
4. **ICB_CF_CVD_64** — Include patients who do not match Clinical Codes with Cardiovascular disease high risk review declined where Date within the last 3 years.
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 1 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- They appear in the results of the search **NHSHC_ELIGIBLE**

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CVD_63_BASE | `people_with_qrisk_10_vs1` |  |  | SNOMED | 52 | Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adver... | b06e6d97 |
| ICB_CF_CVD_63_BASE | `people_with_qrisk_10_vs2` | TXSTAT_COD |  | SNOMED | 3 | Statins contraindicated, Statin not indicated, Statin not tolerated | e99e9880 |
| ICB_CF_CVD_63_BASE | `people_with_qrisk_10_vs3` | CVDASS2_COD |  | SNOMED | 1 | Refset: 999011011000230107 | 7b7df77c |
| ICB_CF_CVD_64_woEX | `with_a_qrisk2_10_and_not_on_a_statin_vs1` | STAT_COD |  | SNOMED | 1 | Refset: 12464001000001103 | 2ab5ff0c |
| ICB_CF_CVD_64_woEX | `with_a_qrisk2_10_and_not_on_a_statin_vs2` | STATINDEC_COD |  | SNOMED | 1 | Statin declined | ac08be53 |
| ICB_CF_CVD_64 | `with_a_qrisk2_10_and_not_on_a_statin_vs1` |  |  | SNOMED | 1 | Cardiovascular disease high risk review declined | c0e1227a |

## Caveats

- ICB_CF_CVD_63_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CVD_63_BASE references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.