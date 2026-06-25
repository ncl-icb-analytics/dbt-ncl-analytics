# ICB_CF_CVD_NHSHC_65_1N

Report title: [ICB_CF_CVD_NHSHC_65_1N] Requires NHS Health check
Folder: 1) Casefinding R2 > [CF-CVD] High risk CVD
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_CVD_NHSHC_65_1D" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_CVD_63_BASE** — Require Patient Details where Age at least 40 years old to under 84 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR Clinical Codes with Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adverse reaction +45 more OR Clinical Codes with Statins contraindicated, Statin not indicated, Statin not tolerated OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item ea06414e-6bec-4593-837f-5b854c54a8c7. Include patients who match Clinical Codes with Refset: 999011011000230107 then Latest 1 where numeric value >= 10.
   - Combines: **ICS_METABOLIC_LTC**
3. **ICB_CF_CVD_65_woEX** — Require Clinical Codes with Refset: 999011011000230107 then Latest 1 where numeric value >= 10. Exclude patients who match Patients included in search ICB_CF_CVD_61_woEX OR patients included in search ICB_CF_CVD_62_woEX OR patients included in search ICB_CF_CVD_63_woEX OR patients included in search ICB_CF_CVD_64_woEX; Clinical Codes with Adverse reaction caused by statin, Adverse reaction to antilipaemic and antiarteriosclerotic drugs NOS, Lipid-lowering drug adverse reaction +14 more. Include patients who match Medication Courses with Atorvastatin, Simvastatin, Fluvastatin +2 more.
   - Combines: **ICB_CF_CVD_61_woEX**; **ICB_CF_CVD_62_woEX**; **ICB_CF_CVD_63_woEX**; **ICB_CF_CVD_64_woEX**
4. **ICB_CF_CVD_65** — Include patients who do not match Clinical Codes with Cardiovascular disease high risk review declined where Date within the last 3 years.
5. **ICB_CF_CVD_NHSHC_65_1D** — Include patients who match Patients included in search NHSHC_ELIGIBLE.
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
  - Code in: `requires_nhs_health_check_vs1` (1 code)
  - Where date within the last 2 years — `date >= today - 2 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_CVD_63_BASE | `people_with_qrisk_10_vs1` |  |  | SNOMED | 52 | Statin adverse reaction, Adverse reaction caused by statin, Pravastatin adver... | b06e6d97 |
| ICB_CF_CVD_63_BASE | `people_with_qrisk_10_vs2` | TXSTAT_COD |  | SNOMED | 3 | Statins contraindicated, Statin not indicated, Statin not tolerated | e99e9880 |
| ICB_CF_CVD_63_BASE | `people_with_qrisk_10_vs3` | CVDASS2_COD |  | SNOMED | 1 | Refset: 999011011000230107 | 7b7df77c |
| ICB_CF_CVD_65_woEX | `with_qrisk210_amd_not_on_a_high_intensity_statin_vs1` |  |  | SNOMED | 19 | Adverse reaction caused by statin, Adverse reaction to antilipaemic and antia... | c985e0b5 |
| ICB_CF_CVD_65_woEX | `with_qrisk210_amd_not_on_a_high_intensity_statin_vs2` | CVDASS2_COD |  | SNOMED | 1 | Refset: 999011011000230107 | 7b7df77c |
| ICB_CF_CVD_65_woEX | `with_qrisk210_amd_not_on_a_high_intensity_statin_vs3` |  |  | SCT Const | 5 | Atorvastatin, Simvastatin, Fluvastatin +2 more | 41bb4af0 |
| ICB_CF_CVD_65 | `with_qrisk210_amd_not_on_a_high_intensity_statin_vs1` |  |  | SNOMED | 1 | Cardiovascular disease high risk review declined | c0e1227a |
| ICB_CF_CVD_NHSHC_65_1N | `requires_nhs_health_check_vs1` |  | 1 | SNOMED | 1 | NHS Health Check completed | 69caa6b6 |

## Caveats

- ICB_CF_CVD_63_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_CVD_63_BASE references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.