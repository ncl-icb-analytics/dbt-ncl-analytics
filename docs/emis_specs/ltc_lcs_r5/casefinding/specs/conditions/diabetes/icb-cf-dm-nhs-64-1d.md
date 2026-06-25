# ICB_CF_DM_NHS_64_1D

Report title: [ICB_CF_DM_NHS_64_1D] Eligible for NHS Health check
Folder: 1) Casefinding R2 > [CF-DM] High risk Diabetes
Source: NCL LTC LCS R5.0 updated: 27112025
Description: If eligible for NHS Health Check, offer to complete

## What this search does

Start with the patients found by "ICB_CF_DM_64" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_DM_64_BASE** — Require Patient Details where Age at least 17 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1. Include patients who match any of: Clinical Codes with Body mass index then Latest 1 where numeric value >= 35; OR Clinical Codes with Body mass index then Latest 1 where numeric value >= 32.5 AND Clinical Codes with Ethnic category - 2001 census, Ethnic category - 2011 census, Ethnic group +3 more OR Irish - ethnic category 2001 census, Other - ethnic category 2001 census, Other Mixed background - ethnic category 2001 census +13 more then Latest 999.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_DM_64_woEX** — Exclude patients who match Patients included in search ICB_CF_DM_61_woEX OR patients included in search ICB_CF_DM_62_woEX OR patients included in search ICB_CF_DM_63_woEX. Include patients who do not match Clinical Codes with Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised, HbA1c (haemoglobin A1c) level (monitoring ranges) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised, HbA1c (haemoglobin A1c) level (diagnostic reference range) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised where Date within the last 2 years.
   - Combines: **ICB_CF_DM_61_woEX**; **ICB_CF_DM_62_woEX**; **ICB_CF_DM_63_woEX**
4. **ICB_CF_DM_64** — Include patients who do not match Clinical Codes with High risk of diabetes mellitus annual review declined where Date within the last 3 years.
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
| ICB_CF_DM_64_BASE | `obesity_with_latest_bmi_35_325_bame_population_vs1` |  |  | SNOMED | 1 | Body mass index | 30a0e00c |
| ICB_CF_DM_64_BASE | `obesity_with_latest_bmi_35_325_bame_population_vs2` |  |  | SNOMED | 7 | Ethnic category - 2001 census, Ethnic category - 2011 census, Ethnic group +3... | 3c6a68cb |
| ICB_CF_DM_64_BASE | `obesity_with_latest_bmi_35_325_bame_population_vs3` |  |  | SNOMED | 17 | Irish - ethnic category 2001 census, Other - ethnic category 2001 census, Oth... | 7b6c6e0f |
| ICB_CF_DM_64_woEX | `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` |  |  | SNOMED | 3 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 95d9e41a |
| ICB_CF_DM_64 | `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` |  |  | SNOMED | 1 | High risk of diabetes mellitus annual review declined | 29adf77b |

## Caveats

- ICB_CF_DM_64_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.