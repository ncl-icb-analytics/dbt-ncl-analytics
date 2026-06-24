# ICB_CF_HTN_NHS_61_2D

Report title: [ICB_CF_HTN_NHS_61_2D] Not Eligible for NHS health check
Folder: 1) Casefinding R2 > [CF-HTN] Raised BP(Possible HTN) - UCLP Criteria
Source: NCL LTC LCS R5.0 updated: 27112025
Description: If not eligible for NHS Health Check, offer blood pressure check and if blood pressure high, follow NICE guidance for management of newly diagnosed hypertension

## What this search does

Start with the patients found by "ICB_CF_HTN_61" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_HTN_61_Base** — Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR Clinical Codes with Hypertension resolved, White coat hypertension OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item ea06414e-6bec-4593-837f-5b854c54a8c7. Include patients who do not match Clinical Codes with Hypertension resolved OR Diastolic hypertension and systolic hypertension, Combined diastolic and systolic hypertension, Diastolic hypertension co-occurrent with systolic hypertension +175 more then Latest 1 where SNOMED code IN: HYP_COD.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_HTN_61_woEX** — Include patients who match Clinical Codes with SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic blood pressure +17 more OR SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic blood pressure +10 more then Latest 1 where numeric value >= 180 OR Clinical Codes with DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic arterial pressure +18 more OR DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic arterial pressure +11 more then Latest 1 where numeric value >= 120 OR Clinical Codes with SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic blood pressure +17 more OR Average home systolic blood pressure, Home systolic blood pressure, 24 hour systolic blood pressure +4 more then Latest 1 where numeric value >= 170 OR Clinical Codes with DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic arterial pressure +18 more OR Average home diastolic blood pressure, Home diastolic blood pressure, 24 hour diastolic blood pressure +4 more then Latest 1 where numeric value >= 115.
4. **ICB_CF_HTN_61** — Include patients who do not match Clinical Codes with Hypertension screening where Date within the last 3 years.
5. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- They appear in the results of the search **NHSHC_ELIGIBLE**

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_HTN_61_Base | `uclp_priority_groups_base_vs1` |  |  | SNOMED | 2 | Hypertension resolved, White coat hypertension | de88558b |
| ICB_CF_HTN_61_Base | `uclp_priority_groups_base_vs2` | HYPRES_COD |  | SNOMED | 2 | Hypertension resolved | 208cb978 |
| ICB_CF_HTN_61_Base | `uclp_priority_groups_base_vs3` | HYP_COD |  | SNOMED | 181 | Diastolic hypertension and systolic hypertension, Combined diastolic and syst... | 95fdda92 |
| ICB_CF_HTN_61_woEX | `uclp_pg1_highest_risk_vs1` |  |  | SNOMED | 22 | SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic ... | 9313261b |
| ICB_CF_HTN_61_woEX | `uclp_pg1_highest_risk_vs2` |  |  | SNOMED | 15 | SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic ... | 0d08b99b |
| ICB_CF_HTN_61_woEX | `uclp_pg1_highest_risk_vs3` |  |  | SNOMED | 22 | DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic ... | 4ce6e7ac |
| ICB_CF_HTN_61_woEX | `uclp_pg1_highest_risk_vs4` |  |  | SNOMED | 15 | DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic ... | 570b4e40 |
| ICB_CF_HTN_61_woEX | `uclp_pg1_highest_risk_vs5` |  |  | SNOMED | 7 | Average home systolic blood pressure, Home systolic blood pressure, 24 hour s... | 543c057e |
| ICB_CF_HTN_61_woEX | `uclp_pg1_highest_risk_vs6` |  |  | SNOMED | 7 | Average home diastolic blood pressure, Home diastolic blood pressure, 24 hour... | 5339faaa |
| ICB_CF_HTN_61 | `uclp_pg1_highest_risk_vs1` |  |  | SNOMED | 1 | Hypertension screening | a4b6291a |

## Caveats

- ICB_CF_HTN_61_Base references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_HTN_61_Base references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.