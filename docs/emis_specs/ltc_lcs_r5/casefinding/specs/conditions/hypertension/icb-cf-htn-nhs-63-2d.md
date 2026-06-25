# ICB_CF_HTN_NHS_63_2D

Report title: [ICB_CF_HTN_NHS_63_2D] Not Eligible for NHS health check
Folder: 1) Casefinding R2 > [CF-HTN] Raised BP(Possible HTN) - UCLP Criteria
Source: NCL LTC LCS R5.0 updated: 27112025
Description: If not eligible for NHS Health Check, offer blood pressure check and if blood pressure high, follow NICE guidance for management of newly diagnosed hypertension

## What this search does

Start with the patients found by "ICB_CF_HTN_63" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_HTN_61_Base** — Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR Clinical Codes with Hypertension resolved, White coat hypertension OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1 OR library item ea06414e-6bec-4593-837f-5b854c54a8c7. Include patients who do not match Clinical Codes with Hypertension resolved OR Diastolic hypertension and systolic hypertension, Combined diastolic and systolic hypertension, Diastolic hypertension co-occurrent with systolic hypertension +175 more then Latest 1 where SNOMED code IN: HYP_COD.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_HTN_63_woEX** — Require Clinical Codes with Black African, Black Caribbean, Black Black - other +73 more; Clinical Codes with SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic blood pressure +17 more OR SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic blood pressure +10 more then Latest 1 where numeric value >= 140 OR Clinical Codes with DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic arterial pressure +18 more OR DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic arterial pressure +11 more then Latest 1 where numeric value >= 90 OR Clinical Codes with SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic blood pressure +17 more OR Average home systolic blood pressure, Home systolic blood pressure, 24 hour systolic blood pressure +4 more then Latest 1 where numeric value >= 135 OR Clinical Codes with DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic arterial pressure +18 more OR Average home diastolic blood pressure, Home diastolic blood pressure, 24 hour diastolic blood pressure +4 more then Latest 1 where numeric value >= 85. Exclude patients who match Patients included in search ICB_CF_HTN_61_woEX; Patients included in search ICB_CF_HTN_62_woEX. Include patients who match Clinical Codes with Mural thrombus of right ventricle following acute myocardial infarction, Postoperative nontransmural myocardial infarction, Postoperative transmural myocardial infarction +432 more OR Clinical Codes with Thrombosis of left middle cerebral artery, Left middle cerebral artery thrombosis, Thrombosis of right middle cerebral artery +262 more OR Transient cerebral ischemia, Anterior circulation transient ischaemic attack, Anterior circulation transient ischemic attack +34 more OR Clinical Codes with Claudication, Charcot  s syndrome, IC - Intermittent claudication +27 more OR Clinical Codes with Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occurrent and due to chronic kidney disease stage 3, Chronic kidney disease stage 5 on dialysis +100 more OR Clinical Codes with GFR (glomerular filtration rate) calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation then Latest 1 where numeric value < 60 OR Clinical Codes with Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosis, Hyperosmolar hyperglycemic coma due to diabetes mellitus without ketoacidosis, Lactic acidosis with diabetes mellitus +423 more OR Clinical Codes with Body mass index, BMI - Body mass index, Weight: body mass +1 more then Latest 1 where numeric value > 35.
   - Combines: **ICB_CF_HTN_61_woEX**; **ICB_CF_HTN_62_woEX**
4. **ICB_CF_HTN_63** — Include patients who do not match Clinical Codes with Hypertension screening where Date within the last 3 years.
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
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs1` |  |  | SNOMED | 78 | Black African, Black Caribbean, Black Black - other +73 more | 88a4167f |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs2` |  |  | SNOMED | 22 | SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic ... | 9313261b |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs3` |  |  | SNOMED | 15 | SAP - Systolic arterial pressure, Systolic blood pressure, Baseline systolic ... | 0d08b99b |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs4` |  |  | SNOMED | 22 | DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic ... | 4ce6e7ac |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs5` |  |  | SNOMED | 15 | DAP - Diastolic arterial pressure, DBP - Diastolic blood pressure, Diastolic ... | 570b4e40 |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs6` |  |  | SNOMED | 7 | Average home systolic blood pressure, Home systolic blood pressure, 24 hour s... | 543c057e |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs7` |  |  | SNOMED | 7 | Average home diastolic blood pressure, Home diastolic blood pressure, 24 hour... | 5339faaa |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs8` | CHD_COD |  | SNOMED | 444 | Mural thrombus of right ventricle following acute myocardial infarction, Post... | e4c73e0d |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs9` | STRK_COD |  | SNOMED | 271 | Thrombosis of left middle cerebral artery, Left middle cerebral artery thromb... | bb3a48ed |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs10` | TIA_COD |  | SNOMED | 38 | Transient cerebral ischemia, Anterior circulation transient ischaemic attack,... | 8b1f1274 |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs11` | PAD_COD |  | SNOMED | 32 | Claudication, Charcot  s syndrome, IC - Intermittent claudication +27 more | e5d7772c |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs12` | CKD_COD |  | SNOMED | 106 | Anaemia co-occurrent and due to chronic kidney disease stage 3, Anemia co-occ... | 68fffd80 |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs13` |  |  | SNOMED | 1 | GFR (glomerular filtration rate) calculated by abbreviated Modification of Di... | 20855d05 |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs14` | DM_COD |  | SNOMED | 527 | Hyperosmolar hyperglycaemic coma due to diabetes mellitus without ketoacidosi... | ea63b842 |
| ICB_CF_HTN_63_woEX | `uclp_priority_group_2b_vs15` |  |  | SNOMED | 5 | Body mass index, BMI - Body mass index, Weight: body mass +1 more | 435b40ad |
| ICB_CF_HTN_63 | `uclp_priority_group_2b_vs1` |  |  | SNOMED | 1 | Hypertension screening | a4b6291a |

## Caveats

- ICB_CF_HTN_61_Base references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_HTN_61_Base references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.