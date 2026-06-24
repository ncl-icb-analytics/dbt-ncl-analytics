# ICB_CF_DM_NDDP_64_1D

Report title: [ICB_CF_DM_NDDP_64_1D] NDPP Eligible Population
Folder: 1) Casefinding R2 > [CF-DM] High risk Diabetes
Source: NCL LTC LCS R5.0 updated: 27112025
Description: Inclusion
-	Aged over 18
-	Not pregnant
-	HbA1c between 42-47 mmol/mol (6.0-6.4%) or Fasting Plasma Glucose between 5.5-6.9 mmol/l within the last 24 months - based on their most recent blood test result 
-	Patient has a history of Gestational Diabetes (GDM) - patient is eligible with HbA1c <42 mmol/mol or FPG <5.5mmol/l 
 
Exclusion
-	Pregnant
-	Patient invited/referred to DPP in past 6 months
-	Patient who declined DPP invitation in the past 6 months
-	Patient previously completed DPP 
-	Anyone deemed clinically unsuitable for DPP
-	End of life /palliative care
- Currently on Diabetes register

## What this search does

Start with the patients found by "ICB_CF_DM_64" (see below). Patients must match Rules 1 and 3 to stay in. Patients matching Rules 2 and 4-6 are excluded. A patient is included when they do NOT match Rule 7. Rules run in order; each patient stops at the first rule that includes or excludes them.

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
| 1 | Continue to Rule 2 | Excluded | Filter — must match |
| 2 | Excluded | Continue to Rule 3 | Exclusion |
| 3 | Continue to Rule 4 | Excluded | Filter — must match |
| 4 | Excluded | Continue to Rule 5 | Exclusion |
| 5 | Excluded | Continue to Rule 6 | Exclusion |
| 6 | Excluded | Continue to Rule 7 | Exclusion |
| 7 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 7 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Patient Details**
  - Where age at least 18 years old — `age >= 18 years`

### Rule 2 of 7 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"Pregnant in last 12 months"*
  - Code in: `ndpp_eligible_population_vs1` (82 codes — cluster PREG_COD)
  - Keep only the latest matching record, and require its date > today - 12 months

### Rule 3 of 7 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 4; those who do not are excluded.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `ndpp_eligible_population_vs3` (1 code)
  - Keep only the latest matching record, and require its numeric value >= 42 and <= 47 and date >= today - 12 months — `numeric value >= 42 and <= 47 AND date >= today - 12 months`
- **Criterion B — Clinical Codes** (clinical events)
  - Code in: `ndpp_eligible_population_vs4` (3 codes)
  - Keep only the latest matching record, and require its numeric value >= 5.5 and <= 6.9 and date >= today - 12 months — `numeric value >= 5.5 and <= 6.9 AND date >= today - 12 months`
- **Criterion C — Clinical Codes** (clinical events)
  - Code in: `ndpp_eligible_population_vs5` (4 codes)

### Rule 4 of 7 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `ndpp_eligible_population_vs6` (3 codes)
  - Keep only the latest matching record, and require its date >= today - 6 months

### Rule 5 of 7 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 6.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `ndpp_eligible_population_vs7` (2 codes)

### Rule 6 of 7 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 7.

A patient matches this rule when:

- They match the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7` (see Caveats)

### Rule 7 of 7 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when:

- They match the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1` (see Caveats)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_DM_64_BASE | `obesity_with_latest_bmi_35_325_bame_population_vs1` |  |  | SNOMED | 1 | Body mass index | 30a0e00c |
| ICB_CF_DM_64_BASE | `obesity_with_latest_bmi_35_325_bame_population_vs2` |  |  | SNOMED | 7 | Ethnic category - 2001 census, Ethnic category - 2011 census, Ethnic group +3... | 3c6a68cb |
| ICB_CF_DM_64_BASE | `obesity_with_latest_bmi_35_325_bame_population_vs3` |  |  | SNOMED | 17 | Irish - ethnic category 2001 census, Other - ethnic category 2001 census, Oth... | 7b6c6e0f |
| ICB_CF_DM_64_woEX | `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` |  |  | SNOMED | 3 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 95d9e41a |
| ICB_CF_DM_64 | `with_latest_bmi_35325bame_populationand_no_hba1c_in_l2y_vs1` |  |  | SNOMED | 1 | High risk of diabetes mellitus annual review declined | 29adf77b |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs1` | PREG_COD | 2 | SNOMED | 82 | Pregnant - on abdom. palpation, Pregnancy insufficiently advanced for reliabl... | b82805bb |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs2` |  |  | Internal | 1 | Active Problem | 559aead0 |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs3` |  | 3 | SNOMED | 1 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 50ab8202 |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs4` |  | 3 | SNOMED | 3 | Plasma fasting glucose level, Serum fasting glucose level, Fasting blood gluc... | b0cde2cf |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs5` |  | 3 | SNOMED | 4 | Gestational diabetes mellitus, Gestational diabetes mellitus uncontrolled, Ge... | 2e762dad |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs6` |  | 4 | SNOMED | 3 | NHS Diabetes Prevention Programme invitation, Referral to NHS Diabetes Preven... | f10d89c3 |
| ICB_CF_DM_NDDP_64_1D | `ndpp_eligible_population_vs7` |  | 5 | SNOMED | 2 | Unsuitable for NHS Diabetes Prevention Programme, NHS Diabetes Prevention Pro... | eac3168f |

## Caveats

- ICB_CF_DM_64_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This search references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This search references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.