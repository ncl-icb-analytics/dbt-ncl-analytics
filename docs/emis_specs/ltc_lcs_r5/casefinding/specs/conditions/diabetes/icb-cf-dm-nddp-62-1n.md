# ICB_CF_DM_NDDP_62_1N

Report title: [ICB_CF_DM_NDDP_62_1N] Eligible for NDPP health check referral
Folder: 1) Casefinding R2 > [CF-DM] High risk Diabetes
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_DM_NDDP_62_1D" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_DM_62_BASE** — Require Patient Details where Age at least 17 years old. Exclude patients who match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1. Include patients who match Clinical Codes with Gestational diabetes mellitus, H/O gestational diabetes mellitus, Gestational diabetes mellitus uncontrolled +2 more.
   - Combines: **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_DM_62_woEX** — Exclude patients who match Patients included in search ICB_CF_DM_61_woEX. Include patients who do not match Clinical Codes with Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised, HbA1c (haemoglobin A1c) level (monitoring ranges) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised, HbA1c (haemoglobin A1c) level (diagnostic reference range) - IFCC (International Federation of Clinical Chemistry and Laboratory Medicine) standardised where Date within the last 1 year.
   - Combines: **ICB_CF_DM_61_woEX**
4. **ICB_CF_DM_62** — Include patients who do not match Clinical Codes with High risk of diabetes mellitus annual review declined where Date within the last 3 years.
5. **ICB_CF_DM_NDDP_62_1D** — Require Patient Details where Age at least 18 years old; Clinical Codes with Haemoglobin A1c level - International Federation of Clinical Chemistry and Laboratory Medicine standardised then Latest 1 where numeric value >= 42 and <= 47 and date >= today - 12 months OR Clinical Codes with Plasma fasting glucose level, Serum fasting glucose level, Fasting blood glucose level then Latest 1 where numeric value >= 5.5 and <= 6.9 and date >= today - 12 months OR Clinical Codes with Gestational diabetes mellitus, Gestational diabetes mellitus uncontrolled, Gestational diabetes mellitus in childbirth +1 more. Exclude patients who match Clinical Codes with Pregnant - on abdom. palpation, Pregnancy insufficiently advanced for reliable antenatal screening, Pregnancy too advanced for reliable antenatal screening +77 more OR Active Problem then Latest 1 where date > today - 12 months; Clinical Codes with NHS Diabetes Prevention Programme invitation, Referral to NHS Diabetes Prevention Programme, Referral to NHS Diabetes Prevention Programme declined then Latest 1 where date >= today - 6 months; Clinical Codes with Unsuitable for NHS Diabetes Prevention Programme, NHS Diabetes Prevention Programme completed; Library item ea06414e-6bec-4593-837f-5b854c54a8c7. Include patients who do not match Library item 3de35e4f-7964-4f24-a0b4-fd42930a1dd1.
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
  - Code in: `eligible_for_ndpp_health_check_referral_vs1` (3 codes)

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_DM_62_BASE | `patients_on_gestational_dm_vs1` |  |  | SNOMED | 5 | Gestational diabetes mellitus, H/O gestational diabetes mellitus, Gestational... | f9a800d3 |
| ICB_CF_DM_62_woEX | `gestational_dm_and_no_hba1c_in_the_last_year_vs1` |  |  | SNOMED | 3 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 95d9e41a |
| ICB_CF_DM_62 | `gestational_dm_and_no_hba1c_in_the_last_year_vs1` |  |  | SNOMED | 1 | High risk of diabetes mellitus annual review declined | 29adf77b |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs1` | PREG_COD |  | SNOMED | 82 | Pregnant - on abdom. palpation, Pregnancy insufficiently advanced for reliabl... | b82805bb |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs2` |  |  | Internal | 1 | Active Problem | 559aead0 |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs3` |  |  | SNOMED | 1 | Haemoglobin A1c level - International Federation of Clinical Chemistry and La... | 50ab8202 |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs4` |  |  | SNOMED | 3 | Plasma fasting glucose level, Serum fasting glucose level, Fasting blood gluc... | b0cde2cf |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs5` |  |  | SNOMED | 4 | Gestational diabetes mellitus, Gestational diabetes mellitus uncontrolled, Ge... | 2e762dad |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs6` |  |  | SNOMED | 3 | NHS Diabetes Prevention Programme invitation, Referral to NHS Diabetes Preven... | f10d89c3 |
| ICB_CF_DM_NDDP_62_1D | `ndpp_eligible_population_vs7` |  |  | SNOMED | 2 | Unsuitable for NHS Diabetes Prevention Programme, NHS Diabetes Prevention Pro... | eac3168f |
| ICB_CF_DM_NDDP_62_1N | `eligible_for_ndpp_health_check_referral_vs1` |  | 1 | SNOMED | 3 | NHS Diabetes Prevention Programme completed, Referral to NHS Diabetes Prevent... | eb0ff9d2 |

## Caveats

- ICB_CF_DM_62_BASE references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_DM_NDDP_62_1D references the EMIS library item `ea06414e-6bec-4593-837f-5b854c54a8c7`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- ICB_CF_DM_NDDP_62_1D references the EMIS library item `3de35e4f-7964-4f24-a0b4-fd42930a1dd1`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.