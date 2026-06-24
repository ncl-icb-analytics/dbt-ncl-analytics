# ICB_CF_AF_62

Report title: [ICB_CF_AF_62] AF Case Finding Eligible Population- Over 65's missing pulse
Folder: 1) Casefinding R2 > [CF-AF] AF Case finding
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "ICB_CF_AF_62_woEX" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **ICB_CF_AF_62_BASE** — Require Patient Details where Age at least 65 years old. Exclude patients who match Patients included in search ICB_CF_AF_61_base_woEX. Include patients who do not match Patients included in search ICS_METABOLIC_LTC OR patients included in search CF_NHSHC2Y OR AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).
   - Combines: **ICB_CF_AF_61_base_woEX**; **ICS_METABOLIC_LTC**; **CF_NHSHC2Y**
3. **ICB_CF_AF_62_woEX** — Include patients who do not match Clinical Codes with O/E - pulse rhythm, O/E - pulse rate, Pulse rate +7 more where Date within the last 3 years OR Clinical Codes with O/E - pulse rhythm, O/E - pulse rate where Date within the last 3 years OR Clinical Codes with O/E - pulse rhythm regular, O/E -pulse regularly irregular, O/E - irregular pulse +5 more where Date within the last 3 years OR Clinical Codes with O/E - pulse rate - bradycardia, O/E - pulse rate tachycardia, O/E - pulse +25 more where Date within the last 3 years.
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
  - Code in: `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` (2 codes)
  - Where date within the last 3 years — `date > today - 3 years`

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICB_CF_AF_62_woEX | `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` |  |  | SNOMED | 10 | O/E - pulse rhythm, O/E - pulse rate, Pulse rate +7 more | d70a795a |
| ICB_CF_AF_62_woEX | `af_case_finding_eligible_population_over_65s_missing_pulse_vs2` |  |  | SNOMED | 2 | O/E - pulse rhythm, O/E - pulse rate | d8324fa3 |
| ICB_CF_AF_62_woEX | `af_case_finding_eligible_population_over_65s_missing_pulse_vs3` |  |  | SNOMED | 8 | O/E - pulse rhythm regular, O/E -pulse regularly irregular, O/E - irregular p... | ab4a354c |
| ICB_CF_AF_62_woEX | `af_case_finding_eligible_population_over_65s_missing_pulse_vs4` |  |  | SNOMED | 28 | O/E - pulse rate - bradycardia, O/E - pulse rate tachycardia, O/E - pulse +25... | 109e359b |
| ICB_CF_AF_62 | `af_case_finding_eligible_population_over_65s_missing_pulse_vs1` |  | 1 | SNOMED | 2 | Atrial fibrillation excluded, Atrial fibrillation confirmed | ea9dcb07 |

## Caveats

- ICB_CF_AF_62_BASE references the EMIS library item `e6742de9-2073-4a23-8c94-e05f668eaabf`, whose logic is not included in this XML export. It is likely **AF Register** (inferred from wrapper report "LTC LCS: AF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.