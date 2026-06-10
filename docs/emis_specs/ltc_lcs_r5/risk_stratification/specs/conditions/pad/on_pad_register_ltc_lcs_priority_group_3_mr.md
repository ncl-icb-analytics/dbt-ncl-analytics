<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0d69ytz1-sp6o-cg-0vdy-13qqhpt1nm84
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On PAD Register- LTC LCS Priority Group 3 (MR)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: PAD Register*" (see below). Patients must match Rule 2 to stay in. Patients matching Rules 1 and 4 are excluded. A patient is included when they match Rule 3. A patient is included when they do NOT match Rule 5. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Start population

1. Currently registered patients
2. **LTC LCS: PAD Register*** — Include patients who match PAD Register (library item ffccdb77-bd5e-47fc-add3-d700835ace65).
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | Continue to Rule 2 | Exclusion |
| 2 | Continue to Rule 3 | Excluded | Filter — must match |
| 3 | **Included** | Continue to Rule 4 | Inclusion route |
| 4 | Excluded | Continue to Rule 5 | Exclusion |
| 5 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 5 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **On PAD Register- LTC LCS Priority Group 1 (HRC)**
- They appear in the results of the search **On PAD Register- LTC LCS Priority Group 2 (HR)**

### Rule 2 of 5 — Filter — must match

Patients **must match** this rule to stay in. Those who match continue to Rule 3; those who do not are excluded.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  *"PAD Register"*
  - Code in: `on_pad_reg_pg3_mr_vs1` (53 codes)
  - Where date before 12 months ago — `date < today - 12 months`

### Rule 3 of 5 — Inclusion route

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:

- **Criterion A — Clinical Codes** (clinical events)
  - Code in: `on_pad_reg_pg3_mr_vs2` (4 codes)
  - Keep only the latest matching record, and require its numeric value > 2.5

### Rule 4 of 5 — Exclusion

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 5.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Medication Issues**
  - Code in: `on_pad_reg_pg3_mr_vs3` (6 codes)
  - Where issue date within the last 6 months — `issue date >= today - 6 months`
- **Criterion B — Medication Courses**
  - Code in: `on_pad_reg_pg3_mr_vs6` (4 codes)
  - Where course status is Current
  - Where prescription type is Automatic or Repeat or Repeat Dispensed

### Rule 5 of 5 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when **ANY (OR)** of the following are true:

- **Criterion A — Medication Issues**
  *"Medication Issues of Statins in the 6 months"*
  - Code in: `on_pad_reg_pg3_mr_vs7` (1 code — cluster STAT_COD)
  - Keep only the latest matching record, and require its issue date > today - 12 months
- **Criterion B — Clinical Codes** (clinical events)
  *"Statins in the 6 months"*
  - Code in: `on_pad_reg_pg3_mr_vs7` (1 code — cluster STAT_COD)
  - Keep only the latest matching record, and require its date > today - 12 months
- **Criterion C — Clinical Codes** (clinical events)
  *"Statin Prescription received in 12 months"*
  - Code in: `on_pad_reg_pg3_mr_vs8` (1 code — cluster STATINDEC_COD)
  - Keep only the latest matching record, and require its date > today - 1 year

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | Used in rules | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs1` |  | 2 | SNOMED | 53 | Peripheral ischaemia, Peripheral ischaemic vascular disease, Ischaemic foot +... | 36d83560 |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs2` |  | 3 | SNOMED | 4 | Non high density lipoprotein cholesterol level, Non HDL cholesterol level, Se... | 561cf433 |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs3` |  | 4 | SCT_PREP | 6 | Atorvastatin 80mg tablets, Lipitor 80mg tablets (Viatris UK Healthcare Ltd), ... | dd8f85b7 |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs4` |  | 4 | Internal | 1 | Current | 6b23c0d5 |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs5` |  | 4 | Internal | 3 | Automatic, Repeat, Repeat Dispensed | 23417f0f |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs6` |  | 4 | SCT Const | 4 | Atorvastatin, Simvastatin, Fluvastatin +1 more | 61715e8d |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs7` | STAT_COD | 5 | SNOMED | 1 | Refset: 12464001000001103 | 2ab5ff0c |
| On PAD Register- LTC LCS Priority Group 3 (MR) | `on_pad_reg_pg3_mr_vs8` | STATINDEC_COD | 5 | SNOMED | 1 | Statin declined | ac08be53 |

## Caveats

- LTC LCS: PAD Register* references the EMIS library item `ffccdb77-bd5e-47fc-add3-d700835ace65`, whose logic is not included in this XML export. It is likely **PAD Register** (inferred from wrapper report "LTC LCS: PAD Register*"), but this is not certain. Verify it in EMIS before implementing.
- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.