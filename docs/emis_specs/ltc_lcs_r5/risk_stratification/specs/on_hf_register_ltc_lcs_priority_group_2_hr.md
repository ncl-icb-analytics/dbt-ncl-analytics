<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0l70flp1-dneu-su-07um-09ixd0l0dv00
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On HF Register- LTC LCS Priority Group 2 (HR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: HF Register*" (see below). Patients must match Rules 3-4 to stay in. A patient is included when they match any one of Rules 1-2. Rule 8 includes only patients who do NOT match it.

## Who we start with

1. **LTC LCS: HF Register*** — Start with currently registered patients. Include patients who match HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 2.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs1` (4 codes), or `on_hf_reg_pg2_hr_vs2` (2 codes)
  - Keep only the latest matching record

### Rule 2 of 8

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs3` (40 codes), or `on_hf_reg_pg2_hr_vs4` (16 codes)
  - Keep only the latest matching record
- **Medication Issues**
  - Code in: `on_hf_reg_pg2_hr_vs5` (1 code)
  - Where drug code in `on_hf_reg_pg2_hr_vs5` (1 code)
  - Where issue date within the last 1 year

### Rule 3 of 8

Patients **must match** this rule to stay in. Those who match continue to Rule 4; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs6` (3 codes — cluster IFCCHBAM_COD)
  - Keep only the latest matching record, and require its numeric value > 48 and <= 75

### Rule 4 of 8

Patients **must match** this rule to stay in. Those who match continue to Rule 5; those who do not are excluded.

A patient matches this rule when ANY of the following is true:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs7` (1 code)
  - Keep only the latest matching record, and require its numeric value < 50
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs8` (1 code — cluster HFLVSD_COD), or `on_hf_reg_pg2_hr_vs9` (1 code — cluster REDEJCFRAC_COD)

### Rule 5 of 8

This rule does not change who is included.

A patient matches this rule when ANY of the following is true:
- **Medication Courses**
  - Code in: `on_hf_reg_pg2_hr_vs10` (4 codes)
  - Where drug code in `on_hf_reg_pg2_hr_vs10` (4 codes)
  - Where commence date within the last 6 months
- **Medication Issues**
  - Code in: `on_hf_reg_pg2_hr_vs10` (4 codes)
  - Where drug code in `on_hf_reg_pg2_hr_vs10` (4 codes)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs11` (7 codes)

### Rule 6 of 8

This rule does not change who is included.

A patient matches this rule when ANY of the following is true:
- **Medication Courses**
  - Code in: `on_hf_reg_pg2_hr_vs12` (3 codes)
  - Where drug code in `on_hf_reg_pg2_hr_vs12` (3 codes)
  - Where commence date within the last 6 months
- **Medication Issues**
  - Code in: `on_hf_reg_pg2_hr_vs12` (3 codes)
  - Where drug code in `on_hf_reg_pg2_hr_vs12` (3 codes)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs13` (5 codes)

### Rule 7 of 8

This rule does not change who is included.

A patient matches this rule when ANY of the following is true:
- **Medication Issues**
  - Code in: `on_hf_reg_pg2_hr_vs14` (54 codes — cluster LBB_COD)
  - Where drug code in `on_hf_reg_pg2_hr_vs14` (54 codes — cluster LBB_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs15` (3 codes — cluster XLBB_COD)
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs16` (1 code — cluster TXLBB_COD)
  - Where date on or before 1 year ago
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs17` (1 code — cluster LBBDEC_COD)
  - Where date on or before 1 year ago

### Rule 8 of 8

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ANY of the following is true:
- They match the EMIS library item `80709f0e-d62c-4851-b8b5-22ce2fb29319` (see Caveats)
- **Medication Issues**
  - Code in: `on_hf_reg_pg2_hr_vs18` (352 codes)
  - Where drug code in `on_hf_reg_pg2_hr_vs18` (352 codes)
  - Keep only the latest matching record, and require its issue date > today - 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs19` (1 code — cluster ACEDEC_COD)
  - Keep only the latest matching record, and require its date > today - 12 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs20` (1 code — cluster AIIDEC_COD)
  - Keep only the latest matching record, and require its date > today - 12 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs21` (171 codes)
  - Keep only the latest matching record, and require its date > today - 1 year
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs22` (1 code — cluster XAII_COD)
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg2_hr_vs23` (1 code — cluster TXAII_COD)
  - Keep only the latest matching record, and require its date > today - 12 months

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs1` |  | SNOMED | 4 | New York Heart Association Classification - Class I, New York Heart Associati... | 4ec0ed9b |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs10` |  | SCT Const | 4 | Dapagliflozin, Empagliflozin, Canagliflozin +1 more | 4ca4d3a3 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs11` |  | SNOMED | 7 | Adverse reaction to Dapagliflozin, Adverse reaction to Empagliflozin, Adverse... | a5be9bf6 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs12` |  | SCT Const | 3 | Spironolactone, Eplerenone, Finerenone | a4b2da82 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs13` |  | SNOMED | 5 | Adverse reaction to Spironolactone, Adverse reaction to Eplerenone, Adverse r... | 3338e94b |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs14` | LBB_COD | SNOMED | 54 | Bisoprolol 5mg / Aspirin 75mg capsules, Bisoprolol 5mg / Aspirin 100mg capsul... | 45878485 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs15` | XLBB_COD | SNOMED | 3 | Hypersensitivity to atenolol, Atenolol hypersensitivity | af676e6c |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs16` | TXLBB_COD | SNOMED | 1 | Refset: 999008251000230108 | 46d0d2c0 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs17` | LBBDEC_COD | SNOMED | 1 | Refset: 999013291000230105 | f6f51ae7 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs18` |  | SCT_PREP | 352 | Acepril 12.5mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 25... | 5c072804 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs19` | ACEDEC_COD | SNOMED | 1 | Refset: 999009011000230109 | 8a36ae84 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs2` |  | SNOMED | 2 | New York Heart Association Classification - Class III, New York Heart Associa... | a191256b |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs20` | AIIDEC_COD | SNOMED | 1 | Refset: 999008011000230100 | f2d0e917 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs21` |  | SNOMED | 171 | Lisinopril adverse reaction, H/O: angiotensin converting enzyme inhibitor pse... | 277499b3 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs22` | XAII_COD | SNOMED | 1 | Refset: 999004331000230101 | d4aea85d |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs23` | TXAII_COD | SNOMED | 1 | Refset: 999004491000230106 | 24284ef4 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs3` |  | SNOMED | 40 | O/E - oedema not present, O/E - oedema of ankles, O/E - oedema of legs +37 more | 6cd34574 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs4` |  | SNOMED | 16 | Oedema of thigh, O/E - oedema of thighs, O/E - sacral oedema +13 more | aae2d273 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs5` |  | SCT Const | 1 | Metolazone | 86043711 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs6` | IFCCHBAM_COD | SNOMED | 3 | Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference ... | 95d9e41a |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs7` |  | SNOMED | 1 | Left ventricular ejection fraction | 4c79b106 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs8` | HFLVSD_COD | SNOMED | 1 | Refset: 999007771000230106 | 2c54e779 |
| On HF Register- LTC LCS Priority Group 2 (HR)* | `on_hf_reg_pg2_hr_vs9` | REDEJCFRAC_COD | SNOMED | 1 | Refset: 999020531000230105 | d9fd1aa5 |

## Caveats

- LTC LCS: HF Register* references the EMIS library item `79888a16-aa09-4ef4-ba5e-a3be8e1daf23`, whose logic is not included in this XML export. It is likely **HF Register** (inferred from wrapper report "LTC LCS: HF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `80709f0e-d62c-4851-b8b5-22ce2fb29319`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.