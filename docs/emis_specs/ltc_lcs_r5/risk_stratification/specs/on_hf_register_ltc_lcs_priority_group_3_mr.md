<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1ubhgmf0-j4fe-mi-1xl6-0vmx8nm0yrcb
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On HF Register- LTC LCS Priority Group 3 (MR)*

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS: HF Register*" (see below). Patients must match Rule 5 to stay in. Patients matching Rule 1 are excluded. A patient is included when they match any one of Rules 2-4. A patient is included unless they match every one of Rules 6-9 — failing any one of them includes the patient. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

1. **LTC LCS: HF Register*** — Start with currently registered patients. Include patients who match HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23).
2. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 9

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 2.

A patient matches this rule when:
- They appear in the results of the search **On HF Register- LTC LCS Priority Group 2 (HR)***

### Rule 2 of 9

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 3.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs1` (1 code — cluster HF_COD)
  - Where episode type is not Review or Ended
  - Keep only the earliest matching record, and require its date > today - 6 months

### Rule 3 of 9

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 4.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs2` (4 codes), or `on_hf_reg_pg3_mr_vs3` (1 code)
  - Keep only the latest matching record

### Rule 4 of 9

If a patient matches this rule they are **included** and no further rules are checked. If not, continue to Rule 5.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs4` (11 codes), or `on_hf_reg_pg3_mr_vs5` (7 codes)
  - Keep only the latest matching record

### Rule 5 of 9

Patients **must match** this rule to stay in. Those who match continue to Rule 6; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs6` (1 code)
  - Keep only the latest matching record, and require its numeric value < 50

### Rule 6 of 9

If a patient does **not** match this rule they are **included** and no further rules are checked. If they do match, continue to Rule 7.

A patient matches this rule when ANY of the following is true:
- **Medication Courses**
  - Code in: `on_hf_reg_pg3_mr_vs7` (4 codes)
  - Where date drug added within the last 6 months
- **Medication Issues**
  - Code in: `on_hf_reg_pg3_mr_vs7` (4 codes)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs8` (7 codes)

### Rule 7 of 9

If a patient does **not** match this rule they are **included** and no further rules are checked. If they do match, continue to Rule 8.

A patient matches this rule when ANY of the following is true:
- **Medication Courses**
  - Code in: `on_hf_reg_pg3_mr_vs9` (3 codes)
  - Where date drug added within the last 6 months
- **Medication Issues**
  - Code in: `on_hf_reg_pg3_mr_vs9` (3 codes)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs10` (5 codes)

### Rule 8 of 9

If a patient does **not** match this rule they are **included** and no further rules are checked. If they do match, continue to Rule 9.

A patient matches this rule when ANY of the following is true:
- **Medication Issues**
  - Code in: `on_hf_reg_pg3_mr_vs11` (54 codes — cluster LBB_COD)
  - Where issue date within the last 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs12` (3 codes — cluster XLBB_COD)
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs13` (1 code — cluster TXLBB_COD)
  - Where date on or before 1 year ago
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs14` (1 code — cluster LBBDEC_COD)
  - Where date on or before 1 year ago

### Rule 9 of 9

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ANY of the following is true:
- They match the EMIS library item `80709f0e-d62c-4851-b8b5-22ce2fb29319` (see Caveats)
- **Medication Issues**
  - Code in: `on_hf_reg_pg3_mr_vs15` (352 codes)
  - Keep only the latest matching record, and require its issue date > today - 6 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs16` (1 code — cluster ACEDEC_COD)
  - Keep only the latest matching record, and require its date > today - 12 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs17` (1 code — cluster AIIDEC_COD)
  - Keep only the latest matching record, and require its date > today - 12 months
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs18` (171 codes)
  - Keep only the latest matching record, and require its date > today - 1 year
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs19` (1 code — cluster XAII_COD)
- **Clinical Codes** (clinical events)
  - Code in: `on_hf_reg_pg3_mr_vs20` (1 code — cluster TXAII_COD)
  - Keep only the latest matching record, and require its date > today - 12 months

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs1` | HF_COD | SNOMED | 1 | Refset: 999013691000230108 | 94f38705 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs10` |  | SNOMED | 5 | Adverse reaction to Spironolactone, Adverse reaction to Eplerenone, Adverse r... | 3338e94b |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs11` | LBB_COD | SNOMED | 54 | Bisoprolol 5mg / Aspirin 75mg capsules, Bisoprolol 5mg / Aspirin 100mg capsul... | 45878485 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs12` | XLBB_COD | SNOMED | 3 | Hypersensitivity to atenolol, Atenolol hypersensitivity | af676e6c |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs13` | TXLBB_COD | SNOMED | 1 | Refset: 999008251000230108 | 46d0d2c0 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs14` | LBBDEC_COD | SNOMED | 1 | Refset: 999013291000230105 | f6f51ae7 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs15` |  | SCT_PREP | 352 | Acepril 12.5mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 25... | 5c072804 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs16` | ACEDEC_COD | SNOMED | 1 | Refset: 999009011000230109 | 8a36ae84 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs17` | AIIDEC_COD | SNOMED | 1 | Refset: 999008011000230100 | f2d0e917 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs18` |  | SNOMED | 171 | Lisinopril adverse reaction, H/O: angiotensin converting enzyme inhibitor pse... | 277499b3 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs19` | XAII_COD | SNOMED | 1 | Refset: 999004331000230101 | d4aea85d |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs2` |  | SNOMED | 4 | New York Heart Association Classification - Class I, New York Heart Associati... | 4ec0ed9b |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs20` | TXAII_COD | SNOMED | 1 | Refset: 999004491000230106 | 24284ef4 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs3` |  | SNOMED | 1 | New York Heart Association Classification - Class II | 7cb628e4 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs4` |  | SNOMED | 11 | O/E - oedema not present, O/E - oedema of ankles, O/E - oedema of feet +8 more | e9b19d9f |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs5` |  | SNOMED | 7 | O/E - oedema of feet, Bilateral feet oedema, O/E - oedema of ankles +4 more | 27256413 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs6` |  | SNOMED | 1 | Left ventricular ejection fraction | 4c79b106 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs7` |  | SCT Const | 4 | Dapagliflozin, Empagliflozin, Canagliflozin +1 more | 4ca4d3a3 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs8` |  | SNOMED | 7 | Adverse reaction to Dapagliflozin, Adverse reaction to Empagliflozin, Adverse... | a5be9bf6 |
| On HF Register- LTC LCS Priority Group 3 (MR)* | `on_hf_reg_pg3_mr_vs9` |  | SCT Const | 3 | Spironolactone, Eplerenone, Finerenone | a4b2da82 |

## Caveats

- LTC LCS: HF Register* references the EMIS library item `79888a16-aa09-4ef4-ba5e-a3be8e1daf23`, whose logic is not included in this XML export. It is likely **HF Register** (inferred from wrapper report "LTC LCS: HF Register*"), but this is not certain. Verify it in EMIS before implementing.
- This search references the EMIS library item `80709f0e-d62c-4851-b8b5-22ce2fb29319`, whose logic is not included in this XML export. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.