<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0l70flp1-dneu-su-07um-09ixd0l0dv00
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On HF Register- LTC LCS Priority Group 2 (HR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On HF Register- LTC LCS Priority Group 2 (HR)*
Parent population: Based on "LTC LCS: HF Register*" search results

## Parent Chain
- LTC LCS: HF Register*: Start with currently registered patients. Finally include patients who match HF Register (library item 79888a16-aa09-4ef4-ba5e-a3be8e1daf23).
  Library refs: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23)

## Library Items
- LTC LCS: HF Register*: HF Register (79888a16-aa09-4ef4-ba5e-a3be8e1daf23); wrapper reports: LTC LCS: HF Register*
- On HF Register- LTC LCS Priority Group 2 (HR)*: Unknown library item (80709f0e-d62c-4851-b8b5-22ce2fb29319)

## Target Report Logic
Start with based on "ltc lcs: hf register*" search results. Require Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 48 and <= 75; Clinical Codes [EVENTS] with Left ventricular ejection fraction then Latest 1 where numeric value < 50 OR Clinical Codes [EVENTS] with Refset: 999007771000230106 OR Refset: 999020531000230105. Finally include patients who do not match Medication Issues [MEDICATION_ISSUES] with Acepril 12.5mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 25mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 50mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd) +349 more then Latest 1 where issue date > today - 6 months OR Clinical Codes [EVENTS] with Refset: 999009011000230109 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 999008011000230100 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Lisinopril adverse reaction, H/O: angiotensin converting enzyme inhibitor pseudoallergy, Acute renal failure due to ACE inhibitor +168 more then Latest 1 where date > today - 1 year OR Clinical Codes [EVENTS] with Refset: 999004331000230101 OR Clinical Codes [EVENTS] with Refset: 999004491000230106 then Latest 1 where date > today - 12 months OR library item 80709f0e-d62c-4851-b8b5-22ce2fb29319.

Boolean logic:
(Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 48 and <= 75) AND (Clinical Codes [EVENTS] with Left ventricular ejection fraction then Latest 1 where numeric value < 50 OR Clinical Codes [EVENTS] with Refset: 999007771000230106 OR Refset: 999020531000230105) AND NOT (Medication Issues [MEDICATION_ISSUES] with Acepril 12.5mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 25mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 50mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd) +349 more then Latest 1 where issue date > today - 6 months OR Clinical Codes [EVENTS] with Refset: 999009011000230109 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 999008011000230100 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Lisinopril adverse reaction, H/O: angiotensin converting enzyme inhibitor pseudoallergy, Acute renal failure due to ACE inhibitor +168 more then Latest 1 where date > today - 1 year OR Clinical Codes [EVENTS] with Refset: 999004331000230101 OR Clinical Codes [EVENTS] with Refset: 999004491000230106 then Latest 1 where date > today - 12 months OR library item 80709f0e-d62c-4851-b8b5-22ce2fb29319)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with New York Heart Association Classification - Class I, New York Heart Association Classification - Class II, New York Heart Association Classification - Class III +1 more OR New York Heart Association Classification - Class III, New York Heart Association Classification - Class IV then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs1`, `on_hf_reg_pg2_hr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs1`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with O/E - oedema not present, O/E - oedema of ankles, O/E - oedema of legs +37 more OR Oedema of thigh, O/E - oedema of thighs, O/E - sacral oedema +13 more then Latest 1 OR Medication Issues [MEDICATION_ISSUES] with Metolazone where Date of Issue within the last 1 year
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs3`, `on_hf_reg_pg2_hr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs3`
  - Restriction: Latest 1
    - Condition: READCODE IN
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_hf_reg_pg2_hr_vs5`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs5`
  - Filter: Date of Issue IN within the last 1 year
    - From: within the last 1 year

### Rule 3 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Clinical Codes [EVENTS] with Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised then Latest 1 where numeric value > 48 and <= 75
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs6`
  - Restriction: Latest 1 where numeric value > 48 and <= 75
    - Condition: NUMERIC_VALUE IN | > 48 and <= 75

### Rule 4 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Clinical Codes [EVENTS] with Left ventricular ejection fraction then Latest 1 where numeric value < 50 OR Clinical Codes [EVENTS] with Refset: 999007771000230106 OR Refset: 999020531000230105
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs7`
  - Restriction: Latest 1 where numeric value < 50
    - Condition: NUMERIC_VALUE IN | < 50
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs8`, `on_hf_reg_pg2_hr_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs8`, `on_hf_reg_pg2_hr_vs9`
  - Filter: Episode (First, New...)
  - Filter: Date
    - To: <=

### Rule 5 (Additional)
- Clause type: informational
- Pass: Next rule
- Fail: Include
- Operator: OR
- Summary: Medication Courses [MEDICATION_COURSES] with Dapagliflozin, Empagliflozin, Canagliflozin +1 more where Date Drug Added within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dapagliflozin, Empagliflozin, Canagliflozin +1 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Adverse reaction to Dapagliflozin, Adverse reaction to Empagliflozin, Adverse reaction to Empagliflozin +4 more
- Medication Courses [MEDICATION_COURSES]
  - ValueSets: `on_hf_reg_pg2_hr_vs10`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs10`
  - Filter: Date Drug Added IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_hf_reg_pg2_hr_vs10`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs10`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs11`

### Rule 6 (Additional)
- Clause type: informational
- Pass: Next rule
- Fail: Include
- Operator: OR
- Summary: Medication Courses [MEDICATION_COURSES] with Spironolactone, Eplerenone, Finerenone where Date Drug Added within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Spironolactone, Eplerenone, Finerenone where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Adverse reaction to Spironolactone, Adverse reaction to Eplerenone, Adverse reaction to Eplerenone +2 more
- Medication Courses [MEDICATION_COURSES]
  - ValueSets: `on_hf_reg_pg2_hr_vs12`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs12`
  - Filter: Date Drug Added IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_hf_reg_pg2_hr_vs12`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs12`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs13`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs13`

### Rule 7 (Additional)
- Clause type: informational
- Pass: Next rule
- Fail: Include
- Operator: OR
- Summary: Medication Issues [MEDICATION_ISSUES] with Bisoprolol 5mg / Aspirin 75mg capsules, Bisoprolol 5mg / Aspirin 100mg capsules, Bisoprolol 10mg / Aspirin 75mg capsules +51 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Hypersensitivity to atenolol, Atenolol hypersensitivity OR Clinical Codes [EVENTS] with Refset: 999008251000230108 where Date on or before 1 year ago OR Clinical Codes [EVENTS] with Refset: 999013291000230105 where Date on or before 1 year ago
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_hf_reg_pg2_hr_vs14`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs14`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Date of Issue
    - To: <=
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs15`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs15`
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs16`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs16`
  - Filter: Date IN on or before 1 year ago
    - To: on or before 1 year ago
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs17`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs17`
  - Filter: Date IN on or before 1 year ago
    - To: on or before 1 year ago

### Rule 8 (Additional)
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: Medication Issues [MEDICATION_ISSUES] with Acepril 12.5mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 25mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 50mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd) +349 more then Latest 1 where issue date > today - 6 months OR Clinical Codes [EVENTS] with Refset: 999009011000230109 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Refset: 999008011000230100 then Latest 1 where date > today - 12 months OR Clinical Codes [EVENTS] with Lisinopril adverse reaction, H/O: angiotensin converting enzyme inhibitor pseudoallergy, Acute renal failure due to ACE inhibitor +168 more then Latest 1 where date > today - 1 year OR Clinical Codes [EVENTS] with Refset: 999004331000230101 OR Clinical Codes [EVENTS] with Refset: 999004491000230106 then Latest 1 where date > today - 12 months OR library item 80709f0e-d62c-4851-b8b5-22ce2fb29319
- Library item: Unknown library item (80709f0e-d62c-4851-b8b5-22ce2fb29319)
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_hf_reg_pg2_hr_vs18`
  - Filter: Drug
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs18`
  - Filter: Date of Issue
    - To: <=
  - Restriction: Latest 1 where issue date > today - 6 months
    - Condition: ISSUE_DATE IN | > today - 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs19`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs19`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs20`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs20`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs21`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs21`
  - Restriction: Latest 1 where date > today - 1 year
    - Condition: DATE IN | > today - 1 year
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs22`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs22`
- Clinical Codes [EVENTS]
  - ValueSets: `on_hf_reg_pg2_hr_vs23`
  - Filter: Clinical Code
    - Filter ValueSets: `on_hf_reg_pg2_hr_vs23`
  - Restriction: Latest 1 where date > today - 12 months
    - Condition: DATE IN | > today - 12 months


## ValueSet Friendly Names
### LTC LCS: HF Register*
- None
### On HF Register- LTC LCS Priority Group 2 (HR)*
- `on_hf_reg_pg2_hr_vs1` (SNOMED, 4 codes): New York Heart Association Classification - Class I, New York Heart Association Classification - Class II, New York Heart Association Classification - Class III +1 more
- `on_hf_reg_pg2_hr_vs2` (SNOMED, 2 codes): New York Heart Association Classification - Class III, New York Heart Association Classification - Class IV
- `on_hf_reg_pg2_hr_vs3` (SNOMED, 40 codes): O/E - oedema not present, O/E - oedema of ankles, O/E - oedema of legs +37 more
- `on_hf_reg_pg2_hr_vs4` (SNOMED, 16 codes): Oedema of thigh, O/E - oedema of thighs, O/E - sacral oedema +13 more
- `on_hf_reg_pg2_hr_vs5` (SCT Const, 1 codes): Metolazone
- `on_hf_reg_pg2_hr_vs6` (SNOMED, 3 codes): Haemoglobin A1c level - IFCC standardised, HbA1c level (diagnostic reference range) - IFCC standardised, HbA1c level (monitoring ranges) - IFCC standardised | Cluster: IFCCHBAM_COD
- `on_hf_reg_pg2_hr_vs7` (SNOMED, 1 codes): Left ventricular ejection fraction
- `on_hf_reg_pg2_hr_vs8` (SNOMED, 1 codes): Refset: 999007771000230106 | Cluster: HFLVSD_COD
- `on_hf_reg_pg2_hr_vs9` (SNOMED, 1 codes): Refset: 999020531000230105 | Cluster: REDEJCFRAC_COD
- `on_hf_reg_pg2_hr_vs10` (SCT Const, 4 codes): Dapagliflozin, Empagliflozin, Canagliflozin +1 more
- `on_hf_reg_pg2_hr_vs11` (SNOMED, 7 codes): Adverse reaction to Dapagliflozin, Adverse reaction to Empagliflozin, Adverse reaction to Empagliflozin +4 more
- `on_hf_reg_pg2_hr_vs12` (SCT Const, 3 codes): Spironolactone, Eplerenone, Finerenone
- `on_hf_reg_pg2_hr_vs13` (SNOMED, 5 codes): Adverse reaction to Spironolactone, Adverse reaction to Eplerenone, Adverse reaction to Eplerenone +2 more
- `on_hf_reg_pg2_hr_vs14` (SNOMED, 54 codes): Bisoprolol 5mg / Aspirin 75mg capsules, Bisoprolol 5mg / Aspirin 100mg capsules, Bisoprolol 10mg / Aspirin 75mg capsules +51 more | Cluster: LBB_COD
- `on_hf_reg_pg2_hr_vs15` (SNOMED, 3 codes): Hypersensitivity to atenolol, Atenolol hypersensitivity | Cluster: XLBB_COD
- `on_hf_reg_pg2_hr_vs16` (SNOMED, 1 codes): Refset: 999008251000230108 | Cluster: TXLBB_COD
- `on_hf_reg_pg2_hr_vs17` (SNOMED, 1 codes): Refset: 999013291000230105 | Cluster: LBBDEC_COD
- `on_hf_reg_pg2_hr_vs18` (SCT_PREP, 352 codes): Acepril 12.5mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 25mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd), Acepril 50mg tablets (Bristol-Myers Squibb Pharmaceuticals Ltd) +349 more
- `on_hf_reg_pg2_hr_vs19` (SNOMED, 1 codes): Refset: 999009011000230109 | Cluster: ACEDEC_COD
- `on_hf_reg_pg2_hr_vs20` (SNOMED, 1 codes): Refset: 999008011000230100 | Cluster: AIIDEC_COD
- `on_hf_reg_pg2_hr_vs21` (SNOMED, 171 codes): Lisinopril adverse reaction, H/O: angiotensin converting enzyme inhibitor pseudoallergy, Acute renal failure due to ACE inhibitor +168 more
- `on_hf_reg_pg2_hr_vs22` (SNOMED, 1 codes): Refset: 999004331000230101 | Cluster: XAII_COD
- `on_hf_reg_pg2_hr_vs23` (SNOMED, 1 codes): Refset: 999004491000230106 | Cluster: TXAII_COD