<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0n0fqrz1-ku4w-y7-1j09-0jh5ui70x0ja
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On AF Register- LTC LCS Priority Group 3 (MR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On AF Register- LTC LCS Priority Group 3 (MR)*
Parent population: Based on "LTC LCS: AF Register*" search results

## Parent Chain
- LTC LCS: AF Register*: Start with currently registered patients. Finally include patients who match AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).
  Library refs: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf)

## Library Items
- LTC LCS: AF Register*: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf); wrapper reports: LTC LCS: AF Register*

## Target Report Logic
Start with based on "ltc lcs: af register*" search results. Require Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months OR Patient Details [PATIENTS] where Age more than 65 years old OR Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value >= 40 and < 60 OR Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value >= 50 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 1 - very fit, Rockwood Clinical Frailty Scale level 2 - well, Rockwood Clinical Frailty Scale level 3 - managing well +3 more OR Rockwood Clinical Frailty Scale level 6 - moderately frail then Latest 1 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 6 and < 7 and date > today - 6 months OR Clinical codes [EVENTS] with Refset: 999011331000230100 then Latest 1 where numeric value >= 1. Exclude patients who match Patients included in search On AF Register- LTC LCS Priority Group 2 (HR)*; Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months. Finally include patients who match Clinical codes [EVENTS] NOT with Refset: 999011331000230100 AND Patient Details [PATIENTS] where Age more than 65 years old.

Boolean logic:
(Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months OR Patient Details [PATIENTS] where Age more than 65 years old OR Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value >= 40 and < 60 OR Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value >= 50 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 1 - very fit, Rockwood Clinical Frailty Scale level 2 - well, Rockwood Clinical Frailty Scale level 3 - managing well +3 more OR Rockwood Clinical Frailty Scale level 6 - moderately frail then Latest 1 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 6 and < 7 and date > today - 6 months OR Clinical codes [EVENTS] with Refset: 999011331000230100 then Latest 1 where numeric value >= 1) AND NOT (patients included in search On AF Register- LTC LCS Priority Group 2 (HR)*) AND NOT (Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months) AND (Clinical codes [EVENTS] NOT with Refset: 999011331000230100 AND Patient Details [PATIENTS] where Age more than 65 years old)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On AF Register- LTC LCS Priority Group 2 (HR)*
- Population ref: On AF Register- LTC LCS Priority Group 2 (HR)* (d4e73a7f-7e8c-4a19-9673-18abd33cfc15)

### Rule 2 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months OR Patient Details [PATIENTS] where Age more than 65 years old OR Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value >= 40 and < 60 OR Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value >= 50 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 1 - very fit, Rockwood Clinical Frailty Scale level 2 - well, Rockwood Clinical Frailty Scale level 3 - managing well +3 more OR Rockwood Clinical Frailty Scale level 6 - moderately frail then Latest 1 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 6 and < 7 and date > today - 6 months OR Clinical codes [EVENTS] with Refset: 999011331000230100 then Latest 1 where numeric value >= 1
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs1`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs1`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs2`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs4`
  - Filter: Date IN within the last 6 months
    - From: within the last 6 months
- Patient Details [PATIENTS]
  - Filter: Age IN more than 65 years old
    - From: more than 65 years old
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs5`
  - Restriction: Latest 1 where numeric value >= 40 and < 60
    - Condition: NUMERIC_VALUE IN | >= 40 and < 60
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs6`
  - Restriction: Latest 1 where numeric value >= 50
    - Condition: NUMERIC_VALUE IN | >= 50
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs7`, `on_af_reg_pg3_mr_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs7`
  - Restriction: Latest 1
    - Condition: READCODE IN
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs9`
  - Restriction: Latest 1 where numeric value >= 6 and < 7 and date > today - 6 months
    - Condition: NUMERIC_VALUE IN | >= 6 and < 7
    - Condition: DATE IN | > today - 6 months
- Clinical codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where numeric value >= 1
    - Condition: NUMERIC_VALUE IN | >= 1

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Serum creatinine level then Latest 1 where date < today - 15 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs1`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs1`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs11`
  - Restriction: Latest 1 where date < today - 15 months
    - Condition: DATE IN | < today - 15 months

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Serum creatinine level then Latest 1 where date < today - 15 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs11`
  - Restriction: Latest 1 where date < today - 15 months
    - Condition: DATE IN | < today - 15 months

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value >= 40 and < 60
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs5`
  - Restriction: Latest 1 where numeric value >= 40 and < 60
    - Condition: NUMERIC_VALUE IN | >= 40 and < 60

### Rule 6 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value > 50 and <= 60
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs6`
  - Restriction: Latest 1 where numeric value > 50 and <= 60
    - Condition: NUMERIC_VALUE IN | > 50 and <= 60

### Rule 7 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months AND Patient Details [PATIENTS] where Age at least 75 years old
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Patient Details [PATIENTS]
  - Filter: Age IN at least 75 years old
    - From: at least 75 years old

### Rule 8 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 1 - very fit, Rockwood Clinical Frailty Scale level 2 - well, Rockwood Clinical Frailty Scale level 3 - managing well +3 more OR Rockwood Clinical Frailty Scale level 6 - moderately frail then Latest 1
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs7`, `on_af_reg_pg3_mr_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs7`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Rule 9 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months AND Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 6 and < 7 and date > today - 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs9`
  - Restriction: Latest 1 where numeric value >= 6 and < 7 and date > today - 6 months
    - Condition: NUMERIC_VALUE IN | >= 6 and < 7
    - Condition: DATE IN | > today - 6 months

### Rule 10 (Additional)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs1`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs1`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs2`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg3_mr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs4`
  - Filter: Date IN within the last 6 months
    - From: within the last 6 months

### Rule 11 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical codes [EVENTS] with Refset: 999011331000230100 then Latest 1 where numeric value >= 1 AND Patient Details [PATIENTS] with Male where Gender = Male
- Clinical codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where numeric value >= 1
    - Condition: NUMERIC_VALUE IN | >= 1
- Patient Details [PATIENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs12`
  - Filter: Gender
    - Filter ValueSets: `on_af_reg_pg3_mr_vs12`

### Rule 12 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical codes [EVENTS] with Refset: 999011331000230100 then Latest 1 where numeric value >= 2 AND Patient Details [PATIENTS] with Female where Gender = Female
- Clinical codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where numeric value >= 2
    - Condition: NUMERIC_VALUE IN | >= 2
- Patient Details [PATIENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs13`
  - Filter: Gender
    - Filter ValueSets: `on_af_reg_pg3_mr_vs13`

### Rule 13 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical codes [EVENTS] with Refset: 999011331000230100 then Latest 1 where date < today - 2 years AND Patient Details [PATIENTS] where Age more than 65 years old
- Clinical codes [EVENTS]
  - ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1 where date < today - 2 years
    - Condition: DATE IN | < today - 2 years
- Patient Details [PATIENTS]
  - Filter: Age IN more than 65 years old
    - From: more than 65 years old

### Rule 14 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical codes [EVENTS] NOT with Refset: 999011331000230100 AND Patient Details [PATIENTS] where Age more than 65 years old
- Clinical codes [EVENTS] (NOT)
  - ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg3_mr_vs10`
  - Filter: Date
    - To: <=
- Patient Details [PATIENTS]
  - Filter: Age IN more than 65 years old
    - From: more than 65 years old


## ValueSet Friendly Names
### LTC LCS: AF Register*
- None
### On AF Register- LTC LCS Priority Group 3 (MR)*
- `on_af_reg_pg3_mr_vs1` (Drug Group, 1 codes): Oral Anticoagulants | Cluster: Warfarin
- `on_af_reg_pg3_mr_vs2` (SCT Const, 6 codes): Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more
- `on_af_reg_pg3_mr_vs3` (SNOMED, 40 codes): Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more | Cluster: DIRECTORANTICOAGDRUG_COD
- `on_af_reg_pg3_mr_vs4` (SNOMED, 1 codes): Anticoagulant prescribed by third party
- `on_af_reg_pg3_mr_vs5` (SNOMED, 8 codes): Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more
- `on_af_reg_pg3_mr_vs6` (SNOMED, 3 codes): Body weight, O/E - weight, O/E - weight
- `on_af_reg_pg3_mr_vs7` (SNOMED, 6 codes): Rockwood Clinical Frailty Scale level 1 - very fit, Rockwood Clinical Frailty Scale level 2 - well, Rockwood Clinical Frailty Scale level 3 - managing well +3 more
- `on_af_reg_pg3_mr_vs8` (SNOMED, 1 codes): Rockwood Clinical Frailty Scale level 6 - moderately frail
- `on_af_reg_pg3_mr_vs9` (SNOMED, 3 codes): Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale
- `on_af_reg_pg3_mr_vs10` (SNOMED, 1 codes): Refset: 999011331000230100 | Cluster: CHADVASC_COD
- `on_af_reg_pg3_mr_vs11` (SNOMED, 1 codes): Serum creatinine level
- `on_af_reg_pg3_mr_vs12` (Internal, 1 codes): Male
- `on_af_reg_pg3_mr_vs13` (Internal, 1 codes): Female