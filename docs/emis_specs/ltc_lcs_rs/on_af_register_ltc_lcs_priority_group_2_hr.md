<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0ya2yux0-jt2t-pc-06f3-0ybvpy61sush
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On AF Register- LTC LCS Priority Group 2 (HR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On AF Register- LTC LCS Priority Group 2 (HR)*
Parent population: Based on "LTC LCS: AF Register*" search results

## Parent Chain
- LTC LCS: AF Register*: Start with currently registered patients. Finally include patients who match AF Register (library item e6742de9-2073-4a23-8c94-e05f668eaabf).
  Library refs: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf)

## Library Items
- LTC LCS: AF Register*: AF Register (e6742de9-2073-4a23-8c94-e05f668eaabf); wrapper reports: LTC LCS: AF Register*

## Target Report Logic
Start with based on "ltc lcs: af register*" search results. Require Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months; Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months. Finally include patients who match Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value < 40 OR Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value < 50 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 7 - severely frail, Rockwood Clinical Frailty Scale level 8 - very severely frail, Rockwood Clinical Frailty Scale level 9 - terminally ill OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 7.

Boolean logic:
(Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months) AND (Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months) AND (Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value < 40 OR Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value < 50 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 7 - severely frail, Rockwood Clinical Frailty Scale level 8 - very severely frail, Rockwood Clinical Frailty Scale level 9 - terminally ill OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 7)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Medication Issues [MEDICATION_ISSUES] with Oral Anticoagulants where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Anticoagulant prescribed by third party where Date within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg2_hr_vs1`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg2_hr_vs1`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg2_hr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg2_hr_vs2`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg2_hr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg2_hr_vs3`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs4`
  - Filter: Date IN within the last 6 months
    - From: within the last 6 months

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Medication Issues [MEDICATION_ISSUES] with Aspirin, Clopidogrel, Clopidogrel Hydrogen Sulfate +4 more where Date of Issue within the last 6 months OR Clinical Codes [EVENTS] with Aspirin prophylaxis - IHD, Aspirin prophylaxis for ischaemic heart disease, Aspirin prophylaxis for ischemic heart disease +9 more where Date within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg2_hr_vs5`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg2_hr_vs5`
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs6`
  - Filter: Date IN within the last 6 months
    - From: within the last 6 months

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleeding history or predisposition, labile INR (international normalised ratio), elderly over 65, and drugs and/or alcohol concomitantly) score, HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleeding history or predisposition, labile international normalised ratio, elderly over 65, and drugs and/or alcohol concomitantly) bleeding risk score, Assessment using HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleeding history or predisposition, labile INR (international normalised ratio), elderly over 65, and drugs and/or alcohol concomitantly) score then Latest 1 where numeric value >= 3 OR Clinical Codes [EVENTS] with ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillation) bleeding risk score, ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillation) bleeding risk score, Assessment using ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillation) bleeding risk score then Latest 1 where numeric value >= 4
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs7`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs7`
  - Restriction: Latest 1 where numeric value >= 3
    - Condition: NUMERIC_VALUE IN | >= 3
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs8`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs8`
  - Restriction: Latest 1 where numeric value >= 4
    - Condition: NUMERIC_VALUE IN | >= 4

### Rule 4 (Additional)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: OR
- Summary: Must match: Medication Issues [MEDICATION_ISSUES] with Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more where Date of Issue within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_af_reg_pg2_hr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_af_reg_pg2_hr_vs2`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months

### Rule 5 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more then Latest 1 where numeric value < 40 OR Clinical Codes [EVENTS] with Body weight, O/E - weight, O/E - weight then Latest 1 where numeric value < 50 OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale level 7 - severely frail, Rockwood Clinical Frailty Scale level 8 - very severely frail, Rockwood Clinical Frailty Scale level 9 - terminally ill OR Clinical Codes [EVENTS] with Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale then Latest 1 where numeric value >= 7
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs9`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs9`
  - Restriction: Latest 1 where numeric value < 40
    - Condition: NUMERIC_VALUE IN | < 40
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs10`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs10`
  - Restriction: Latest 1 where numeric value < 50
    - Condition: NUMERIC_VALUE IN | < 50
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs11`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs11`
- Clinical Codes [EVENTS]
  - ValueSets: `on_af_reg_pg2_hr_vs12`
  - Filter: Clinical Code
    - Filter ValueSets: `on_af_reg_pg2_hr_vs12`
  - Restriction: Latest 1 where numeric value >= 7
    - Condition: NUMERIC_VALUE IN | >= 7


## ValueSet Friendly Names
### LTC LCS: AF Register*
- None
### On AF Register- LTC LCS Priority Group 2 (HR)*
- `on_af_reg_pg2_hr_vs1` (Drug Group, 1 codes): Oral Anticoagulants | Cluster: Warfarin
- `on_af_reg_pg2_hr_vs2` (SCT Const, 6 codes): Dabigatran Etexilate, Rivaroxaban, Apixaban +3 more
- `on_af_reg_pg2_hr_vs3` (SNOMED, 40 codes): Apixaban 2.5mg tablets, Apixaban 5mg tablets, Dabigatran etexilate 75mg capsules +37 more | Cluster: DIRECTORANTICOAGDRUG_COD
- `on_af_reg_pg2_hr_vs4` (SNOMED, 1 codes): Anticoagulant prescribed by third party
- `on_af_reg_pg2_hr_vs5` (SCT Const, 7 codes): Aspirin, Clopidogrel, Clopidogrel Hydrogen Sulfate +4 more
- `on_af_reg_pg2_hr_vs6` (SNOMED, 12 codes): Aspirin prophylaxis - IHD, Aspirin prophylaxis for ischaemic heart disease, Aspirin prophylaxis for ischemic heart disease +9 more
- `on_af_reg_pg2_hr_vs7` (SNOMED, 3 codes): HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleeding history or predisposition, labile INR (international normalised ratio), elderly over 65, and drugs and/or alcohol concomitantly) score, HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleeding history or predisposition, labile international normalised ratio, elderly over 65, and drugs and/or alcohol concomitantly) bleeding risk score, Assessment using HAS-BLED (hypertension, abnormal renal and/or liver function, stroke, bleeding history or predisposition, labile INR (international normalised ratio), elderly over 65, and drugs and/or alcohol concomitantly) score
- `on_af_reg_pg2_hr_vs8` (SNOMED, 3 codes): ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillation) bleeding risk score, ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillation) bleeding risk score, Assessment using ORBIT-AF (Outcomes Registry for Better Informed Treatment of Atrial Fibrillation) bleeding risk score
- `on_af_reg_pg2_hr_vs9` (SNOMED, 8 codes): Cockcroft-Gault formula, Predicted by Cockcroft-Gault formula, Estimated creatinine clearance (Cockcroft-Gault formula) +5 more
- `on_af_reg_pg2_hr_vs10` (SNOMED, 3 codes): Body weight, O/E - weight, O/E - weight
- `on_af_reg_pg2_hr_vs11` (SNOMED, 3 codes): Rockwood Clinical Frailty Scale level 7 - severely frail, Rockwood Clinical Frailty Scale level 8 - very severely frail, Rockwood Clinical Frailty Scale level 9 - terminally ill
- `on_af_reg_pg2_hr_vs12` (SNOMED, 3 codes): Rockwood Clinical Frailty Scale score, Rockwood Clinical Frailty Scale, Assessment using Rockwood Clinical Frailty Scale