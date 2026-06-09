<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 058ijs21-6wve-bw-020g-0q44mtw0wq6x
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On COPD Register- LTC LCS Priority Group 3 (MR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On COPD Register- LTC LCS Priority Group 3 (MR)
Parent population: Based on "LTC LCS: COPD Register*" search results

## Parent Chain
- LTC LCS: COPD Register*: Start with currently registered patients. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD.
  Library refs: ee5b135f-b9b2-4ef7-8b51-939a754cf935

## Library Items
- LTC LCS: COPD Register*: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Target Report Logic
Start with based on "ltc lcs: copd register*" search results. Exclude patients who match Patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On COPD Register- LTC LCS Priority Group 2 (HR). Finally include patients who match Medication Issues [MEDICATION_ISSUES] with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Aclidinium Bromide OR Glycopyrronium bromide 55microgram inhalation powder capsules with device, Seebri Breezhaler 44microgram inhalation powder capsules with device (Novartis Pharmaceuticals UK Ltd), Indacaterol 85micrograms/dose / Glycopyrronium bromide 54micrograms/dose inhalation powder capsules with device +1 more OR Tiotropium bromide monohydrate, Umeclidinium Bromide, Ipratropium Bromide where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Bambuterol Hydrochloride, Formoterol Fumarate, Indacaterol +3 more OR Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Beclometasone 200micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler +18 more OR Fluticasone Furoate OR Aerivio Spiromax 50micrograms/dose / 500micrograms/dose dry powder inhaler (Teva UK Ltd), AirFluSal 25micrograms/dose / 125micrograms/dose inhaler (Sandoz Ltd), AirFluSal 25micrograms/dose / 250micrograms/dose inhaler (Sandoz Ltd) +22 more where Date of Issue within the last 6 months.

Boolean logic:
NOT (patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On COPD Register- LTC LCS Priority Group 2 (HR)) AND (Medication Issues [MEDICATION_ISSUES] with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Aclidinium Bromide OR Glycopyrronium bromide 55microgram inhalation powder capsules with device, Seebri Breezhaler 44microgram inhalation powder capsules with device (Novartis Pharmaceuticals UK Ltd), Indacaterol 85micrograms/dose / Glycopyrronium bromide 54micrograms/dose inhalation powder capsules with device +1 more OR Tiotropium bromide monohydrate, Umeclidinium Bromide, Ipratropium Bromide where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Bambuterol Hydrochloride, Formoterol Fumarate, Indacaterol +3 more OR Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Beclometasone 200micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler +18 more OR Fluticasone Furoate OR Aerivio Spiromax 50micrograms/dose / 500micrograms/dose dry powder inhaler (Teva UK Ltd), AirFluSal 25micrograms/dose / 125micrograms/dose inhaler (Sandoz Ltd), AirFluSal 25micrograms/dose / 250micrograms/dose inhaler (Sandoz Ltd) +22 more where Date of Issue within the last 6 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On COPD Register- LTC LCS Priority Group 1 (HRC) OR patients included in search On COPD Register- LTC LCS Priority Group 2 (HR)
- Population ref: On COPD Register- LTC LCS Priority Group 1 (HRC) (e07489c2-3c39-42da-961f-f905d81e607d)
- Population ref: On COPD Register- LTC LCS Priority Group 2 (HR) (2e944842-a252-4479-8530-78484eede55f)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Percent predicted FEV1 then Latest 1 where numeric value >= 50 and < 80
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg3_mr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs1`
  - Restriction: Latest 1 where numeric value >= 50 and < 80
    - Condition: READCODE IN
    - Condition: NUMERIC_VALUE IN | >= 50 and < 80

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Medical Research Council (MRC) Breathlessness Scale: grade 2, Medical Research Council Breathlessness Scale grade 2, Medical Research Council Breathlessness Scale: grade 3 +6 more
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg3_mr_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs2`

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with 1 COPD exacerbation in past year where Date within the last 12 months OR Medication Issues [MEDICATION_ISSUES] with Amoxicillin, Amoxicillin Trihydrate, Doxycycline +6 more where Date of Issue within the last 12 months OR Medication Issues [MEDICATION_ISSUES] with Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate where Date of Issue within the last 12 months OR Medication Issues [MEDICATION_ISSUES] with Azithromycin, Azithromycin where Date of Issue within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg3_mr_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs3`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs4`
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs4`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs5`
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs5`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs6`
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs6`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Trimbow, Trelegy Ellipta where Date of Issue within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs7`
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs7`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months

### Rule 6 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Medication Issues [MEDICATION_ISSUES] with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Aclidinium Bromide OR Glycopyrronium bromide 55microgram inhalation powder capsules with device, Seebri Breezhaler 44microgram inhalation powder capsules with device (Novartis Pharmaceuticals UK Ltd), Indacaterol 85micrograms/dose / Glycopyrronium bromide 54micrograms/dose inhalation powder capsules with device +1 more OR Tiotropium bromide monohydrate, Umeclidinium Bromide, Ipratropium Bromide where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Bambuterol Hydrochloride, Formoterol Fumarate, Indacaterol +3 more OR Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Beclometasone 200micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler +18 more OR Fluticasone Furoate OR Aerivio Spiromax 50micrograms/dose / 500micrograms/dose dry powder inhaler (Teva UK Ltd), AirFluSal 25micrograms/dose / 125micrograms/dose inhaler (Sandoz Ltd), AirFluSal 25micrograms/dose / 250micrograms/dose inhaler (Sandoz Ltd) +22 more where Date of Issue within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs8`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs8`
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs9`, `on_copd_reg_pg3_mr_vs10`, `on_copd_reg_pg3_mr_vs11`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs9`, `on_copd_reg_pg3_mr_vs10`, `on_copd_reg_pg3_mr_vs11`
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_copd_reg_pg3_mr_vs12`, `on_copd_reg_pg3_mr_vs13`, `on_copd_reg_pg3_mr_vs14`, `on_copd_reg_pg3_mr_vs15`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_copd_reg_pg3_mr_vs12`, `on_copd_reg_pg3_mr_vs13`, `on_copd_reg_pg3_mr_vs14`, `on_copd_reg_pg3_mr_vs15`


## ValueSet Friendly Names
### LTC LCS: COPD Register*
- `copd_reg_vs1` (SNOMED, 1 codes): Refset: 999011571000230107 | Cluster: COPD_COD
- `copd_reg_vs3` (SNOMED, 1 codes): Refset: 999020251000230104 | Cluster: FEV1FVC_COD
- `copd_reg_vs4` (SNOMED, 1 codes): Refset: 999020291000230109 | Cluster: FEV1FVCL70_COD
- `copd_reg_vs5` (SNOMED, 1 codes): UK NHS primary care data extraction - General practice data extraction - FEV1 FVC ratio below 70 per cent simple reference set
### On COPD Register- LTC LCS Priority Group 3 (MR)
- `on_copd_reg_pg3_mr_vs1` (SNOMED, 1 codes): Percent predicted FEV1
- `on_copd_reg_pg3_mr_vs2` (SNOMED, 9 codes): Medical Research Council (MRC) Breathlessness Scale: grade 2, Medical Research Council Breathlessness Scale grade 2, Medical Research Council Breathlessness Scale: grade 3 +6 more
- `on_copd_reg_pg3_mr_vs3` (SNOMED, 1 codes): 1 COPD exacerbation in past year
- `on_copd_reg_pg3_mr_vs4` (SCT Const, 9 codes): Amoxicillin, Amoxicillin Trihydrate, Doxycycline +6 more
- `on_copd_reg_pg3_mr_vs5` (SCT Const, 3 codes): Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate
- `on_copd_reg_pg3_mr_vs6` (SCT Const, 2 codes): Azithromycin, Azithromycin
- `on_copd_reg_pg3_mr_vs7` (Brand, 2 codes): Trimbow, Trelegy Ellipta
- `on_copd_reg_pg3_mr_vs8` (SCT Const, 6 codes): Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more
- `on_copd_reg_pg3_mr_vs9` (SCT Const, 1 codes): Aclidinium Bromide
- `on_copd_reg_pg3_mr_vs10` (SCT_PREP, 4 codes): Glycopyrronium bromide 55microgram inhalation powder capsules with device, Seebri Breezhaler 44microgram inhalation powder capsules with device (Novartis Pharmaceuticals UK Ltd), Indacaterol 85micrograms/dose / Glycopyrronium bromide 54micrograms/dose inhalation powder capsules with device +1 more
- `on_copd_reg_pg3_mr_vs11` (SCT Const, 3 codes): Tiotropium bromide monohydrate, Umeclidinium Bromide, Ipratropium Bromide
- `on_copd_reg_pg3_mr_vs12` (SCT Const, 6 codes): Bambuterol Hydrochloride, Formoterol Fumarate, Indacaterol +3 more
- `on_copd_reg_pg3_mr_vs13` (SCT_PREP, 21 codes): Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Beclometasone 200micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler +18 more
- `on_copd_reg_pg3_mr_vs14` (SCT Const, 1 codes): Fluticasone Furoate
- `on_copd_reg_pg3_mr_vs15` (SCT_PREP, 25 codes): Aerivio Spiromax 50micrograms/dose / 500micrograms/dose dry powder inhaler (Teva UK Ltd), AirFluSal 25micrograms/dose / 125micrograms/dose inhaler (Sandoz Ltd), AirFluSal 25micrograms/dose / 250micrograms/dose inhaler (Sandoz Ltd) +22 more