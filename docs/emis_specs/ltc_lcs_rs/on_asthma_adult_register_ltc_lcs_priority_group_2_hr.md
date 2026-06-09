<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0gu52gt0-cwlb-86-192l-1k6n4u21gc6p
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)*
Parent population: Based on "LTC LCS: Asthma Adult Register*" search results

## Parent Chain
- LTC LCS: Asthma Adult Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: asthma adult register*" search results. Exclude patients who match Patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*. Finally include patients who match Medication Issues [MEDICATION_ISSUES] with Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Fostair 100micrograms/dose / 6micrograms/dose inhaler (Chiesi Ltd) +31 more where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Beclazone 200 inhaler (Teva UK Ltd), Beclazone 250 Easi-Breathe inhaler (Teva UK Ltd), Beclazone 250 inhaler (Teva UK Ltd) +31 more where Date of Issue within the last 6 months.

Boolean logic:
NOT (patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*) AND (Medication Issues [MEDICATION_ISSUES] with Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Fostair 100micrograms/dose / 6micrograms/dose inhaler (Chiesi Ltd) +31 more where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Beclazone 200 inhaler (Teva UK Ltd), Beclazone 250 Easi-Breathe inhaler (Teva UK Ltd), Beclazone 250 inhaler (Teva UK Ltd) +31 more where Date of Issue within the last 6 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: AND
- Summary: Must not match: patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*
- Population ref: On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* (79cb583c-22e3-45b0-a5f5-89e813ade686)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Emergency hospital admission for asthma, Emergency asthma admission since last encounter where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs1`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Medication Issues [MEDICATION_ISSUES] with Tiotropium bromide monohydrate where Date of Issue within the last 6 months OR Medication Issues [MEDICATION_ISSUES] with Montelukast Sodium where Date of Issue within the last 12 months OR Medication Issues [MEDICATION_ISSUES] with Theophylline, Theophylline Hydrate, Theophylline S/R +2 more where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs2`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs3`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs3`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs4`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs4`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Medication Issues [MEDICATION_ISSUES] with Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs5`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs5`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Amoxicillin, Amoxicillin Trihydrate, Doxycycline +6 more where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs6`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs6`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months

### Rule 6 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Fostair 200micrograms/dose / 6micrograms/dose inhaler (Chiesi Ltd), Fostair NEXThaler 200micrograms/dose / 6micrograms/dose dry powder inhaler (Chiesi Ltd), Budesonide 200micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler +25 more where Date of Issue within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs7`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs7`

### Rule 7 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Medication Issues [MEDICATION_ISSUES] with Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Fostair 100micrograms/dose / 6micrograms/dose inhaler (Chiesi Ltd) +31 more where Date of Issue within the last 6 months AND Medication Issues [MEDICATION_ISSUES] with Beclazone 200 inhaler (Teva UK Ltd), Beclazone 250 Easi-Breathe inhaler (Teva UK Ltd), Beclazone 250 inhaler (Teva UK Ltd) +31 more where Date of Issue within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs8`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs8`
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg2_hr_vs9`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg2_hr_vs9`


## ValueSet Friendly Names
### LTC LCS: Asthma Adult Register*
- `asthma_adult_reg_vs1` (SNOMED, 1 codes): Refset: 999010051000230100 | Cluster: ASTRES_COD
- `asthma_adult_reg_vs2` (SNOMED, 1 codes): Refset: 999012891000230104 | Cluster: AST_COD
- `asthma_adult_reg_vs3` (SNOMED, 521 codes): Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more | Cluster: ASTTRT_COD
### On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)*
- `on_asthma_adult_reg_pg2_hr_vs1` (SNOMED, 2 codes): Emergency hospital admission for asthma, Emergency asthma admission since last encounter
- `on_asthma_adult_reg_pg2_hr_vs2` (SCT Const, 1 codes): Tiotropium bromide monohydrate
- `on_asthma_adult_reg_pg2_hr_vs3` (SCT Const, 1 codes): Montelukast Sodium
- `on_asthma_adult_reg_pg2_hr_vs4` (SCT Const, 5 codes): Theophylline, Theophylline Hydrate, Theophylline S/R +2 more
- `on_asthma_adult_reg_pg2_hr_vs5` (SCT Const, 3 codes): Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate
- `on_asthma_adult_reg_pg2_hr_vs6` (SCT Const, 9 codes): Amoxicillin, Amoxicillin Trihydrate, Doxycycline +6 more
- `on_asthma_adult_reg_pg2_hr_vs7` (SCT_PREP, 28 codes): Fostair 200micrograms/dose / 6micrograms/dose inhaler (Chiesi Ltd), Fostair NEXThaler 200micrograms/dose / 6micrograms/dose dry powder inhaler (Chiesi Ltd), Budesonide 200micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler +25 more
- `on_asthma_adult_reg_pg2_hr_vs8` (SCT_PREP, 34 codes): Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free, Fostair 100micrograms/dose / 6micrograms/dose inhaler (Chiesi Ltd) +31 more
- `on_asthma_adult_reg_pg2_hr_vs9` (SCT_PREP, 34 codes): Beclazone 200 inhaler (Teva UK Ltd), Beclazone 250 Easi-Breathe inhaler (Teva UK Ltd), Beclazone 250 inhaler (Teva UK Ltd) +31 more