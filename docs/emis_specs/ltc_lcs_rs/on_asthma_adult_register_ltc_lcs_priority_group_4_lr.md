<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1jlvkx90-a63b-96-05sg-0694nuk1chu1
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)*
Parent population: Based on "LTC LCS: Asthma Adult Register*" search results

## Parent Chain
- LTC LCS: Asthma Adult Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: asthma adult register*" search results. Exclude patients who match Patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)*. Finally include patients who match Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 12 months.

Boolean logic:
NOT (patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)*) AND (Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 12 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* OR patients included in search On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)*
- Population ref: On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)* (79cb583c-22e3-45b0-a5f5-89e813ade686)
- Population ref: On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)* (fa29c50e-03e1-4aad-84e1-52db7bf02fa5)
- Population ref: On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)* (67b569bc-998d-4a25-85b8-2c49cfef1d6c)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 6 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg4_lr_vs1`
  - Filter: Date of Issue IN within the last 6 months
    - From: within the last 6 months
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg4_lr_vs1`

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg4_lr_vs2`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg4_lr_vs2`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months


## ValueSet Friendly Names
### LTC LCS: Asthma Adult Register*
- `asthma_adult_reg_vs1` (SNOMED, 1 codes): Refset: 999010051000230100 | Cluster: ASTRES_COD
- `asthma_adult_reg_vs2` (SNOMED, 1 codes): Refset: 999012891000230104 | Cluster: AST_COD
- `asthma_adult_reg_vs3` (SNOMED, 521 codes): Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more | Cluster: ASTTRT_COD
### On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)*
- `on_asthma_adult_reg_pg4_lr_vs1` (SCT Const, 6 codes): Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more
- `on_asthma_adult_reg_pg4_lr_vs2` (SCT Const, 3 codes): Salbutamol, Salbutamol Cr, Terbutaline Sulfate