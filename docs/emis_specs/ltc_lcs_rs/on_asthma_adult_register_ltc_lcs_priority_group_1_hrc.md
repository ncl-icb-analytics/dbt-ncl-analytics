<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1m0bjxd0-dsp8-w3-02o0-1fuvsr51rwjo
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*
Parent population: Based on "LTC LCS: Asthma Adult Register*" search results

## Parent Chain
- LTC LCS: Asthma Adult Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: asthma adult register*" search results. Finally include patients who match Medication Issues [MEDICATION_ISSUES] with Omalizumab, Mepolizumab, Reslizumab +1 more where Date of Issue within the last 12 months.

Boolean logic:
(Medication Issues [MEDICATION_ISSUES] with Omalizumab, Mepolizumab, Reslizumab +1 more where Date of Issue within the last 12 months)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Medication Issues [MEDICATION_ISSUES] with Omalizumab, Mepolizumab, Reslizumab +1 more where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_adult_reg_pg1_hrc_vs1`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_adult_reg_pg1_hrc_vs1`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months


## ValueSet Friendly Names
### LTC LCS: Asthma Adult Register*
- `asthma_adult_reg_vs1` (SNOMED, 1 codes): Refset: 999010051000230100 | Cluster: ASTRES_COD
- `asthma_adult_reg_vs2` (SNOMED, 1 codes): Refset: 999012891000230104 | Cluster: AST_COD
- `asthma_adult_reg_vs3` (SNOMED, 521 codes): Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more | Cluster: ASTTRT_COD
### On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*
- `on_asthma_adult_reg_pg1_hrc_vs1` (SCT Const, 4 codes): Omalizumab, Mepolizumab, Reslizumab +1 more