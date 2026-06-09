<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0r0npvo0-tjyk-kp-0hip-15n7nxd0guto
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*
Parent population: Based on "LTC LCS: Asthma CYP Register ONLY" search results

## Parent Chain
- LTC LCS: Asthma CYP Register ONLY: Start with based on "ltc lcs: asthma cyp register*" search results. Finally include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
- LTC LCS: Asthma CYP Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age under 18 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: asthma cyp register only" search results. Finally include patients who do not match Patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2.

Boolean logic:
NOT (patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: patients included in search On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2
- Population ref: On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 (cbe1df41-0a17-498a-aa99-43116bc6d9b7)


## ValueSet Friendly Names
### LTC LCS: Asthma CYP Register*
- `asthma_cyp_reg_vs1` (SNOMED, 1 codes): Refset: 999010051000230100 | Cluster: ASTRES_COD
- `asthma_cyp_reg_vs2` (SNOMED, 1 codes): Refset: 999012891000230104 | Cluster: AST_COD
- `asthma_cyp_reg_vs3` (SNOMED, 521 codes): Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more | Cluster: ASTTRT_COD
### LTC LCS: Asthma CYP Register ONLY
- None
### On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*
- None