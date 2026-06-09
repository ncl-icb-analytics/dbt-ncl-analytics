<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 027k9f11-s62w-hm-1rfs-1w8tbzi0zq1g
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025
Parent population: Based on "LTC LCS: Asthma CYP Register ONLY" search results

## Parent Chain
- LTC LCS: Asthma CYP Register ONLY: Start with based on "ltc lcs: asthma cyp register*" search results. Finally include patients who do not match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
- LTC LCS: Asthma CYP Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age under 18 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999010051000230100 OR Refset: 999012891000230104 then Latest 1 where SNOMED code IN: AST_COD AND Medication Issues [MEDICATION_ISSUES] with Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more where Date of Issue within the last 12 months.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: asthma cyp register only" search results. Finally include patients who match Medication Issues [MEDICATION_ISSUES] NOT with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 12 months AND Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 3 months.

Boolean logic:
(Medication Issues [MEDICATION_ISSUES] NOT with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 12 months AND Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 3 months)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Child on protection register, Child removed from protection register, Child no longer the subject of child protection plan +1 more OR Child on protection register then Latest 1 OR Clinical Codes [EVENTS] with Child in need, Child no longer in need OR Child in need then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs1`, `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs1`
  - Restriction: Latest 1
    - Condition: READCODE IN
- Clinical Codes [EVENTS]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs3`, `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs3`
  - Restriction: Latest 1
    - Condition: READCODE IN

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Emergency hospital admission for asthma, Emergency asthma admission since last encounter, Emergency asthma patient visit since last encounter where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs5`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Acute exacerbation of asthma, Acute severe exacerbation of asthma, Exacerbation of allergic asthma +36 more where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs6`
  - Filter: Clinical Code
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs6`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs7`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs7`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Theophylline, Theophylline Hydrate, Theophylline S/R +2 more where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs8`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs8`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months

### Rule 6 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 3 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9`
  - Filter: Date of Issue IN within the last 3 months
    - From: within the last 3 months

### Rule 7 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Medication Issues [MEDICATION_ISSUES] with Bambuterol Hydrochloride OR Atimos Modulite 12micrograms/dose inhaler (Chiesi Ltd), Foradil 12microgram inhalation powder capsules with device (Novartis Pharmaceuticals UK Ltd), Formoterol 12microgram inhalation powder capsules with device +25 more where Date of Issue within the last 12 months
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs10`, `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs11`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months
  - Filter: Drug
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs10`, `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs11`

### Rule 8 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Medication Issues [MEDICATION_ISSUES] NOT with Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more where Date of Issue within the last 12 months AND Medication Issues [MEDICATION_ISSUES] with Salbutamol, Salbutamol Cr, Terbutaline Sulfate where Date of Issue within the last 3 months
- Medication Issues [MEDICATION_ISSUES] (NOT)
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs12`
  - Filter: Date of Issue IN within the last 12 months
    - From: within the last 12 months
  - Filter: Drug
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs12`
- Medication Issues [MEDICATION_ISSUES]
  - ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9`
  - Filter: Drug
    - Filter ValueSets: `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9`
  - Filter: Date of Issue IN within the last 3 months
    - From: within the last 3 months


## ValueSet Friendly Names
### LTC LCS: Asthma CYP Register*
- `asthma_cyp_reg_vs1` (SNOMED, 1 codes): Refset: 999010051000230100 | Cluster: ASTRES_COD
- `asthma_cyp_reg_vs2` (SNOMED, 1 codes): Refset: 999012891000230104 | Cluster: AST_COD
- `asthma_cyp_reg_vs3` (SNOMED, 521 codes): Proxor 100micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Proxor 200micrograms/dose / 6micrograms/dose inhaler (Genus Pharmaceuticals Ltd), Budesonide 1mg/2ml nebuliser suspension unit dose ampoules +518 more | Cluster: ASTTRT_COD
### LTC LCS: Asthma CYP Register ONLY
- None
### On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs1` (SNOMED, 4 codes): Child on protection register, Child removed from protection register, Child no longer the subject of child protection plan +1 more
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs2` (SNOMED, 1 codes): Child on protection register
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs3` (SNOMED, 2 codes): Child in need, Child no longer in need
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs4` (SNOMED, 1 codes): Child in need
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs5` (SNOMED, 3 codes): Emergency hospital admission for asthma, Emergency asthma admission since last encounter, Emergency asthma patient visit since last encounter
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs6` (SNOMED, 39 codes): Acute exacerbation of asthma, Acute severe exacerbation of asthma, Exacerbation of allergic asthma +36 more
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs7` (SCT Const, 3 codes): Prednisolone, Prednisolone Sodium Phosphate, Prednisolone Steaglate
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs8` (SCT Const, 5 codes): Theophylline, Theophylline Hydrate, Theophylline S/R +2 more
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9` (SCT Const, 3 codes): Salbutamol, Salbutamol Cr, Terbutaline Sulfate
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs10` (SCT Const, 1 codes): Bambuterol Hydrochloride
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs11` (SCT_PREP, 28 codes): Atimos Modulite 12micrograms/dose inhaler (Chiesi Ltd), Foradil 12microgram inhalation powder capsules with device (Novartis Pharmaceuticals UK Ltd), Formoterol 12microgram inhalation powder capsules with device +25 more
- `on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs12` (SCT Const, 6 codes): Beclometasone Dipropionate, Budesonide, Ciclesonide +3 more