<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 09iy47m0-74dr-g2-03o0-1pv29e00lxf8
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On COPD Register- LTC LCS Priority Group 1 (HRC)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On COPD Register- LTC LCS Priority Group 1 (HRC)
Parent population: Based on "LTC LCS: COPD Register*" search results

## Parent Chain
- LTC LCS: COPD Register*: Start with currently registered patients. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD.
  Library refs: ee5b135f-b9b2-4ef7-8b51-939a754cf935

## Library Items
- LTC LCS: COPD Register*: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Target Report Logic
Start with based on "ltc lcs: copd register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Percent predicted FEV1 then Latest 1 where numeric value < 30 OR Clinical Codes [EVENTS] with Medical Research Council (MRC) Breathlessness Scale: grade 5, Medical Research Council Breathlessness Scale grade 5, MRC Breathlessness Scale: grade 5 OR Clinical Codes [EVENTS] with Home oxygen supply, Oxygen therapy where Date within the last 12 months.

Boolean logic:
(Clinical Codes [EVENTS] with Percent predicted FEV1 then Latest 1 where numeric value < 30 OR Clinical Codes [EVENTS] with Medical Research Council (MRC) Breathlessness Scale: grade 5, Medical Research Council Breathlessness Scale grade 5, MRC Breathlessness Scale: grade 5 OR Clinical Codes [EVENTS] with Home oxygen supply, Oxygen therapy where Date within the last 12 months)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Percent predicted FEV1 then Latest 1 where numeric value < 30 OR Clinical Codes [EVENTS] with Medical Research Council (MRC) Breathlessness Scale: grade 5, Medical Research Council Breathlessness Scale grade 5, MRC Breathlessness Scale: grade 5 OR Clinical Codes [EVENTS] with Home oxygen supply, Oxygen therapy where Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg1_hrc_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg1_hrc_vs1`
  - Restriction: Latest 1 where numeric value < 30
    - Condition: READCODE IN
    - Condition: NUMERIC_VALUE IN | < 30
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg1_hrc_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg1_hrc_vs2`
- Clinical Codes [EVENTS]
  - ValueSets: `on_copd_reg_pg1_hrc_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `on_copd_reg_pg1_hrc_vs3`
  - Filter: Date IN within the last 12 months
    - From: within the last 12 months


## ValueSet Friendly Names
### LTC LCS: COPD Register*
- `copd_reg_vs1` (SNOMED, 1 codes): Refset: 999011571000230107 | Cluster: COPD_COD
- `copd_reg_vs3` (SNOMED, 1 codes): Refset: 999020251000230104 | Cluster: FEV1FVC_COD
- `copd_reg_vs4` (SNOMED, 1 codes): Refset: 999020291000230109 | Cluster: FEV1FVCL70_COD
- `copd_reg_vs5` (SNOMED, 1 codes): UK NHS primary care data extraction - General practice data extraction - FEV1 FVC ratio below 70 per cent simple reference set
### On COPD Register- LTC LCS Priority Group 1 (HRC)
- `on_copd_reg_pg1_hrc_vs1` (SNOMED, 1 codes): Percent predicted FEV1
- `on_copd_reg_pg1_hrc_vs2` (SNOMED, 3 codes): Medical Research Council (MRC) Breathlessness Scale: grade 5, Medical Research Council Breathlessness Scale grade 5, MRC Breathlessness Scale: grade 5
- `on_copd_reg_pg1_hrc_vs3` (SNOMED, 2 codes): Home oxygen supply, Oxygen therapy