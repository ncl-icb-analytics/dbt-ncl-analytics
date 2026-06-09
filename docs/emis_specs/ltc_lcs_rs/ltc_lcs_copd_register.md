<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0bkid961-q1iy-w4-0kge-144fcwf1s3r5
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: COPD Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: COPD Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: COPD Register*: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

## Target Report Logic
Start with currently registered patients. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD.

Boolean logic:
(Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: library item ee5b135f-b9b2-4ef7-8b51-939a754cf935
- Library item: Unknown library item (ee5b135f-b9b2-4ef7-8b51-939a754cf935)

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date before 1 year ago then Earliest 1 where SNOMED code IN: COPD_COD
- Clinical Codes [EVENTS]
  - ValueSets: `copd_reg_vs1`, `copd_reg_vs2`
  - Filter: Date IN before 1 year ago
    - To: before 1 year ago
  - Restriction: Earliest 1 where SNOMED code IN: COPD_COD
    - Condition: READCODE IN | COPD_COD

### Rule 3 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year OR Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year
- Clinical Codes [EVENTS]
  - ValueSets: `copd_reg_vs1`, `copd_reg_vs2`
  - Filter: Date IN within the last 1 year
    - From: within the last 1 year
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `copd_reg_vs3`
      - Relationship: DATE at least 93 days before and at most 186 days after parent record's DATE
      - Filter: Clinical Code
        - Filter ValueSets: `copd_reg_vs3`
      - Filter: Value IN < 0.7
        - To: < 0.7
      - Filter: Date
        - To: <=
      - Restriction: Earliest 1 where numeric value < 0.7
        - Condition: NUMERIC_VALUE IN | < 0.7
- Clinical Codes [EVENTS]
  - ValueSets: `copd_reg_vs1`, `copd_reg_vs2`
  - Filter: Date IN within the last 1 year
    - From: within the last 1 year
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `copd_reg_vs4`, `copd_reg_vs5`
      - Relationship: DATE at least 93 days before and at most 186 days after parent record's DATE
      - Filter: Clinical Code
        - Filter ValueSets: `copd_reg_vs4`
      - Filter: Date
        - To: <=
      - Restriction: Earliest 1
        - Condition: READCODE IN

### Rule 4 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD AND Patient Details [PATIENTS] where Registration Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `copd_reg_vs1`, `copd_reg_vs2`
  - Filter: Date IN within the last 1 year
    - From: within the last 1 year
  - Restriction: Earliest 1 where SNOMED code IN: COPD_COD
    - Condition: READCODE IN | COPD_COD
- Patient Details [PATIENTS]
  - Filter: Registration Date IN within the last 12 months
    - From: within the last 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `copd_reg_vs3`
      - Relationship: DATE at least 93 days before and at most 186 days after parent record's GMS_DATE_OF_REGISTRATION
      - Filter: Clinical Code
        - Filter ValueSets: `copd_reg_vs3`
      - Filter: Value IN < 0.7
        - To: < 0.7
      - Filter: Date
        - To: <=
      - Restriction: Earliest 1

### Rule 5 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD AND Patient Details [PATIENTS] where Registration Date within the last 12 months
- Clinical Codes [EVENTS]
  - ValueSets: `copd_reg_vs1`, `copd_reg_vs2`
  - Filter: Date IN within the last 1 year
    - From: within the last 1 year
  - Restriction: Earliest 1 where SNOMED code IN: COPD_COD
    - Condition: READCODE IN | COPD_COD
- Patient Details [PATIENTS]
  - Filter: Registration Date IN within the last 12 months
    - From: within the last 12 months
  - Linked criterion:
    - Clinical Codes [EVENTS]
      - ValueSets: `copd_reg_vs4`
      - Relationship: DATE at least 93 days before and at most 186 days after parent record's GMS_DATE_OF_REGISTRATION
      - Filter: Clinical Code
        - Filter ValueSets: `copd_reg_vs4`
      - Filter: Date
        - To: <=
      - Restriction: Earliest 1

### Rule 6 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Refset: 999011571000230107 OR Refset: 999009131000230100 where Date within the last 1 year then Earliest 1 where SNOMED code IN: COPD_COD
- Clinical Codes [EVENTS]
  - ValueSets: `copd_reg_vs1`, `copd_reg_vs2`
  - Filter: Date IN within the last 1 year
    - From: within the last 1 year
  - Restriction: Earliest 1 where SNOMED code IN: COPD_COD
    - Condition: READCODE IN | COPD_COD


## ValueSet Friendly Names
### LTC LCS: COPD Register*
- `copd_reg_vs1` (SNOMED, 1 codes): Refset: 999011571000230107 | Cluster: COPD_COD
- `copd_reg_vs3` (SNOMED, 1 codes): Refset: 999020251000230104 | Cluster: FEV1FVC_COD
- `copd_reg_vs4` (SNOMED, 1 codes): Refset: 999020291000230109 | Cluster: FEV1FVCL70_COD
- `copd_reg_vs5` (SNOMED, 1 codes): UK NHS primary care data extraction - General practice data extraction - FEV1 FVC ratio below 70 per cent simple reference set