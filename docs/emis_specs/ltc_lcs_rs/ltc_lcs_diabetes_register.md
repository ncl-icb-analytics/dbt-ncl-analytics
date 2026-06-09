<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1x0ilqd1-i68v-m9-1b36-0bhi4600hdw2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: Diabetes Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: Diabetes Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- None

## Target Report Logic
Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

Boolean logic:
(Patient Details [PATIENTS] where Age at least 17 years old) AND (Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Patient Details [PATIENTS] where Age at least 17 years old
- Patient Details [PATIENTS]
  - Filter: Age IN at least 17 years old
    - From: at least 17 years old

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `dm_reg_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `dm_reg_vs1`
  - Filter: Date
    - To: <=
  - Restriction: Latest 1
  - Linked criterion:
    - Clinical Codes [EVENTS] (NOT)
      - ValueSets: `dm_reg_vs2`
      - Relationship: DATE after parent record's DATE
      - Filter: Clinical Code
        - Filter ValueSets: `dm_reg_vs2`
      - Filter: Date
        - To: <=
      - Restriction: Latest 1


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD