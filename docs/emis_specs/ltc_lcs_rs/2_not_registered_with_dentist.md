<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 070c53a1-jgut-o1-0uxf-10wcp2i1ln9h
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: 2- Not Registered with Dentist

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: 2- Not Registered with Dentist
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Registered with dentist, Patient not registered with dentist, Advised to see dentist OR Patient not registered with dentist then Latest 1.

Boolean logic:
(Clinical Codes [EVENTS] with Registered with dentist, Patient not registered with dentist, Advised to see dentist OR Patient not registered with dentist then Latest 1)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: Clinical Codes [EVENTS] with Registered with dentist, Patient not registered with dentist, Advised to see dentist OR Patient not registered with dentist then Latest 1
- Clinical Codes [EVENTS]
  - ValueSets: `2_not_registered_with_dentist_vs1`, `2_not_registered_with_dentist_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `2_not_registered_with_dentist_vs1`
  - Restriction: Latest 1
    - Condition: READCODE IN


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### 2- Not Registered with Dentist
- `2_not_registered_with_dentist_vs1` (SNOMED, 3 codes): Registered with dentist, Patient not registered with dentist, Advised to see dentist
- `2_not_registered_with_dentist_vs2` (SNOMED, 1 codes): Patient not registered with dentist