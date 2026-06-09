<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1ceo9k51-k9y8-87-1hpw-02qbpqq1bwy2
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: Attends MOC since Apr 2024

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: Attends MOC since Apr 2024
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Finally include patients who match Clinical Codes [EVENTS] with Personalised Care and Support Plan agreed where Date at least 01/04/2024 OR Clinical Codes [EVENTS] with Review of Personalised Care and Support Plan where Date at least 01/04/2024.

Boolean logic:
(Clinical Codes [EVENTS] with Personalised Care and Support Plan agreed where Date at least 01/04/2024 OR Clinical Codes [EVENTS] with Review of Personalised Care and Support Plan where Date at least 01/04/2024)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: AND
- Summary: Clinical Codes [EVENTS] with Chronic disease initial assessment where Date at least 01/04/2024
- Clinical Codes [EVENTS]
  - ValueSets: `attends_moc_since_apr_2024_vs1`
  - Filter: Clinical Code
    - Filter ValueSets: `attends_moc_since_apr_2024_vs1`
  - Filter: Date IN at least 01/04/2024
    - From: at least 01/04/2024

### Rule 2 (Additional)
- Clause type: informational
- Pass: Include
- Fail: Next rule
- Operator: OR
- Summary: Clinical Codes [EVENTS] with Chronic disease management annual review completed where Date at least 01/04/2024 OR Clinical Codes [EVENTS] with Long term condition summary sent to patient where Date at least 01/04/2024
- Clinical Codes [EVENTS]
  - ValueSets: `attends_moc_since_apr_2024_vs2`
  - Filter: Clinical Code
    - Filter ValueSets: `attends_moc_since_apr_2024_vs2`
  - Filter: Date IN at least 01/04/2024
    - From: at least 01/04/2024
- Clinical Codes [EVENTS]
  - ValueSets: `attends_moc_since_apr_2024_vs3`
  - Filter: Clinical Code
    - Filter ValueSets: `attends_moc_since_apr_2024_vs3`
  - Filter: Date IN at least 01/04/2024
    - From: at least 01/04/2024

### Rule 3 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: Clinical Codes [EVENTS] with Personalised Care and Support Plan agreed where Date at least 01/04/2024 OR Clinical Codes [EVENTS] with Review of Personalised Care and Support Plan where Date at least 01/04/2024
- Clinical Codes [EVENTS]
  - ValueSets: `attends_moc_since_apr_2024_vs4`
  - Filter: Clinical Code
    - Filter ValueSets: `attends_moc_since_apr_2024_vs4`
  - Filter: Date IN at least 01/04/2024
    - From: at least 01/04/2024
- Clinical Codes [EVENTS]
  - ValueSets: `attends_moc_since_apr_2024_vs5`
  - Filter: Clinical Code
    - Filter ValueSets: `attends_moc_since_apr_2024_vs5`
  - Filter: Date IN at least 01/04/2024
    - From: at least 01/04/2024


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### Attends MOC since Apr 2024
- `attends_moc_since_apr_2024_vs1` (SNOMED, 1 codes): Chronic disease initial assessment
- `attends_moc_since_apr_2024_vs2` (SNOMED, 1 codes): Chronic disease management annual review completed
- `attends_moc_since_apr_2024_vs3` (SNOMED, 1 codes): Long term condition summary sent to patient
- `attends_moc_since_apr_2024_vs4` (SNOMED, 1 codes): Personalised Care and Support Plan agreed
- `attends_moc_since_apr_2024_vs5` (SNOMED, 1 codes): Review of Personalised Care and Support Plan