<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 15du35t0-s30k-ap-1t9w-1u4bt161qxnh
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: on Diabetes Register- LTC LCS Priority Group 3 (MR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: on Diabetes Register- LTC LCS Priority Group 3 (MR)
Parent population: Based on "LTC LCS: Diabetes Register*" search results

## Parent Chain
- LTC LCS: Diabetes Register*: Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 17 years old. Finally include patients who match Clinical Codes [EVENTS] with Refset: 999004691000230108 then Latest 1.

## Library Items
- None

## Target Report Logic
Start with based on "ltc lcs: diabetes register*" search results. Exclude patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR). Finally include patients who match Patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa).

Boolean logic:
NOT (patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR)) AND (patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa))

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-not-match
- Pass: Exclude
- Fail: Next rule
- Operator: OR
- Summary: Must not match: patients included in search on Diabetes Register- LTC LCS Priority Group 1 (HRC) OR patients included in search on Diabetes Register- LTC LCS Priority Group 2 (HR)
- Population ref: on Diabetes Register- LTC LCS Priority Group 1 (HRC) (d132f6b5-fefd-4d7e-a16d-d33e5387cd81)
- Population ref: on Diabetes Register- LTC LCS Priority Group 2 (HR) (684e6c7a-077e-4f61-99b0-3324aee1d76f)

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: OR
- Summary: Included if matches: patients included in search on Diabetes Register- LTC LCS Priority Group 3B (MRb) OR patients included in search on Diabetes Register- LTC LCS Priority Group 3A (MRa)
- Population ref: on Diabetes Register- LTC LCS Priority Group 3B (MRb) (bcbd0990-42cd-468b-ab66-df76e1a614a4)
- Population ref: on Diabetes Register- LTC LCS Priority Group 3A (MRa) (ad2cbe36-3a96-4446-950c-1edffd856e3d)


## ValueSet Friendly Names
### LTC LCS: Diabetes Register*
- `dm_reg_vs1` (SNOMED, 1 codes): Refset: 999004691000230108 | Cluster: DM_COD
- `dm_reg_vs2` (SNOMED, 1 codes): Refset: 999003371000230102 | Cluster: DMRES_COD
### on Diabetes Register- LTC LCS Priority Group 3 (MR)
- None