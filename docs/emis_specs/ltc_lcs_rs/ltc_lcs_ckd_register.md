<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 07th1xs0-55mb-it-18oo-0vrkuy20w0sz
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: LTC LCS: CKD Register*

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: LTC LCS: CKD Register*
Parent population: Currently registered patients

## Parent Chain
- No parent reports.

## Library Items
- LTC LCS: CKD Register*: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)

## Target Report Logic
Start with currently registered patients. Require Patient Details [PATIENTS] where Age at least 18 years old. Finally include patients who match Library item c913f5a7-1256-4de6-871e-23650e72765e.

Boolean logic:
(Patient Details [PATIENTS] where Age at least 18 years old) AND (library item c913f5a7-1256-4de6-871e-23650e72765e)

## Detailed Rule Logic
### Rule 1 (Primary)
- Clause type: must-match
- Pass: Next rule
- Fail: Exclude
- Operator: AND
- Summary: Must match: Patient Details [PATIENTS] where Age at least 18 years old
- Patient Details [PATIENTS]
  - Filter: Age IN at least 18 years old
    - From: at least 18 years old

### Rule 2 (Additional)
- Clause type: include-if-match
- Pass: Include
- Fail: Exclude
- Operator: AND
- Summary: Included if matches: library item c913f5a7-1256-4de6-871e-23650e72765e
- Library item: Unknown library item (c913f5a7-1256-4de6-871e-23650e72765e)


## ValueSet Friendly Names
### LTC LCS: CKD Register*
- None