<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0tr8o3q1-nkcy-rp-1n5i-0hj3j160vu8t
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Implementation Guide: On Hypertension Register- LTC LCS Priority Group 4 (LR)

Important: This markdown is a readable guide. For exact operators, ranges, thresholds, restrictions, and linked-criterion logic, inspect `report.agentInterpretation.decisionFlow[].criteriaDetails` in the JSON response.

Target report: On Hypertension Register- LTC LCS Priority Group 4 (LR)
Parent population: Based on "LTC LCS: Hypertension Register*" search results

## Parent Chain
- LTC LCS: Hypertension Register*: Start with currently registered patients. Finally include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
  Library refs: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877)

## Library Items
- LTC LCS: Hypertension Register*: Hypertension Register (a5ff1b4e-f130-4fea-b11c-5b40dc9b0877); wrapper reports: LTC LCS: Hypertension Register*

## Target Report Logic
Start with based on "ltc lcs: hypertension register*" search results. Finally include patients who do not match Patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3.

Boolean logic:
NOT (patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3)

## Detailed Rule Logic
### Rule 1
- Clause type: include-if-not-match
- Pass: Exclude
- Fail: Include
- Operator: OR
- Summary: Included if it does not match: patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3
- Population ref: On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 (bf30380c-6e3e-4a4a-ba3d-8529d45ce74f)
- Population ref: On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 (2b4ea7fe-2657-486d-a276-9cdc39835660)
- Population ref: On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 (9e0e6ca6-dfa9-4197-816e-d7df66dd8324)
- Population ref: On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3 (7389d325-2293-4b14-89d1-37775446952b)


## ValueSet Friendly Names
### LTC LCS: Hypertension Register*
- None
### On Hypertension Register- LTC LCS Priority Group 4 (LR)
- None