<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0z99a4d0-teiy-lr-06p4-1h50fkl1c4ug
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only)

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "On Hypertension Register- LTC LCS Priority Group 4 (LR)" (see below). Rule 1 includes only patients who do NOT match it.

## Who we start with

1. **LTC LCS: Hypertension Register*** — Start with currently registered patients. Include patients who match Hypertension Register (library item a5ff1b4e-f130-4fea-b11c-5b40dc9b0877).
2. **On Hypertension Register- LTC LCS Priority Group 4 (LR)** — Start with the patients found by "LTC LCS: Hypertension Register*". Finally include patients who do not match Patients included in search On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 2 (HR) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3 OR patients included in search On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3.
3. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **LTC LCS: AF Register***
- They appear in the results of the search **LTC LCS: CKD Register***
- They appear in the results of the search **LTC LCS: CHD Register***
- They appear in the results of the search **LTC LCS: Diabetes Register***
- They appear in the results of the search **LTC LCS: NAFLD Register v2***
- They appear in the results of the search **LTC LCS: Asthma Adult Register***
- They appear in the results of the search **LTC LCS: Asthma CYP Register***
- They appear in the results of the search **LTC LCS: COPD Register***
- They appear in the results of the search **LTC LCS: HF Register***
- They appear in the results of the search **LTC LCS: PAD Register***
- They appear in the results of the search **LTC LCS: Stroke/TIA Register***

## Code lists used

None.

## Caveats

- LTC LCS: Hypertension Register* references the EMIS library item `a5ff1b4e-f130-4fea-b11c-5b40dc9b0877`, whose logic is not included in this XML export. It is likely **Hypertension Register** (inferred from wrapper report "LTC LCS: Hypertension Register*"), but this is not certain. Verify it in EMIS before implementing.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.