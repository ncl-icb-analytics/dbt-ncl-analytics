<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1veu9ss0-71jx-h6-0gtv-00uz2t60ghvy
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# Hypertension or Asthma Master

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "6. All on Hypertension or Asthma register" (see below). A patient is included when they do NOT match Rule 1.

## Start population

1. Currently registered patients
2. **6. All on Hypertension or Asthma register** — Include patients who match Patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: Hypertension Register*.
   - Combines: **LTC LCS: Asthma Adult Register***; **LTC LCS: Asthma CYP Register***; **LTC LCS: Hypertension Register***
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | Excluded | **Included** | Final — exclude if matched |

## Rule details

### Rule 1 of 1 — Final — exclude if matched

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when **ANY (OR)** of the following are true:

- They appear in the results of the search **GROUP1- HRC**
- They appear in the results of the search **GROUP2- HR**
- They appear in the results of the search **GROUP3- MR**
- They appear in the results of the search **GROUP4- LR**

## Code lists used

None.

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.