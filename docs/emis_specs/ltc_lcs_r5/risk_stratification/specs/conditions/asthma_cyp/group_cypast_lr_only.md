<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1qh132m1-1fi8-om-1qmr-1vyxkuu0dma1
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# GROUP:CYPAST_LR_ONLY

Report title: [GROUP:CYPAST_LR_ONLY] On Asthma(CYP) Register- LOW RISK
Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "5. LTC LCS: Asthma CYP register ONLY" (see below). A patient is included when they match Rule 1.

## Start population

1. Currently registered patients
2. **5. LTC LCS: Asthma CYP register ONLY** — Include patients who match Patients included in search LTC LCS: Asthma CYP Register ONLY.
   - Combines: **LTC LCS: Asthma CYP Register ONLY**
3. **This search** — applies the rules below.

## Rule flow

| Rule | If patient matches | If patient does not match | Role |
| --- | --- | --- | --- |
| 1 | **Included** | Excluded | Final — include if matched |

## Rule details

### Rule 1 of 1 — Final — include if matched

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:

- They appear in the results of the search **On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)***

## Code lists used

None.

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.