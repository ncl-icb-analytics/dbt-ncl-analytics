<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 16bosq70-1gw6-k6-0080-1utq73b088lu
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only- M12: MAR

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with the patients found by "LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only" (see below). A patient is included when they match Rule 1.

## Who we start with

1. **LTC LCS Base*** — Start with currently registered patients. Include patients who match Patients included in search LTC LCS: AF Register* OR patients included in search LTC LCS: CKD Register* OR patients included in search LTC LCS: CHD Register* OR patients included in search LTC LCS: Diabetes Register* OR patients included in search LTC LCS: Hypertension Register* OR patients included in search LTC LCS: NAFLD Register v2* OR patients included in search LTC LCS: Asthma Adult Register* OR patients included in search LTC LCS: Asthma CYP Register* OR patients included in search LTC LCS: COPD Register* OR patients included in search LTC LCS: HF Register* OR patients included in search LTC LCS: PAD Register* OR patients included in search LTC LCS: Stroke/TIA Register*.
2. **LTC LCS MOC Base excluding CYP only, LR HTN only, LR Adult Asthma only** — Start with the patients found by "LTC LCS Base*". Finally include patients who do not match Patients included in search LTC LCS: Asthma CYP Register ONLY OR patients included in search On Asthma Adult Register ONLY- LTC LCS Priority Group 4 (LR Asthma Adult only) OR patients included in search On Hypertension Register ONLY- LTC LCS Priority Group 4 (LR HTN only).
3. **This search** then applies the rules below to that population.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when:
- **Patient Details**
  - Where date of birth 3 MONTH ABSOLUTE

## Code lists used

None.

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.