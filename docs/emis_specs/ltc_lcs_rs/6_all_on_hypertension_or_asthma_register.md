<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 0xz3f6g1-s3bi-x3-03pf-0k3vqus1kb3w
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
     extracted: 2026-06-09 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# 6. All on Hypertension or Asthma register

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. A patient is included when they match Rule 1.

## Who we start with

Currently registered patients.

## Inclusion logic, step by step

### Rule 1 of 1

Final rule: patients who match are **included**; everyone else is excluded.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **LTC LCS: Asthma Adult Register***
- They appear in the results of the search **LTC LCS: Asthma CYP Register***
- They appear in the results of the search **LTC LCS: Hypertension Register***

## Code lists used

None.

## Caveats

- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.