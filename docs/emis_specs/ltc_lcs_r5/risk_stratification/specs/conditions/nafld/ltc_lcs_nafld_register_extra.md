<!-- Extracted from 'NCL LTC LCS R5 updated 27112025.xml' (sha256 e4984ab973047c074d5cecc3264b29e424751894cf342b1ff371b39c39f5e8f8)
     report id: 1vtx3uz1-4dxy-i6-1c8x-1783r120qy7v
     folder: NCL LTC LCS R5.0 updated: 27112025 > 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
     extracted: 2026-06-10 by scripts/extract_emis_ltc_lcs_specs.py
     Readable guide only: for exact operators/ranges query the agent API
     (agentInterpretation.decisionFlow[].criteriaDetails). -->

# LTC LCS: NAFLD Register*- EXTRA

Folder: 6) Data Quality > zHouse keeping > zSupporting Searches > Risk Stratification R2 > Disease
Source: NCL LTC LCS R5.0 updated: 27112025

## What this search does

Start with currently registered patients. Patients must match Rule 1 to stay in. Patients matching Rule 2 are excluded. A patient is included when they do NOT match Rule 3. Rules run in order; each patient stops at the first rule that includes or excludes them.

## Who we start with

Currently registered patients.

## Inclusion logic, step by step

### Rule 1 of 3

Patients **must match** this rule to stay in. Those who match continue to Rule 2; those who do not are excluded.

A patient matches this rule when:
- **Clinical Codes** (clinical events)
  - Code in: `nafld_reg_extra_vs1` (16 codes)

### Rule 2 of 3

Patients matching this rule are **excluded** and no further rules are checked. Everyone else continues to Rule 3.

A patient matches this rule when:
- They appear in the results of the search **LTC LCS: NAFLD Register v2***

### Rule 3 of 3

Final rule: patients who match are **excluded**; everyone else is included.

A patient matches this rule when ANY of the following is true:
- They appear in the results of the search **LTC LCS: AF Register***
- They appear in the results of the search **LTC LCS: CKD Register***
- They appear in the results of the search **LTC LCS: CHD Register***
- They appear in the results of the search **LTC LCS: Diabetes Register***
- They appear in the results of the search **LTC LCS: Hypertension Register***
- They appear in the results of the search **LTC LCS: Asthma Adult Register***
- They appear in the results of the search **LTC LCS: Asthma CYP Register***
- They appear in the results of the search **LTC LCS: COPD Register***
- They appear in the results of the search **LTC LCS: HF Register***
- They appear in the results of the search **LTC LCS: PAD Register***
- They appear in the results of the search **LTC LCS: Stroke/TIA Register***

## Code lists used

Names below match `valueset_friendly_name` in the extraction CSVs. The hash identifies the exact code list content, so a changed hash means the codes changed.

| Search | Code list | Cluster | System | Codes | Content | Hash |
| --- | --- | --- | --- | --- | --- | --- |
| LTC LCS: NAFLD Register*- EXTRA | `nafld_reg_extra_vs1` |  | SNOMED | 16 | Fatty liver, Acute fatty liver of pregnancy, Hepatic fibrosis due to non-alco... | f5b3dad9 |

## Caveats

- Some code lists exclude specific codes. See `exceptions.csv` in the extraction for the excluded codes and whether each was applied.
- This guide is generated from the EMIS XML export. Validate it against the source search in EMIS before implementing.