{{
    config(
        materialized='table',
        tags=['polypharmacy']
    )
}}

/*
BNF medications in scope for polypharmacy calculation.
Uses NHSBSA standard: BNF chapters 1-4 and 6-10 only.

Excludes:
- Chapter 5 (Infections): Acute/short-term antibiotics
- Chapters 11-15: Topical/local treatments, vaccines, anaesthesia
- Chapters 18+: Diagnostic agents, appliances, dressings

Grain: One row per SNOMED code included in polypharmacy scope
*/

SELECT
    snomed_code,
    bnf_code,
    LEFT(bnf_code, 2) AS bnf_chapter,
    bnf_name
FROM {{ ref('stg_reference_bnf_latest') }}
WHERE LEFT(bnf_code, 2) IN ('01', '02', '03', '04', '06', '07', '08', '09', '10')
    AND snomed_code IS NOT NULL
