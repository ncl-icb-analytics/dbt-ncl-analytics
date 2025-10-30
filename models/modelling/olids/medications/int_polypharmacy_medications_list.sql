{{
    config(
        materialized='table',
        tags=['polypharmacy']
    )
}}

/*
BNF medications in scope for polypharmacy calculation.
Uses exclusion-based approach with granular subchapter-level filtering.

Exclusions are managed in seed file: bnf_polypharmacy_exclusions.csv
This approach provides:
- Single source of truth for exclusions
- Future-proofing: new BNF subchapters automatically included unless explicitly excluded
- Auditable configuration changes

Key exclusions:
- Chapter 5: Most infections (except HIV 5.3.1 and hepatitis 5.3.3)
- Chapter 9: Vitamins, minerals, nutrition products
- Chapter 6.2: Thyroid drugs
- Chapters 7.1-7.3: Obstetrics, contraceptives
- Chapters 11-15, 18, 20-23: Topicals, devices, dressings
- Various subchapters: cough preparations, stoma care, etc.

Logic:
1. Start with all BNF medications that have valid SNOMED codes
2. Apply hierarchical exclusion rules (subchapter overrides chapter)
3. Only include medications not marked as excluded
4. This ensures only drugs with valid BNF mappings in non-excluded chapters are included

Grain: One row per SNOMED code included in polypharmacy scope
*/

WITH bnf_base AS (
    SELECT
        snomed_code,
        bnf_code,
        LEFT(bnf_code, 2) AS bnf_chapter_2digit,
        SUBSTRING(bnf_code, 1, 5) AS bnf_subchapter_4digit,
        bnf_name
    FROM {{ ref('stg_reference_bnf_latest') }}
    WHERE snomed_code IS NOT NULL
        AND bnf_code IS NOT NULL
        AND LEN(bnf_code) >= 2  -- Must have at least chapter-level BNF code
),

exclusions AS (
    SELECT
        bnf_chapter_code,
        chapter_name,
        is_excluded
    FROM {{ ref('stg_reference_bnf_polypharmacy_exclusions') }}
),

-- Apply exclusions hierarchically: subchapter-level rules override chapter-level rules
-- More specific exclusions (4-digit) take precedence over less specific (2-digit)
medications_with_exclusion_status AS (
    SELECT
        b.snomed_code,
        b.bnf_code,
        b.bnf_chapter_2digit AS bnf_chapter,
        b.bnf_name,
        -- Check if excluded at subchapter level (4-digit)
        e_sub.is_excluded AS excluded_at_subchapter,
        -- Check if excluded at chapter level (2-digit)
        e_chap.is_excluded AS excluded_at_chapter,
        -- Determine final exclusion status:
        -- 1. If subchapter rule exists, use it (handles explicit inclusions like 5.3.1)
        -- 2. Otherwise, use chapter rule if it exists
        -- 3. Otherwise, default to FALSE (include by default)
        COALESCE(e_sub.is_excluded, e_chap.is_excluded, FALSE) AS is_excluded
    FROM bnf_base b
    LEFT JOIN exclusions e_sub
        ON b.bnf_subchapter_4digit = e_sub.bnf_chapter_code
    LEFT JOIN exclusions e_chap
        ON b.bnf_chapter_2digit = e_chap.bnf_chapter_code
)

-- Final list: only medications that are NOT excluded
SELECT
    snomed_code,
    bnf_code,
    bnf_chapter,
    bnf_name
FROM medications_with_exclusion_status
WHERE is_excluded = FALSE
