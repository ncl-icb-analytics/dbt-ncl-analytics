{{
    config(
        materialized='table',
        alias='emis_olids_reg_pass',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
EMIS/OLIDS Validated Practices (Direct Care)

Practices meeting EMIS acceptance criteria for Regular registrations.
Use this table for inner joins to filter datasets to practices with validated registration data.

Validation Criteria: <2% variance OR <5 persons difference
Snapshot Date: 04/11/2025 (from EMIS extract)
Registration Type: Regular only (excludes Temporary, Emergency, etc.)
Methodology: EMIS comparison using Regular episode type filtering

Acceptance Criteria Details:
- Per-practice: <2% variance OR fewer than 5 persons difference (whichever is greater)
- This flexible threshold accounts for both percentage and absolute differences
- Small practices can have larger percentage differences if absolute difference is small

Use Cases:
- Filter analyses to practices with reliable Regular registration data
- Inner join to exclude practices with data quality issues
- Quality assurance dashboards

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Table alias: emis_olids_practices_validated
- Join on practice_code to filter to validated practices only
*/

select
    practice_code,
    practice_name,
    borough,
    emis_list_size,
    olids_regular_count,
    difference,
    absolute_difference,
    percent_difference,
    extract_date as snapshot_date,
    variance_category as match_quality
from {{ ref('int_emis_olids_practice_registration_comparison') }}
where meets_acceptance_criteria = true
order by absolute_percent_difference desc
