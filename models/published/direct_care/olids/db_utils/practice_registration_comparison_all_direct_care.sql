{{
    config(
        materialized='view',
        alias='practice_registration_comparison_all',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
PDS/OLIDS Practice Registration Comparison - All Practices (Direct Care)

Complete comparison of all practices with categorized discrepancies.
Uses episode_of_care with registration type filtering and PDS merger handling.

Categories:
- Major Difference: >=20% discrepancy
- Minor Difference: 5-20% discrepancy
- Good Match: <5% discrepancy

Use Cases:
- Comprehensive data quality monitoring
- Practice-level completeness assessment
- Trend analysis over time
- Monthly reporting

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Filter by match_category for different views
- pds_patient_count is the merged count (accounts for NHS number changes)
*/

SELECT
    practice_code,
    practice_name,
    pds_merged_persons AS pds_patient_count,
    olids_patient_count,
    difference,
    percent_difference,
    CASE
        WHEN ABS(percent_difference) >= 20 THEN 'Major Difference'
        WHEN ABS(percent_difference) >= 5 THEN 'Minor Difference'
        WHEN percent_difference IS NOT NULL THEN 'Good Match'
        ELSE 'No Data'
    END AS match_category
FROM {{ ref('int_pds_olids_practice_registration_comparison') }}
ORDER BY
    CASE
        WHEN ABS(percent_difference) >= 20 THEN 1
        WHEN ABS(percent_difference) >= 5 THEN 2
        WHEN percent_difference IS NOT NULL THEN 3
        ELSE 4
    END,
    ABS(percent_difference) DESC
