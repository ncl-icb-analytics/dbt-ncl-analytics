{{
    config(
        materialized='view',
        alias='practices_with_suspect_registration_counts',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
PDS/OLIDS Practice Registration Discrepancies (Direct Care)

Published view showing practices with significant discrepancies (>=20%) between
PDS and OLIDS registration counts. Uses episode_of_care with registration type filtering
and PDS merger handling for accurate comparison.

Use Cases:
- Data quality monitoring dashboards
- Practice-level data completeness assessment
- Identifying practices requiring investigation
- Monthly data quality reporting

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Filter by has_significant_discrepancy = TRUE for problem practices
- Use percent_difference for severity assessment
- pds_merged_persons accounts for NHS number changes (recommended for comparison)
*/

SELECT
    practice_code,
    practice_name,
    pds_unmerged_persons,
    pds_merged_persons,
    olids_patient_count,
    difference,
    percent_difference,
    has_significant_discrepancy
FROM {{ ref('int_pds_olids_practice_registration_comparison') }}
WHERE has_significant_discrepancy = TRUE
ORDER BY ABS(percent_difference) DESC
