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
PDS and OLIDS registration counts.

Use Cases:
- Data quality monitoring dashboards
- Practice-level data completeness assessment
- Identifying practices requiring investigation
- Monthly data quality reporting

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Filter by has_significant_discrepancy = TRUE for problem practices
- Use percent_difference for severity assessment
*/

SELECT
    practice_code,
    practice_name,
    pds_patient_count,
    olids_patient_count,
    difference,
    percent_difference,
    has_significant_discrepancy
FROM {{ ref('int_pds_olids_practice_registration_comparison') }}
WHERE has_significant_discrepancy = TRUE
ORDER BY ABS(percent_difference) DESC
