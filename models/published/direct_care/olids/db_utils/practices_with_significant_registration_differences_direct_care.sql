{{
    config(
        materialized='view',
        alias='practices_with_suspect_registration_counts',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
PDS/OLIDS Practice Registration Discrepancies - Suspect Practices (Direct Care)

Shows practices with major discrepancies (>=20%) between PDS and OLIDS registration counts.
Uses episode_of_care with registration type filtering and PDS merger handling.

Use Cases:
- Identifying practices requiring immediate investigation
- Data quality monitoring dashboards
- Monthly data quality reporting

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- All practices shown have >=20% discrepancy
- pds_patient_count is the merged count (accounts for NHS number changes)
*/

SELECT
    practice_code,
    practice_name,
    pds_merged_persons AS pds_patient_count,
    olids_patient_count,
    difference,
    percent_difference
FROM {{ ref('int_pds_olids_practice_registration_comparison') }}
WHERE has_significant_discrepancy = TRUE
ORDER BY ABS(percent_difference) DESC
