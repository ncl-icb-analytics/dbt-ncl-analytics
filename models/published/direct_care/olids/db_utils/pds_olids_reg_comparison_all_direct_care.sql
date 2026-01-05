{{
    config(
        materialized='table',
        alias='pds_olids_reg_comparison_all',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
PDS/OLIDS Practice Registration Comparison - All Practices (Direct Care)

Complete comparison of all practices with categorized discrepancies.
Uses registration episode filtering and PDS merger handling.

Categories:
- Major Difference: >=20% discrepancy
- Minor Difference: 5-20% discrepancy
- Good Match: <5% discrepancy
- No Data: Missing PDS or OLIDS data

Use Cases:
- Comprehensive data quality monitoring
- Practice-level completeness assessment
- Trend analysis over time
- Monthly reporting

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Table alias: pds_olids_practice_comparison_all
- Filter by match_category for different views
- pds_patient_count is the merged count (accounts for NHS number changes)
*/

select
    practice_code,
    practice_name,
    pds_merged_persons as pds_patient_count,
    olids_patient_count,
    difference,
    percent_difference,
    case
        when abs(percent_difference) >= 20 then 'Major Difference'
        when abs(percent_difference) >= 5 then 'Minor Difference'
        when percent_difference is not null then 'Good Match'
        else 'No Data'
    end as match_category
from {{ ref('int_pds_olids_practice_registration_comparison') }}
order by
    case
        when abs(coalesce(percent_difference, 0)) >= 20 then 1
        when abs(coalesce(percent_difference, 0)) >= 5 then 2
        when percent_difference is not null then 3
        else 4
    end,
    abs(coalesce(percent_difference, 0)) desc,
    practice_code
