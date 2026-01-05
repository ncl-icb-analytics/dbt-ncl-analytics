{{
    config(
        materialized='table',
        alias='pds_olids_reg_pass',
        tags=['data_quality', 'published', 'secondary_use']
    )
}}

/*
PDS/OLIDS Validated Practices (Secondary Use)

Practices with <20% discrepancy between PDS and OLIDS registration counts.
Use this table for inner joins to filter datasets to practices with validated registration data.

Validation Threshold: <20% difference between PDS merged persons and OLIDS patients
Methodology: PDS comparison using merged NHS numbers, registration episode filtering

Use Cases:
- Filter analyses to practices with reliable registration data
- Inner join to exclude practices with data quality issues
- Quality assurance dashboards

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__SECONDARY_USE.OLIDS_PUBLISHED schema
- Table alias: pds_olids_practices_validated
- Join on practice_code to filter to validated practices only
*/

select
    practice_code,
    practice_name,
    pds_merged_persons as pds_patient_count,
    olids_patient_count,
    difference,
    absolute_difference,
    percent_difference,
    absolute_percent_difference,
    validation_methodology,
    case
        when absolute_percent_difference < 1 then 'Excellent Match (<1%)'
        when absolute_percent_difference < 2 or absolute_difference < 5 then 'Meets Criteria'
        else 'Good Match'
    end as match_quality
from {{ ref('int_pds_olids_practice_registration_comparison') }}
where meets_acceptance_criteria = true
order by absolute_percent_difference desc
