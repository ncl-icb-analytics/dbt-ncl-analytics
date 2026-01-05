{{
    config(
        materialized='table',
        alias='pds_olids_reg_fail',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
PDS/OLIDS Unvalidated Practices (Direct Care)

Practices with >=20% discrepancy between PDS and OLIDS registration counts.
These practices require investigation before use in analyses.

Validation Threshold: >=20% difference between PDS merged persons and OLIDS patients
Methodology: PDS comparison using merged NHS numbers, registration episode filtering

Use Cases:
- Identifying practices requiring data quality investigation
- Monthly data quality reports
- Monitoring completeness of OLIDS coverage

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Table alias: pds_olids_practices_unvalidated
- Review regularly to identify practices needing attention
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
        when absolute_percent_difference >= 20 then '20%+ Variance'
        when absolute_percent_difference >= 5 then '5-20% Variance'
        else '2-5% Variance'
    end as issue_category
from {{ ref('int_pds_olids_practice_registration_comparison') }}
where meets_acceptance_criteria = false
order by absolute_percent_difference desc
