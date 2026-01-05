{{
    config(
        materialized='table',
        alias='emis_olids_reg_fail',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
EMIS/OLIDS Unvalidated Practices (Direct Care)

Practices NOT meeting EMIS acceptance criteria for Regular registrations.
These practices require investigation before use in analyses.

Validation Criteria: >=2% variance AND >=5 persons difference
Snapshot Date: 04/11/2025 (from EMIS extract)
Registration Type: Regular only (excludes Temporary, Emergency, etc.)

Issue Categories:
- 2-5% Variance: Minor discrepancy, may warrant review
- 5-20% Variance: Moderate discrepancy, requires investigation
- 20%+ Variance: Major discrepancy, immediate attention needed
- Missing Data: No EMIS or OLIDS data available

Use Cases:
- Identifying practices requiring data quality investigation
- Monthly data quality reports
- Monitoring completeness of OLIDS Regular episode data
- Understanding which practices should be excluded from analyses

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Table alias: emis_olids_practices_unvalidated
- Review regularly to identify practices needing attention
- Filter by variance_category to prioritize investigations
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
    variance_category as issue_category
from {{ ref('int_emis_olids_practice_registration_comparison') }}
where meets_acceptance_criteria = false
order by
    case variance_category
        when '20%+ Variance' then 1
        when '5-20% Variance' then 2
        when '2-5% Variance' then 3
        else 4
    end,
    absolute_percent_difference desc
