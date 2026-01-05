{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'emis', 'olids', 'aggregate']
    )
}}

/*
EMIS/OLIDS Aggregate Validation

Calculates aggregate variance across all practices to validate against acceptance criteria.
Uses EMIS list size vs OLIDS Regular registrations.

Acceptance Criteria: <1% variance in aggregate across all 175 practices
Snapshot Date: 04/11/2025 (from EMIS extract)

Purpose:
- Validate overall data quality at aggregate level
- Complement per-practice validation
- Monitor system-wide accuracy
*/

with aggregate_counts as (
    select
        count(distinct practice_code) as practice_count,
        sum(emis_list_size) as total_emis_list_size,
        sum(olids_regular_count) as total_olids_regular_count,
        max(extract_date) as snapshot_date
    from {{ ref('int_emis_olids_practice_registration_comparison') }}
    where emis_list_size is not null
)

select
    practice_count,
    total_emis_list_size,
    total_olids_regular_count,

    -- Calculate aggregate difference (signed)
    total_olids_regular_count - total_emis_list_size as aggregate_difference,

    -- Calculate aggregate percentage difference
    round(
        (total_olids_regular_count - total_emis_list_size) * 100.0 / total_emis_list_size,
        4
    ) as aggregate_percent_difference,

    -- Calculate absolute aggregate percentage difference
    abs(round(
        (total_olids_regular_count - total_emis_list_size) * 100.0 / total_emis_list_size,
        4
    )) as aggregate_absolute_percent_difference,

    -- Acceptance criteria: <1% variance
    case
        when abs(round(
            (total_olids_regular_count - total_emis_list_size) * 100.0 / total_emis_list_size,
            4
        )) < 1 then 'PASS'
        else 'FAIL'
    end as aggregate_acceptance_status,

    snapshot_date,
    current_timestamp() as validation_timestamp

from aggregate_counts
