{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'emis', 'olids', 'historical']
    )
}}

/*
EMIS/OLIDS Practice Registration Comparison (2021)

Compares OLIDS Regular registration counts against EMIS list size for the
01/04/2021 historical extract.
*/

with emis_registrations as (
    select
        practice_code,
        practice_name,
        borough,
        list_size as emis_list_size,
        extract_date
    from {{ ref('stg_emis_list_size_2021') }}
),

olids_regular_counts as (
    select
        practice_ods_code as practice_code,
        regular_registered_patients as olids_regular_count,
        snapshot_date
    from {{ ref('int_olids_regular_registrations_at_point_in_time_2021') }}
),

comparison as (
    select
        coalesce(e.practice_code, o.practice_code) as practice_code,
        e.practice_name,
        e.borough,
        coalesce(e.emis_list_size, 0) as emis_list_size,
        coalesce(o.olids_regular_count, 0) as olids_regular_count,
        e.extract_date,
        coalesce(o.olids_regular_count, 0) - coalesce(e.emis_list_size, 0) as difference,
        abs(coalesce(o.olids_regular_count, 0) - coalesce(e.emis_list_size, 0)) as absolute_difference,
        case
            when e.emis_list_size = 0 or e.emis_list_size is null then null
            else round(
                (coalesce(o.olids_regular_count, 0) - e.emis_list_size) * 100.0 / e.emis_list_size,
                2
            )
        end as percent_difference,
        case
            when e.emis_list_size = 0 or e.emis_list_size is null then null
            else abs(round(
                (coalesce(o.olids_regular_count, 0) - e.emis_list_size) * 100.0 / e.emis_list_size,
                2
            ))
        end as absolute_percent_difference
    from emis_registrations as e
    full outer join olids_regular_counts as o
        on e.practice_code = o.practice_code
)

select
    practice_code,
    practice_name,
    borough,
    emis_list_size,
    olids_regular_count,
    difference,
    absolute_difference,
    percent_difference,
    absolute_percent_difference,
    extract_date,
    case
        when emis_list_size is null or emis_list_size = 0 then false
        when absolute_percent_difference < 2 then true
        when absolute_difference < 5 then true
        else false
    end as meets_acceptance_criteria,
    case
        when emis_list_size is null or emis_list_size = 0 then 'Missing Data'
        when absolute_percent_difference < 2 or absolute_difference < 5 then 'Meets Criteria'
        when absolute_percent_difference < 5 then '2-5% Variance'
        when absolute_percent_difference < 20 then '5-20% Variance'
        else '20%+ Variance'
    end as variance_category,
    'EMIS Regular Registration Comparison (2021): <2% variance OR <5 persons difference (per-practice); <1% aggregate variance (all practices)' as validation_methodology
from comparison
order by
    case variance_category
        when '20%+ Variance' then 1
        when '5-20% Variance' then 2
        when '2-5% Variance' then 3
        when 'Meets Criteria' then 4
        else 5
    end,
    absolute_percent_difference desc nulls last,
    practice_code
