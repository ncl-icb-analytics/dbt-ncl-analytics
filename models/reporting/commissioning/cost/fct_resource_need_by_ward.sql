-- Resource vs need by residence ward (2025) for the WNL exposure population.
-- Drives the ward choropleths in the Aligning Resource to Need pack.
--   person_years             = registered person-years in the window (exposure)
--   weighted_person_years    = age-sex-weighted exposure (expected / WNL mean rate)
--   spend_per_weighted_year  = actual / weighted exposure = index x WNL mean
--   resource_to_need_index   = actual / expected
-- Denominators are WNL-registered person-time by residence ward, NOT ONS ward
-- populations - boundary wards with many non-WNL-registered residents are
-- partial.
{{ config(materialized = 'table') }}

with base as (
    select
        residence_borough,
        residence_ward_2025_name as ward,
        count(*)                    as population,
        sum(months_registered) / 12 as person_years,
        sum(actual_cost_12m)        as actual_cost_12m,
        sum(expected_cost_12m)      as expected_cost_12m
    from {{ ref('fct_person_resource_need') }}
    where residence_ward_2025_name is not null
    group by 1, 2
),

overall as (
    select sum(actual_cost_12m) / sum(person_years) as mean_cost_per_person_year
    from base
)

select
    b.residence_borough,
    b.ward,
    b.population,
    b.person_years,
    b.actual_cost_12m,
    b.expected_cost_12m,
    div0(b.actual_cost_12m, b.person_years)              as cost_per_person_year,
    div0(b.expected_cost_12m, o.mean_cost_per_person_year) as weighted_person_years,
    div0(b.actual_cost_12m, b.expected_cost_12m)         as resource_to_need_index,
    o.mean_cost_per_person_year * div0(b.actual_cost_12m, b.expected_cost_12m) as spend_per_weighted_year
from base as b
cross join overall as o
