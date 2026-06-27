-- Resource vs need by residence ward (2025) for the registered WNL population.
-- Drives the ward choropleths in the Aligning Resource to Need pack.
--   weighted_population      = age-sex-weighted population (expected / WNL mean)
--   spend_per_weighted_head  = actual / weighted population = index x WNL mean,
--                              i.e. spend per age-weighted head
--   resource_to_need_index   = actual / expected
{{ config(materialized = 'table') }}

with base as (
    select
        residence_borough,
        residence_ward_2025_name as ward,
        count(*)               as population,
        sum(actual_cost_12m)   as actual_cost_12m,
        sum(expected_cost_12m) as expected_cost_12m
    from {{ ref('fct_person_resource_need') }}
    where residence_ward_2025_name is not null
    group by 1, 2
),

overall as (
    select sum(actual_cost_12m) / sum(population) as mean_cost_per_head
    from base
)

select
    b.residence_borough,
    b.ward,
    b.population,
    b.actual_cost_12m,
    b.expected_cost_12m,
    div0(b.actual_cost_12m, b.population)       as cost_per_head,
    div0(b.expected_cost_12m, o.mean_cost_per_head) as weighted_population,
    div0(b.actual_cost_12m, b.expected_cost_12m) as resource_to_need_index,
    o.mean_cost_per_head * div0(b.actual_cost_12m, b.expected_cost_12m) as spend_per_weighted_head
from base as b
cross join overall as o
