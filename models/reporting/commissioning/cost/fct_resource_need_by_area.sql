-- Resource vs need summarised by registered geography (sub-ICB x borough x
-- neighbourhood) for the WNL exposure population. Convenience aggregate for
-- the Aligning Resource to Need pack and dashboards: maps and borough tables.
-- Roll up to borough / sub-ICB by summing actual + expected (never average the
-- index - it is sum(actual) / sum(expected)). Per-head measures use registered
-- person-years, not raw headcount, since exposure varies (deaths, deductions).
{{ config(materialized = 'table') }}

select
    registered_sub_icb_code,
    registered_sub_icb_name,
    registered_borough,
    registered_neighbourhood_name,
    count(*)                              as population,
    sum(months_registered) / 12           as person_years,
    count_if(died_in_window)              as died_in_window,
    sum(actual_cost_12m)                  as actual_cost_12m,
    sum(expected_cost_12m)                as expected_cost_12m,
    div0(sum(actual_cost_12m), sum(expected_cost_12m)) as resource_to_need_index,
    div0(sum(actual_cost_12m),   sum(months_registered) / 12) as cost_per_person_year,
    div0(sum(expected_cost_12m), sum(months_registered) / 12) as expected_per_person_year
from {{ ref('fct_person_resource_need') }}
group by 1, 2, 3, 4
