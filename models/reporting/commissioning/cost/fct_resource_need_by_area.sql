-- Resource vs need summarised by registered geography (sub-ICB x borough x
-- neighbourhood) for the registered WNL population. Convenience aggregate for
-- the Aligning Resource to Need pack and dashboards: maps and borough tables.
-- Roll up to borough / sub-ICB by summing actual + expected (never average the
-- index - it is sum(actual) / sum(expected)).
{{ config(materialized = 'table') }}

select
    registered_sub_icb_code,
    registered_sub_icb_name,
    registered_borough,
    registered_neighbourhood_name,
    count(*)                              as population,
    sum(actual_cost_12m)                  as actual_cost_12m,
    sum(expected_cost_12m)                as expected_cost_12m,
    div0(sum(actual_cost_12m), sum(expected_cost_12m)) as resource_to_need_index,
    sum(actual_cost_12m)   / count(*)     as cost_per_head,
    sum(expected_cost_12m) / count(*)     as expected_per_head
from {{ ref('fct_person_resource_need') }}
group by 1, 2, 3, 4
