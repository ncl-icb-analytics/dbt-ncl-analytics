-- Empirical WNL age-sex cost curve: expected (average) patient-attributable
-- cost per head by NHS age band and gender, over the registered WNL population
-- including non-users (cost 0). These are the "need weights" used for indirect
-- standardisation in fct_person_resource_need - derived from our own spend, so
-- stable and reproducible (no external / flaky weighted-list-size feed).
{{ config(materialized = 'table') }}

with population as (
    select
        p.sk_patient_id,
        p.gender,
        {{ calculate_age_attributes('p.date_of_birth', 'current_date()') }}
    from {{ ref('dim_person_demographics_basic') }} as p
    where p.flag_current_registered
),

pop_cost as (
    select
        population.age_band_nhs as age_band,
        population.gender,
        coalesce(c.actual_cost_12m, 0) as actual_cost_12m
    from population
    left join {{ ref('int_person_cost_12m') }} as c
        on population.sk_patient_id = c.sk_patient_id
)

select
    age_band,
    gender,
    count(*)             as population,
    avg(actual_cost_12m) as expected_cost_per_head
from pop_cost
group by 1, 2
