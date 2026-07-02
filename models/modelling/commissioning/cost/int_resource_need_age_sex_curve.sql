-- Empirical WNL age-sex cost curve on a person-time basis: patient-attributable
-- cost per registered month by NHS age band and gender, over everyone with WNL
-- registration exposure in the window (int_person_registration_exposure) --
-- including non-users (cost 0), the deceased and the deducted. These are the
-- "need weights" used for indirect standardisation in fct_person_resource_need,
-- derived from our own spend (no external / flaky weighted-list-size feed).
-- Age is at the window end, or at death for decedents; missing age/gender
-- fall into an 'Unknown' band so their spend still standardises.
{{ config(materialized = 'table') }}

with population as (
    select
        e.sk_patient_id,
        e.months_registered,
        coalesce(p.gender, 'Unknown') as gender,
        {{ calculate_age_attributes(
            'p.date_of_birth',
            'e.window_end_month',
            is_deceased_field='e.date_of_death is not null',
            death_date_field='e.date_of_death'
        ) }}
    from {{ ref('int_person_registration_exposure') }} as e
    left join {{ ref('dim_person_demographics_basic') }} as p
        on e.sk_patient_id = p.sk_patient_id
),

pop_cost as (
    select
        coalesce(population.age_band_nhs, 'Unknown') as age_band,
        population.gender,
        population.months_registered,
        coalesce(c.actual_cost_12m, 0) as actual_cost_12m
    from population
    left join {{ ref('int_person_cost_12m') }} as c
        on population.sk_patient_id = c.sk_patient_id
)

select
    age_band,
    gender,
    count(*)                                     as population,
    sum(months_registered)                       as person_months,
    div0(sum(actual_cost_12m), sum(months_registered)) as expected_cost_per_month,
    12 * div0(sum(actual_cost_12m), sum(months_registered)) as expected_cost_per_head_12m
from pop_cost
group by 1, 2
