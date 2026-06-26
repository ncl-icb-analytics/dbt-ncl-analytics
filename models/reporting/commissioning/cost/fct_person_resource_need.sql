/*
Resource vs need at person level — indirect age-sex standardisation.

Assembles, one row per current registered WNL patient:
  * actual_cost_12m   - patient-attributable spend over the latest 12 months
    (int_person_cost_12m)
  * expected_cost_12m - the WNL average cost per head for the patient's age-sex
    band (int_resource_need_age_sex_curve)

resource_to_need = sum(actual) / sum(expected) for any cut (borough,
neighbourhood, practice, sub-ICB, age band, sex, ethnicity, IMD). Index > 1 =
spend above what the age-sex profile predicts at WNL-average rates; < 1 = below.
Deprivation / morbidity are exposed as cut dimensions, not folded into the weight.
*/

{{ config(materialized = 'table', tags = ['daily']) }}

with population as (
    select
        p.sk_patient_id,
        p.gender,
        p.ethnicity,
        p.practice_code,
        p.registered_borough,
        p.registered_neighbourhood_name,
        p.registered_sub_icb_code,
        p.registered_sub_icb_name,
        p.residence_imd_decile,
        {{ calculate_age_attributes('p.date_of_birth', 'current_date()') }}
    from {{ ref('dim_person_demographics_basic') }} as p
    where p.flag_current_registered
)

select
    pop.sk_patient_id,
    pop.gender,
    pop.age,
    pop.age_band_nhs as age_band,
    pop.ethnicity,
    pop.practice_code,
    pop.registered_borough,
    pop.registered_neighbourhood_name,
    pop.registered_sub_icb_code,
    pop.registered_sub_icb_name,
    pop.residence_imd_decile,
    coalesce(cost.actual_cost_12m, 0) as actual_cost_12m,
    curve.expected_cost_per_head      as expected_cost_12m
from population as pop
left join {{ ref('int_person_cost_12m') }} as cost
    on pop.sk_patient_id = cost.sk_patient_id
left join {{ ref('int_resource_need_age_sex_curve') }} as curve
    on pop.age_band_nhs = curve.age_band
    and pop.gender = curve.gender
