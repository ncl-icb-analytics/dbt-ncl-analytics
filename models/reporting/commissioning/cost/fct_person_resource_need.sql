/*
Resource vs need at person level — indirect age-sex standardisation on a
person-time basis.

Population: everyone with WNL registration exposure in the rolling 12-month
cost window (int_person_registration_exposure) — including the deceased and
the deducted, whose in-window spend the old current-registration filter
dropped (~17% of in-patch patient-keyed spend). One row per patient.

  * actual_cost_12m   - patient-attributable spend over the window
    (int_person_cost_12m)
  * expected_cost_12m - WNL age-sex cost-per-month rate for the patient's
    band (int_resource_need_age_sex_curve) x their months registered

resource_to_need = sum(actual) / sum(expected) for any cut (borough,
neighbourhood, practice, sub-ICB, age band, sex, ethnicity, IMD, decedents).
Index > 1 = spend above what the age-sex profile predicts at WNL-average
rates; < 1 = below. Deprivation / morbidity are exposed as cut dimensions,
not folded into the weight.

Registered geography attributes via the *window* practice (last registered
month), so leavers and decedents attribute to where they were registered.
Age at window end (at death for decedents); missing age/gender carry an
'Unknown' band matching the curve so their spend still standardises.
*/

{{ config(materialized = 'table') }}

with population as (
    select
        e.sk_patient_id,
        e.months_registered,
        e.died_in_window,
        e.date_of_death,
        e.practice_code,
        coalesce(p.gender, 'Unknown') as gender,
        p.ethnicity,
        p.residence_imd_decile,
        p.residence_borough,
        p.residence_ward_2025_name,
        p.residence_lsoa_2021_code,
        {{ calculate_age_attributes(
            'p.date_of_birth',
            'e.window_end_month',
            is_deceased_field='e.date_of_death is not null',
            death_date_field='e.date_of_death'
        ) }}
    from {{ ref('int_person_registration_exposure') }} as e
    left join {{ ref('dim_person_demographics_basic') }} as p
        on e.sk_patient_id = p.sk_patient_id
)

select
    pop.sk_patient_id,
    pop.gender,
    pop.age,
    coalesce(pop.age_band_nhs, 'Unknown') as age_band,
    pop.ethnicity,
    pop.months_registered,
    pop.died_in_window,
    pop.practice_code,
    coalesce(org_bor.borough_registered, 'Unknown') as registered_borough,
    coalesce(nb_reg.neighbourhood_name, 'Unknown')  as registered_neighbourhood_name,
    org_bor.sub_icb_code                            as registered_sub_icb_code,
    org_bor.sub_icb_name                            as registered_sub_icb_name,
    pop.residence_imd_decile,
    pop.residence_borough,
    pop.residence_ward_2025_name,
    pop.residence_lsoa_2021_code,
    coalesce(cost.actual_cost_12m, 0)                       as actual_cost_12m,
    curve.expected_cost_per_month * pop.months_registered   as expected_cost_12m
from population as pop
left join {{ ref('int_organisation_borough_mapping') }} as org_bor
    on pop.practice_code = org_bor.practice_code
left join {{ ref('stg_reference_wnl_gp_practice_neighbourhood') }} as nb_reg
    on pop.practice_code = nb_reg.practice_code
left join {{ ref('int_person_cost_12m') }} as cost
    on pop.sk_patient_id = cost.sk_patient_id
left join {{ ref('int_resource_need_age_sex_curve') }} as curve
    on coalesce(pop.age_band_nhs, 'Unknown') = curve.age_band
    and pop.gender = curve.gender
