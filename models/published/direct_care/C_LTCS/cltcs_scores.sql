with cohort_data as (
    select patient_id,
        area_code
    from {{ ref('cohort_data') }}
),
activation_score as (
    select patient_id,
        area_code,
        score_activation
    from {{ ref('cltcs_score_activation') }}
),
coordination_score as (
    select patient_id,
        area_code,
        score_coordination
    from {{ ref('cltcs_score_coordination') }}
),
treatment_score as (
    select patient_id,
        area_code,
        score_treatment
    from {{ ref('cltcs_score_treatment') }}
),
final_score as (
    select cohort_data.patient_id,
        cohort_data.area_code,
        score_activation,
        score_coordination,
        score_treatment
        -- TO DO: normalise scores and add final score calculation
    from cohort_data
    left join activation_score  on cohort_data.patient_id = activation_score.patient_id
    left join coordination_score on cohort_data.patient_id = coordination_score.patient_id
    left join treatment_score on cohort_data.patient_id = treatment_score.patient_id
)
select * from final_score