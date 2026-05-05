with inclusion_list as (
    select patient_id,
        area_code
    from {{ ref('cltcs_full_detailed_patient_list') }}
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
frailty_score as (
    select patient_id,
        area_code,
        score_frailty
    from {{ ref('cltcs_score_frailty') }}
),

final_score as (
    select il.patient_id,
        il.area_code,
        score_activation,
        score_coordination,
        score_treatment,
        score_frailty
        -- TO DO: normalise scores and add final score calculation
    from inclusion_list il
    left join activation_score  on il.patient_id = activation_score.patient_id
    left join coordination_score on il.patient_id = coordination_score.patient_id
    left join treatment_score on il.patient_id = treatment_score.patient_id
    left join frailty_score on il.patient_id = frailty_score.patient_id
)
select * from final_score