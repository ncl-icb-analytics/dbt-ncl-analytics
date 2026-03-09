
with encoding_features as(
    select patient_id,
        area_code,
        ae_tot_12mo,
        ae_tot_12mo + 10 as ae_tot_12mo_plus_10,
        gp_att_tot_12mo,
        gp_att_tot_12mo + 10 as gp_att_tot_12mo_plus_10,
        main_language_flag,
        main_language,
        op_oe_ratio,
        case when op_oe_ratio < 0.8 then 1 else 0 end as op_oe_ratio_flag,
        has_severe_mental_illness,
        case when ae_tot_12mo > gp_att_tot_12mo then 1 else 0 end as ae_greater_gp_12mo_flag,
        case when has_severe_mental_illness = TRUE then 1 else 0 end as has_severe_mental_illness_flag,
        has_learning_disability,
        case when has_learning_disability = TRUE then 1 else 0 end as has_learning_disability_flag,
        musculoskeletal_conditions,
        case when musculoskeletal_conditions = TRUE then 1 else 0 end as musculoskeletal_conditions_flag
    from {{ ref('cohort_data') }}
)
select patient_id,
    area_code,
    ae_greater_gp_12mo_flag*5+(ae_tot_12mo_plus_10/gp_att_tot_12mo_plus_10)/2 + main_language_flag*5 + has_severe_mental_illness_flag*10 + has_learning_disability_flag*10 + musculoskeletal_conditions_flag*10 + op_oe_ratio_flag*2 - gp_att_tot_12mo as score_activation
from encoding_features