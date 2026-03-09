          
with encoding_features as(
    select patient_id,
        area_code,
        total_qof_conditions,
        ae_tot_12mo,
        gp_att_tot_12mo,
        unique_active_ingredient_count_12mo,
        age,
        smoking_risk_sort_key,
        bmi_risk_sort_key,
        alcohol_risk_sort_key,
        case when asthma_testing_no_diagnosis = TRUE then 1 else 0 end as asthma_testing_no_diagnosis_flag,
        case when asthma_diagnosis_no_testing = TRUE then 1 else 0 end as asthma_diagnosis_no_testing_flag,
        case when asthma_diagnosis_no_act = TRUE then 1 else 0 end as asthma_diagnosis_no_act_flag,
        case when asthma_salbutamol_only = TRUE then 1 else 0 end as asthma_salbutamol_only_flag,
        case when asthma_salbutamol_repeats = TRUE then 1 else 0 end as asthma_salbutamol_repeats_flag,
        case when is_overall_bp_controlled = FALSE then 1 else 0 end as is_overall_bp_controlled_flag,
      from {{ ref('cohort_data') }}
)

select
    patient_id,
    area_code,
    smoking_risk_sort_key + bmi_risk_sort_key + alcohol_risk_sort_key + total_qof_conditions + ae_tot_12mo - gp_att_tot_12mo*2 + unique_active_ingredient_count_12mo/2 - age/5 + asthma_testing_no_diagnosis_flag + asthma_diagnosis_no_testing_flag + asthma_diagnosis_no_act_flag + asthma_salbutamol_only_flag*5 + asthma_salbutamol_repeats_flag*5 + is_overall_bp_controlled_flag*5 as score_treatment
from encoding_features