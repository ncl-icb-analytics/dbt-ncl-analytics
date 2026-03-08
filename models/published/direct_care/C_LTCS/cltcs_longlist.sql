{{
    config(
        materialized='table')
}}


/*
Patient view for CLTCS longlisting in app

Each variable listed explicitly to allow data quality handling steps per column.
*/
select
    ------ identifiers and geography
    cohort_data.patient_id,
    cohort_data.area_code,
    cohort_data.area_name,
    cohort_data.practice_code,
    cohort_data.practice_name,
    ------ demographics
    cohort_data.age,
    cohort_data.main_language,
    cohort_data.gender,
    cohort_data.ethnicity_category,
    cohort_data.main_language_flag,
    ------ trajectories
    cohort_data.ae_encounters_sl,
    cohort_data.ip_encounters_sl,
    cohort_data.op_encounters_sl,
    cohort_data.gp_encounters_sl,
    ------ disease registries and condition counts
    -- cohort_data.has_atrial_fibrillation,
    cohort_data.has_asthma,
    -- cohort_data.has_cancer,
    -- cohort_data.has_coronary_heart_disease,
    -- cohort_data.has_chronic_kidney_disease,
    -- cohort_data.has_copd,
    -- cohort_data.has_cyp_asthma,
    -- cohort_data.has_dementia,
    -- cohort_data.has_depression,
    -- cohort_data.has_diabetes,
    -- cohort_data.has_epilepsy,
    -- cohort_data.has_familial_hypercholesterolaemia,
    -- cohort_data.has_gestational_diabetes,
    -- cohort_data.has_frailty,
    -- cohort_data.has_heart_failure,
    -- cohort_data.has_hypertension,
    cohort_data.has_learning_disability,
    -- cohort_data.has_learning_disability_under_14,
    -- cohort_data.has_nafld,
    -- cohort_data.has_non_diabetic_hyperglycaemia,
    -- cohort_data.has_obesity,
    -- cohort_data.has_osteoporosis,
    -- cohort_data.has_peripheral_arterial_disease,
    -- cohort_data.has_palliative_care,
    -- cohort_data.has_rheumatoid_arthritis,
    cohort_data.has_severe_mental_illness,
    -- cohort_data.has_stroke_tia,
    -- cohort_data.total_conditions,
    cohort_data.total_qof_conditions,
    -- cohort_data.total_non_qof_conditions,
    -- cohort_data.cardiovascular_conditions,
    -- cohort_data.respiratory_conditions,
    -- cohort_data.mental_health_conditions,
    -- cohort_data.metabolic_conditions,
    cohort_data.musculoskeletal_conditions,
    -- cohort_data.neurology_conditions,
    -- cohort_data.geriatric_conditions,
    ------- lifestyle and behavioural factors
    cohort_data.smoking_status,
    cohort_data.smoking_risk_sort_key,
    cohort_data.bmi_category,
    cohort_data.bmi_value,
    cohort_data.bmi_risk_sort_key,
    cohort_data.alcohol_status,
    cohort_data.alcohol_risk_sort_key,
    ------- current status
    --cohort_data.is_currently_pregnant,
    -- asthma management flags
    cohort_data.asthma_testing_no_diagnosis,
    cohort_data.asthma_diagnosis_no_testing,
    cohort_data.asthma_diagnosis_no_act,
    cohort_data.asthma_salbutamol_only,
    cohort_data.asthma_salbutamol_repeats,
    -- measurement and care process
    cohort_data.is_overall_bp_controlled,
    cohort_data.is_most_recent_overall_bp_controlled,
    cohort_data.latest_systolic_value,
    cohort_data.latest_diastolic_value,
    cohort_data.latest_bp_date,
    cohort_data.care_processes_completed,
    cohort_data.latest_hba1c_value,
    cohort_data.latest_hba1c_date,
    -- annual activity
    cohort_data.op_att_tot_12mo,
    cohort_data.op_spec_12mo,
    cohort_data.op_prov_12mo,
    cohort_data.op_predicted,
    cohort_data.op_oe_ratio,
    cohort_data.apc_12mo,
    cohort_data.apc_los_12mo,
    cohort_data.ae_t1_12mo,
    cohort_data.ae_inj_12mo,
    cohort_data.ae_tot_12mo,
    cohort_data.gp_att_tot_12mo,
    cohort_data.gp_app_tot_12mo,
    cohort_data.gp_dna_tot_12mo,
    -- waiting list
    cohort_data.wl_total_count,
    cohort_data.wl_provider_count,
    cohort_data.wl_specialty_count,
    cohort_data.has_same_tfc_multiple_providers_flag,
    cohort_data.current_waiting_list_arrays,
    -- polypharmacy and medications
    cohort_data.medication_count,
    cohort_data.medication_name_list,
    cohort_data.is_polypharmacy_5plus,
    cohort_data.attendance_difficulty_score,
    cohort_data.medications_recent_12mo,
    cohort_data.unique_active_ingredient_count_12mo,
    -- scores (from join)
    cltcs_scores.score_activation,
    cltcs_scores.score_coordination,
    cltcs_scores.score_treatment
from {{ ref('cohort_data') }} cohort_data
left join {{ ref('cltcs_scores') }} cltcs_scores
    on cohort_data.patient_id = cltcs_scores.patient_id
