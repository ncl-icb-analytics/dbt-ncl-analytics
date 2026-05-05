          
with inclusion_list as (
    select *
    from {{ ref('cltcs_full_detailed_patient_list')}}
    ), 
    
encoding_features as(
    select il.patient_id
        , il.area_code
        , pc.total_qof_conditions
        , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(rm.unique_active_ingredient_count_12mo) as unique_active_ingredient_count_12mo
        , zeroifnull(ccms.cambridge_comorbidity_score) as cambridge_comorbidity_score
        , pd.age
        , zeroifnull(br.smoking_risk_sort_key) as smoking_risk_sort_key
        , zeroifnull(br.bmi_risk_sort_key) as bmi_risk_sort_key
        , zeroifnull(br.alcohol_risk_sort_key) as alcohol_risk_sort_key
        , case when am.testing_no_diagnosis = TRUE then 1 else 0 end as asthma_testing_no_diagnosis_flag
        , case when am.diagnosis_no_testing = TRUE then 1 else 0 end as asthma_diagnosis_no_testing_flag
        , case when am.diagnosis_no_act = TRUE then 1 else 0 end as asthma_diagnosis_no_act_flag
        , case when  am.salbutamol_only= TRUE then 1 else 0 end as asthma_salbutamol_only_flag
        , case when  am.salbutamol_repeats  = TRUE then 1 else 0 end as asthma_salbutamol_repeats_flag
        , case when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date() then bp.is_overall_bp_controlled else null end as is_recent_bp_controlled -- assuming bp control only relevant if recent, replace with more nuanced logic that ascerts likely control given redings history and time
        , case when is_recent_bp_controlled = FALSE then 1 else 0 end as is_overall_bp_controlled_flag
        , case when lcs.chd_risk_group = 'HRC' then 4 when lcs.chd_risk_group = 'HR' then 3 when lcs.chd_risk_group = 'MR' then 2 when lcs.chd_risk_group = 'LR' then 1 else null end as chd_risk_group_sort_key
        , case when lcs.ckd_risk_group = 'HRC' then 4 when lcs.ckd_risk_group = 'HR' then 3 when lcs.ckd_risk_group = 'MR' then 2 when lcs.ckd_risk_group = 'LR' then 1 else null end as ckd_risk_group_sort_key
        , case when lcs.copd_risk_group = 'HRC' then 4 when lcs.copd_risk_group = 'HR' then 3 when lcs.copd_risk_group = 'MR' then 2 when lcs.copd_risk_group = 'LR' then 1 else null end as copd_risk_group_sort_key
        , case when lcs.diabetes_risk_group = 'HRC' then 4 when lcs.diabetes_risk_group = 'HR' then 3 when lcs.diabetes_risk_group = 'MR' then 2 when lcs.diabetes_risk_group = 'LR' then 1 else null end as diabetes_risk_group_sort_key
        , case when lcs.hf_risk_group = 'HR' then 2 when lcs.hf_risk_group = 'MR' then 1 else null end as hf_risk_group_sort_key
        , case when lcs.hypertension_risk_group = 'HRC' then 4 when lcs.hypertension_risk_group = 'HR' then 3 when lcs.hypertension_risk_group = 'MR' then 2 when lcs.hypertension_risk_group = 'LR' then 1 else null end as hypertension_risk_group_sort_key
        , case when lcs.overall_risk_group = 'HRC' then 4 when lcs.overall_risk_group = 'HR' then 3 when lcs.overall_risk_group = 'MR' then 2 when lcs.overall_risk_group = 'LR' then 1 else null end as overall_risk_group_sort_key
        , lcs.moc_stage_completed
      from inclusion_list il
    left join {{ref('dim_person_conditions')}} pc
        on il.olids_id = pc.person_id
    left join {{ref('fct_person_sus_ae_recent')}} aea
        on il.patient_id  = aea.sk_patient_id
    left join {{ref('fct_person_gp_recent')}} gpa
        on il.patient_id  = gpa.sk_patient_id
    left join {{ref('fct_person_medications_recent')}} rm
        on il.olids_id = rm.person_id
    left join {{ref('dim_person_demographics')}} pd
        on il.olids_id = pd.person_id
    left join {{ref('fct_person_behavioural_risk_factors')}} br
        on il.olids_id = br.person_id
    left join {{ref('int_asthma_management')}} am
        on il.olids_id = am.person_id
    left join {{ref('fct_person_bp_control')}} bp
        on il.olids_id = bp.person_id
    left join {{ref('stg_aic_int_ccms_current')}} ccms
        on il.olids_id = ccms.person_id
    left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
        on il.olids_id = lcs.person_id
)

select
    patient_id,
    area_code,
    ( 
        smoking_risk_sort_key 
    + bmi_risk_sort_key 
    + alcohol_risk_sort_key
    + cambridge_comorbidity_score * 2
    + total_qof_conditions 
    + ae_tot_12mo 
    - gp_att_tot_12mo*2 
    + unique_active_ingredient_count_12mo/2 
    - age/5 
    + asthma_testing_no_diagnosis_flag 
    + asthma_diagnosis_no_testing_flag 
    + asthma_diagnosis_no_act_flag 
    + asthma_salbutamol_only_flag*5 
    + asthma_salbutamol_repeats_flag*5 
    + is_overall_bp_controlled_flag*5  
    + zeroifnull(chd_risk_group_sort_key)
    + zeroifnull(ckd_risk_group_sort_key)
    + zeroifnull(copd_risk_group_sort_key)
    + zeroifnull(diabetes_risk_group_sort_key)
    + zeroifnull(hf_risk_group_sort_key)
    + zeroifnull(hypertension_risk_group_sort_key)
    + zeroifnull(overall_risk_group_sort_key)
    - zeroifnull(moc_stage_completed) 
        )as score_treatment
from encoding_features