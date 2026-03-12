          
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
        , pd.age
        , zeroifnull(br.smoking_risk_sort_key) as smoking_risk_sort_key
        , zeroifnull(br.bmi_risk_sort_key) as bmi_risk_sort_key
        , zeroifnull(br.alcohol_risk_sort_key) as alcohol_risk_sort_key
        , zeroifnull(br.alcohol_risk_sort_key) as alcohol_risk_sort_key
        , case when am.testing_no_diagnosis = TRUE then 1 else 0 end as asthma_testing_no_diagnosis_flag
        , case when am.diagnosis_no_testing = TRUE then 1 else 0 end as asthma_diagnosis_no_testing_flag
        , case when am.diagnosis_no_act = TRUE then 1 else 0 end as asthma_diagnosis_no_act_flag
        , case when  am.salbutamol_only= TRUE then 1 else 0 end as asthma_salbutamol_only_flag
        , case when  am.salbutamol_repeats  = TRUE then 1 else 0 end as asthma_salbutamol_repeats_flag
        , case when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date() then bp.is_overall_bp_controlled else null end as is_recent_bp_controlled -- assuming bp control only relevant if recent, replace with more nuanced logic that ascerts likely control given redings history and time
        , case when is_recent_bp_controlled = FALSE then 1 else 0 end as is_overall_bp_controlled_flag
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
)

select
    patient_id,
    area_code,
    smoking_risk_sort_key + bmi_risk_sort_key + alcohol_risk_sort_key + total_qof_conditions + ae_tot_12mo - gp_att_tot_12mo*2 + unique_active_ingredient_count_12mo/2 - age/5 + asthma_testing_no_diagnosis_flag + asthma_diagnosis_no_testing_flag + asthma_diagnosis_no_act_flag + asthma_salbutamol_only_flag*5 + asthma_salbutamol_repeats_flag*5 + is_overall_bp_controlled_flag*5 as score_treatment
from encoding_features