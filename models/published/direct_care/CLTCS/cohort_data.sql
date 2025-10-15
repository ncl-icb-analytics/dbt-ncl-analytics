{{
    config(
        materialized='table')
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Supporting longlisting of patients with complex needs

Testing:
- Actual table have additional fields and pull from a wide array of model outputs
- Replace null handling and time spines with dbt functions in yml

*/

with inclusion_list as (
    select patient_id, olids_id, pcn_code, pcn_name, age
    from {{ ref('inclusion_cohort')}}
    where eligible = 1
)

select distinct il.patient_id
    , il.pcn_code
    , il.age
    -- trajectories for sparkline visualisation [add other domains - GP, Community, MH, total?]
    , tr.ae_encounters_sl
    , tr.ip_encounters_sl
    , tr.op_encounters_sl
    -- local disease registries and counts [ only of primary care for now, add acute/community etc data later ]
    , pc.has_atrial_fibrillation
    , pc.has_asthma
    , pc.has_cancer
    , pc.has_coronary_heart_disease
    , pc.has_chronic_kidney_disease
    , pc.has_copd
    , pc.has_cyp_asthma
    , pc.has_dementia
    , pc.has_depression
    , pc.has_diabetes
    , pc.has_epilepsy
    , pc.has_familial_hypercholesterolaemia
    , pc.has_gestational_diabetes
    , pc.has_frailty -- replace with ef2?
    , pc.has_heart_failure
    , pc.has_hypertension
    , pc.has_learning_disability
    , pc.has_learning_disability_all_ages
    , pc.has_nafld
    , pc.has_non_diabetic_hyperglycaemia
    , pc.has_obesity
    , pc.has_osteoporosis
    , pc.has_peripheral_arterial_disease
    , pc.has_palliative_care
    , pc.has_rheumatoid_arthritis
    , pc.has_severe_mental_illness
    , pc.has_stroke_tia
    , pc.total_conditions
    , pc.total_qof_conditions -- replace cambridge multimorbidity score/ similar complexity metric?
    , pc.total_non_qof_conditions
    , pc.cardiovascular_conditions
    , pc.respiratory_conditions
    , pc.mental_health_conditions
    , pc.metabolic_conditions
    , pc.musculoskeletal_conditions
    , pc.neurology_conditions
    , pc.geriatric_conditions
    -- Lifestyle and behavioural factors
    , br.smoking_status
    , br.smoking_risk_sort_key
    , br.bmi_category
    , br.bmi_value
    , br.bmi_risk_sort_key
    , br.alcohol_status
    , br.alcohol_risk_sort_key
    -- current status to consider 
    , ps.is_currently_pregnant 
    -- dim_person_is_carer?
    -- measurement flags (fully summaries elsewhere or held as array?)
    , case when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date() then bp.is_overall_bp_controlled else null end as is_overall_bp_controlled -- assuming bp control only relevant if recent, replace with more nuanced logic that ascerts likely control given redings history and time
    ,bp.is_overall_bp_controlled as is_most_recent_overall_bp_controlled
    ,bp.latest_systolic_value
    ,bp.latest_diastolic_value
    ,bp.latest_bp_date
    ,dcp.care_processes_completed
    , case when dcp.hba1c_completed_in_last_12m = true then dcp.latest_hba1c_value else null end as latest_hba1c_value
    ,dcp.latest_hba1c_date
    -- Annual activity (OP, APC, UEC, MH, ASC, Community, GP appts, 111?)
    ,zeroifnull(op.op_att_tot_12mo) as op_att_tot_12mo
    ,zeroifnull(op.op_spec_12mo) as op_spec_12mo
    ,zeroifnull(op.op_prov_12mo) as op_prov_12mo
    -- Current waiting list counts and flags
    ,zeroifnull(wl.wl_current_total_count) as wl_total_count
    ,zeroifnull(wl.wl_current_distinct_providers_count) as wl_provider_count
    ,zeroifnull(wl.wl_current_distinct_tfc_count) as wl_specialty_count
    ,wl.same_tfc_multiple_providers_flag as has_same_tfc_multiple_providers_flag

    -- polypharmacy, high risk drugs, suspected non-adherence

    -- Current referrals

    -- Current risk scores?

    -- Other relevant annual activity (LTC LCS, C-LTCS review)

from inclusion_list il
left join {{ ref('trajectories') }} tr
    on il.patient_id = tr.patient_id
left join {{ ref('dim_person_conditions')}} pc
    on il.olids_id = pc.person_id
left join {{ref('fct_person_behavioural_risk_factors')}} br
    on il.olids_id = br.person_id
left join {{ref('fct_person_pregnancy_status')}} ps
    on il.olids_id = ps.person_id
left join {{ref('fct_person_bp_control')}} bp
    on il.olids_id = bp.person_id 
left join {{ref('fct_person_diabetes_8_care_processes')}} dcp
    on il.olids_id = dcp.person_id
left join {{ref('fct_person_wl_current_count_total')}} wl
    on il.patient_id = wl.sk_patient_id
left join {{ref('fct_person_sus_op_recent')}} op
    on il.patient_id  = op.sk_patient_id

