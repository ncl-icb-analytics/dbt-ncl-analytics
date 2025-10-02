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

*/

with inclusion_list as (
    select patient_id, olids_id
    from {{ ref('inclusion_cohort')}}
    where eligible = 1
)

select il.patient_id
    -- trajectories for sparkline visualisation
    , tr.ae_encounters_sl
    , tr.ip_encounters_sl
    , tr.op_encounters_sl
    -- local disease registries and counts
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
    , pc.has_frailty
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
    , pc.total_qof_conditions
    , pc.total_non_qof_conditions
    , pc.cardiovascular_conditions
    , pc.respiratory_conditions
    , pc.mental_health_conditions
    , pc.metabolic_conditions
    , pc.musculoskeletal_conditions
    , pc.neurology_conditions
    , pc.geriatric_conditions
from inclusion_list il
left join {{ ref('trajectories') }} tr
    on il.patient_id = tr.patient_id
left join {{ ref('dim_person_conditions')}} pc
    on il.olids_id = pc.person_id
