{{
    config(
        materialized='table')
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Details LTCS summary data for patients with complex needs in C-LTCS

*/
{% set measurement_cutoff = -5 %}
with inclusion_list as (
    select olids_id, patient_id,pcn_code
    from {{ ref('inclusion_cohort')}}
    where eligible = 1
),
hba1c_measurements as(
    select il.patient_id,
        il.pcn_code,
        hb.clinical_effective_date,
        hb.id as measurement_id,
        hb.hba1c_ifcc as value,
        hb.hba1c_category as category,
        'hba1c' as measurement_type
    from {{ ref('int_hba1c_all')}} hb 
    inner join inclusion_list il on il.olids_id = hb.person_id
    where hb.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        case when hb.is_ifcc then 1 when hb.is_dcct then 2 else 3 end = 
        min(case when hb.is_ifcc then 1 when hb.is_dcct then 2 else 3 end) 
            over (partition by il.patient_id, hb.clinical_effective_date)
        and row_number() over (
            partition by il.patient_id, hb.clinical_effective_date, hb.hba1c_ifcc
            order by measurement_id
        ) = 1
),
blood_pressure_measurements_systolic as(
    select il.patient_id,
        il.pcn_code,
        bp.clinical_effective_date,
        bp.systolic_observation_id as measurement_id,
        bp.systolic_value as value,
        case when bp.is_home_bp_event then 'HOME' when bp.is_abpm_bp_event then 'ABPM' else 'CLINIC' end as category,
        'blood_pressure_systolic' as measurement_type,
    from {{ ref('int_blood_pressure_all')}} bp 
    inner join inclusion_list il on il.olids_id = bp.person_id
    where bp.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over ( -- consider ranking by type of reading if which ever category is arbitrarily taken influences the clinican's decision
            partition by il.patient_id, bp.clinical_effective_date, bp.systolic_value
            order by measurement_id
        ) = 1
),
blood_pressure_measurements_diastolic as(
    select il.patient_id,
        il.pcn_code,
        bp.clinical_effective_date,
        bp.diastolic_observation_id as measurement_id,
        bp.diastolic_value as value,
        case when bp.is_home_bp_event then 'HOME' when bp.is_abpm_bp_event then 'ABPM' else 'CLINIC' end as category,
        'blood_pressure_diastolic' as measurement_type,
    from {{ ref('int_blood_pressure_all')}} bp 
    inner join inclusion_list il on il.olids_id = bp.person_id
    where bp.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over ( -- consider ranking by type of reading if which ever category is arbitrarily taken influences the clinican's decision
            partition by il.patient_id, bp.clinical_effective_date, bp.diastolic_value
            order by measurement_id
        ) = 1
),
egfr_measurements as(
    select il.patient_id,
        il.pcn_code,
        egfr.clinical_effective_date,
        egfr.id as measurement_id,
        egfr.egfr_value as value,
        egfr.ckd_stage as category,
        'egfr' as measurement_type,
    from {{ ref('int_egfr_all')}} egfr 
    inner join inclusion_list il on il.olids_id = egfr.person_id
    where egfr.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over (
            partition by il.patient_id, egfr.clinical_effective_date, egfr.egfr_value
            order by measurement_id
        ) = 1
),
urine_acr_measurements as(
    select il.patient_id,
        il.pcn_code,
        acr.clinical_effective_date,
        acr.id as measurement_id,
        acr.acr_value as value,
        acr.acr_category as category,
        'urine_acr' as measurement_type,
    from {{ ref('int_urine_acr_all')}} acr 
    inner join inclusion_list il on il.olids_id = acr.person_id
    where acr.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over (
            partition by il.patient_id, acr.clinical_effective_date, acr.acr_value
            order by measurement_id
        ) = 1
),
total_cholesterol_measurements as(
    select il.patient_id,
        il.pcn_code,
        cholesterol.clinical_effective_date,
        cholesterol.id as measurement_id,
        cholesterol.cholesterol_value as value,
        cholesterol.cholesterol_category as category,
        'total_cholesterol' as measurement_type,
    from {{ ref('int_cholesterol_all')}} cholesterol 
    inner join inclusion_list il on il.olids_id = cholesterol.person_id
    where cholesterol.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over (
            partition by il.patient_id, cholesterol.clinical_effective_date, cholesterol.cholesterol_value
            order by measurement_id
        ) = 1
),
ldl_cholesterol_measurements as(
    select il.patient_id,
        il.pcn_code,
        ldl.clinical_effective_date,
        ldl.id as measurement_id,
        ldl.cholesterol_value as value,
        ldl.ldl_cvd_target_met as category,
        'ldl_cholesterol' as measurement_type,
    from {{ ref('int_cholesterol_ldl_all')}} ldl 
    inner join inclusion_list il on il.olids_id = ldl.person_id
    where ldl.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over (
            partition by il.patient_id, ldl.clinical_effective_date, ldl.cholesterol_value
            order by measurement_id
        ) = 1
)

select * from hba1c_measurements
union all
select * from blood_pressure_measurements_systolic
union all
select * from blood_pressure_measurements_diastolic
union all
select * from egfr_measurements
union all
select * from urine_acr_measurements
union all
select * from total_cholesterol_measurements
union all
select * from ldl_cholesterol_measurements
-- union with other measurements