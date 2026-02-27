{{
    config(
        materialized='table')
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Details LTCS summary data for patients with complex needs in C-LTCS

*/
{% set measurement_cutoff = -2 %}
with inclusion_list as (
    select *
    from {{ ref('cltcs_full_detailed_patient_list')}}
),
sus_ae_events as(
    select 
        il.patient_id,
        il.pcn_code,
        sus_events.start_date as event_start_date,
        sus_events.end_date as event_end_date,
        sus_events.visit_occurrence_type as event_type,
        sus_events.pod as event_detail,
        sus_events.visit_occurrence_id::varchar as event_id
    from {{ ref('int_sus_ae_encounters')}} sus_events 
    inner join inclusion_list il on il.patient_id = sus_events.sk_patient_id
    where sus_events.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
),
sus_apc_events as(
    select 
        il.patient_id,
        il.pcn_code,
        sus_events.start_date as event_start_date,
        sus_events.end_date as event_end_date,
        sus_events.visit_occurrence_type as event_type,
        admission_method_name as event_detail,
        sus_events.visit_occurrence_id::varchar as event_id
    from {{ ref('int_sus_ip_encounters')}} sus_events 
    inner join inclusion_list il on il.patient_id = sus_events.sk_patient_id
    where sus_events.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
),
sus_op_events as(
    select 
        il.patient_id,
        il.pcn_code,
        sus_events.start_date as event_start_date,
        sus_events.start_date as event_end_date,
        sus_events.visit_occurrence_type as event_type,
        sus_events.pod as event_detail,
        sus_events.visit_occurrence_id::varchar as event_id
    from {{ ref('int_sus_op_encounters')}} sus_events 
    inner join inclusion_list il on il.patient_id = sus_events.sk_patient_id
    where sus_events.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
),
gp_events as (
    select 
        il.patient_id,
        il.pcn_code,
        gpa.start_date as event_start_date,
        gpa.start_date as event_end_date,
        'GP_APPT' as event_type,
        gpa.national_slot_category_name as event_detail,
        gpa.encounter_id::varchar as event_id
    from {{ ref('int_gp_encounters_appt') }} gpa
    inner join inclusion_list il on il.olids_id = gpa.person_id
    where gpa.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and gpa.code not in ('3', '0')
), 

complete_events as (
    select * from sus_ae_events
    union all
    select * from sus_apc_events
    union all
    select * from sus_op_events
    union all
    select * from gp_events
  )

select patient_id, 
    pcn_code, 
    event_start_date,
    event_end_date,
    event_type,
    event_detail,
    event_id,
from complete_events 