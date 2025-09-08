{{
    config(
        materialized='view')
}}


/*
Recent ECDS activities from SUS

Clinical Purpose:
- Establishing demand for emergency care services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

{% set years_from_now = -1 %}

/* Establish event range */
with filtered_core as (
    select *
    from {{ ref('stg_sus_ae_emergency_care')}}
    where attendance_arrival_date between dateadd(year, {{years_from_now}}, current_date()) and current_date()
)


select 
    /* Information needed to derive standard event information */
    core.primarykey_id as event_id
    , core.patient_nhs_number_value_pseudo as sk_patient_id
    , core.attendance_location_site as location_id
    , core.attendance_arrival_date as start_date
    , core.attendance_departure_time_since_arrival as duration
    , core.clinical_chief_complaint_code as primary_reason_for_event
    , core.clinical_acuity_code as acuity
    , diagnosis.flat_diagnosis_codes
    , treatments.code as primary_treatment
    , investigations.code as primary_investigation
    , 'SUS_ECDS' as source
    , core.attendance_location_department_type as department_type
    , core.commissioning_national_pricing_final_price as cost

from filtered_core as core

/* Diagnosis code for infering reason */
left join (
    select diag.primarykey_id
    , listagg(diag.code, ', ') within group (order by diag.snomed_id) as flat_diagnosis_codes
    from {{ref('stg_sus_ae_clinical_diagnoses_snomed')}} diag
    inner join filtered_core fc on diag.primarykey_id = fc.primarykey_id
    group by diag.primarykey_id
) as diagnosis on core.primarykey_id = diagnosis.primarykey_id

/* First investigation code for infering reason */
left join {{ref('stg_sus_ae_clinical_investigations_snomed')}} as investigations
    on core.primarykey_id = investigations.primarykey_id 
    and investigations.snomed_id = 1

/* First treatment for infering reason  */
left join {{ref('stg_sus_ae_clinical_treatments_snomed')}} as treatments
    on core.primarykey_id = treatments.primarykey_id 
    and treatments.snomed_id = 1