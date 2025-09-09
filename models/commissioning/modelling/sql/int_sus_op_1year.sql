{{
    config(
        materialized='view')
}}


/*
Recent (1-year) outpatient activities from SUS

Processing:
- Filter to recent activity
- Select key fields
- Rename fields to standard event model

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

{% set years_from_now = -1 %}

/* Establish event range */
with filtered_core as (
    select *
    from {{ ref('stg_sus_op_appointment')}}
    where appointment_date between dateadd(year, {{years_from_now}}, current_date()) and current_date()
    and appointment_identifier is not null
    and appointment_patient_identity_nhs_number_value_pseudo is not null
)

select 
    /* Information needed to derive standard event information */
    core.appointment_identifier as event_id
    , core.appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id
    , core.appointment_commissioning_service_agreement_provider as provider_id
    , core.appointment_care_location_site_code_of_treatment as site_id
    , core.appointment_date as start_date
    , core.appointment_expected_duration as expected_duration
    , core.appointment_outcome as appointment_outcome
    , core.appointment_attended_or_dna as appointment_attended_or_dna
    , core.appointment_first_attendance as appointment_first_attendance
    , core.appointment_care_professional_main_specialty as primary_reason_for_event
    , core.appointment_referral_priority_type as acuity -- proxy for acuity, change as poor
    , core.appointment_care_professional_treatment_function as primary_treatment
    , 'SUS_OP' as source
    , core.appointment_commissioning_grouping_core_hrg as type -- consider changing to something more aligned with team understanding
    , core.appointment_commissioning_tariff_calculation_final_price as cost

from filtered_core as core
