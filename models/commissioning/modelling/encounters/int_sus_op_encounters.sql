{{
    config(
        materialized='view')
}}


/*
Outpatient encounters from SUS

Processing:
- Select key fields
- Rename fields to standard event model
- Add dictionary lookups to int_sus_op_min to provide descriptive fields
- Map to known definitions [added later]

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

select 
    /* Information needed to derive standard event information */
    core.appointment_identifier as encounter_id
    , core.appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id
    , core.appointment_commissioning_service_agreement_provider as provider_id
    , dict_provider.service_provider_name as provider_name
    , core.appointment_care_location_site_code_of_treatment as site_id
    , dict_org.organisation_name as site_name
    , core.appointment_date as start_date
    , core.appointment_expected_duration as expected_duration
    , core.appointment_outcome as appointment_outcome
    , dict_appt_outcome.attendance_outcome as outcome_desc
    , dict_att_t.attendant_type_desc as first_attendance_desc
    , core.appointment_attended_or_dna as appointment_attended_or_dna
    , dict_att_dna.dna_indicator_desc as appointment_outcome_desc
    , core.appointment_first_attendance as appointment_first_attendance
    , core.appointment_care_professional_main_specialty as primary_reason_for_encounter
    , dict_spec.specialty_name as primary_reason_desc
    , core.appointment_referral_priority_type as acuity -- proxy for acuity, change as poor
    , dict_appt_priority.priority_type_desc as acuity_desc
    , core.appointment_care_professional_treatment_function as primary_treatment
    , dict_treat.specialty_name as treatment_desc
    , 'SUS_OP' as source
    , core.appointment_commissioning_grouping_core_hrg as type -- consider changing to something more aligned with team understanding
    , dict_hrg.hrg_description as type_desc
    , core.appointment_commissioning_tariff_calculation_final_price as cost

from {{ ref('stg_sus_op_appointment')}} as core

-- speciality and treatment descriptions
left join 
    {{ref('stg_dictionary_dbo_specialties')}} as dict_spec 
    on core.appointment_care_professional_main_specialty = dict_spec.bk_specialty_code 
    and dict_spec.is_main_specialty = 1

left join 
    {{ref('stg_dictionary_dbo_specialties')}} as dict_treat 
    on core.appointment_care_professional_treatment_function = dict_treat.bk_specialty_code 
    and dict_treat.is_treatment_function = 1

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg 
    on core.appointment_commissioning_grouping_core_hrg = dict_hrg.hrg_code

-- organisations
left join 
    {{ ref('stg_dictionary_dbo_organisation') }} as dict_org 
    on core.appointment_care_location_site_code_of_treatment = dict_org.organisation_code 

left join 
    {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    on core.appointment_commissioning_service_agreement_provider = dict_provider.service_provider_full_code

-- referral priority
left join
    {{ ref('stg_dictionary_op_prioritytype') }} as dict_appt_priority 
    on cast(core.appointment_referral_priority_type as bigint) = dict_appt_priority.bk_priority_type_code

-- attendance outcome
left join {{ ref('stg_dictionary_op_attendanceoutcomes') }} as dict_appt_outcome 
    on core.appointment_outcome = dict_appt_outcome.bk_attendance_outcome

left join {{ ref('stg_dictionary_op_dnaindicators') }} as dict_att_dna 
    on core.appointment_attended_or_dna = dict_att_dna.bk_dna_code

left join {{ ref('stg_dictionary_op_attendancetypes') }} as dict_att_t 
    on core.appointment_first_attendance = dict_att_t.bk_attendance_type_code

where
    core.appointment_identifier is not null
    and core.appointment_patient_identity_nhs_number_value_pseudo is not null