{{
    config(
        materialized='view')
}}


/*
Recent inpatient activities from SUS

Processing:
- Add dictionary lookups to int_sus_op_min to provide descriptive fields
- Map to known definitions [added later]

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

select
    core.*
    , dict_provider.service_provider_name as provider_name
    , dict_org.organisation_name as site_name
    , dict_appt_outcome.attendance_outcome as outcome_desc
    , dict_att_dna.dna_indicator_desc as appointment_outcome_desc
    , dict_att_t.attendant_type_desc as first_attendance_desc
    , dict_spec.specialty_name as primary_reason_desc
    , dict_appt_priority.priority_type_desc as acuity_desc
    , dict_treat.specialty_name as treatment_desc
    , dict_hrg.hrg_description as type_desc

from {{ ref('int_sus_op_1year')}} as core

-- speciality and treatment descriptions
LEFT JOIN {{ref('stg_dictionary_dbo_specialties')}} as dict_spec ON 
    core.primary_reason_for_event = dict_spec.bk_specialty_code and 
    dict_spec.is_main_specialty = 1

LEFT JOIN {{ref('stg_dictionary_dbo_specialties')}} as dict_treat ON 
    core.primary_treatment = dict_treat.bk_specialty_code and 
    dict_treat.is_treatment_function = 1

LEFT JOIN {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg ON 
    core.type = dict_hrg.hrg_code

-- organisations
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    core.site_id = dict_org.organisation_code 

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON core.provider_id = dict_provider.service_provider_full_code

-- referral priority
LEFT JOIN {{ ref('stg_dictionary_op_prioritytype') }} AS dict_appt_priority ON 
    cast(core.acuity as bigint) = dict_appt_priority.bk_priority_type_code

-- attendance outcome
LEFT JOIN {{ ref('stg_dictionary_op_attendanceoutcomes') }} AS dict_appt_outcome ON 
    core.appointment_outcome = dict_appt_outcome.bk_attendance_outcome

LEFT JOIN {{ ref('stg_dictionary_op_dnaindicators') }} dict_att_dna on 
    core.appointment_attended_or_dna = dict_att_dna.bk_dna_code

LEFT JOIN {{ ref('stg_dictionary_op_attendancetypes') }} dict_att_t on
    core.appointment_first_attendance = dict_att_t.bk_attendance_type_code