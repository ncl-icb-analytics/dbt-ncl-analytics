/*
Outpatient encounters from SUS

Processing:
- Select key fields
- Rename fields to standard encounter model
- Add dictionary lookups to int_sus_op_min to provide descriptive fields
- Map to known definitions [added later]

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

with 
ethnicity_codes as (
    select distinct bk_ethnicity_code, ethnicity_desc 
    from {{ref('stg_dictionary_dbo_ethnicity')}}
    where ethnicity_code_type = 'Current' or bk_ethnicity_code = '99'
    ),
gender_codes as (
    select distinct gender_code, gender
    from {{ref('stg_dictionary_dbo_gender')}}
    )

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , core.appointment_commissioning_service_agreement_provider as organisation_id
    , dict_provider.service_provider_name as organisation_name
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
    , case
        when core.appointment_commissioning_grouping_core_hrg in ('WF01A','WF02A') then 'OPFUP-F2F'
        when core.appointment_commissioning_grouping_core_hrg in ('WF01B','WF02B') then 'OPFA-F2F'
        when core.appointment_commissioning_grouping_core_hrg in ('WF01C','WF02C') then 'OPFUP-NFTF'
        when core.appointment_commissioning_grouping_core_hrg in ('WF01D','WF02D') then 'OPFA-NFTF'
        when left(core.appointment_commissioning_grouping_core_hrg, 1) not in ('W', 'U')
            and dict_att_t.attendant_type_desc = 'First' then 'OPPROC-FA'
        when left(core.appointment_commissioning_grouping_core_hrg, 1) not in ('W', 'U')
            and dict_att_t.attendant_type_desc = 'Follow-up' then 'OPPROC-FU'
        when core.appointment_commissioning_grouping_core_hrg is null then 'UNKNOWN'
        else 'OPPROC'
        end as pod
    -- TO DO: add pod and pod_group
    , core.appointment_commissioning_grouping_core_hrg as type -- consider changing to something more aligned with team understanding
    , dict_hrg.hrg_description as type_desc
    , core.appointment_commissioning_tariff_calculation_final_price as cost
    /* Patient details at time */
    ,core.appointment_patient_identity_age_at_cds_activity_date as age_at_event
    ,core.appointment_patient_identity_gender as gender_at_event
    ,gen.gender as gender_desc_at_event
    ,core.appointment_patient_identity_ethnic_category as ethnicity_at_event
    ,eth.ethnicity_desc as ethnicity_desc_at_event
    ,core.appointment_patient_residence_derived_postcode_district as postcode_district_at_event
    ,core.appointment_patient_residence_derived_lsoa_11 as lsoa_11_at_event
    ,core.appointment_patient_residence_derived_local_authority_district as lad_at_event
    ,core.appointment_patient_residence_derived_index_of_multiple_deprivation_decile as imd_at_event
    ,core.appointment_patient_registration_general_practice as reg_practice_at_event
    ,'OP_ATTENDANCE' as visit_occurrence_type
from {{ ref('stg_sus_op_appointment')}} as core -- TO DO: check if appointments and encounters can be used interchangeably in this context

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

left join ethnicity_codes as eth
    on core.appointment_patient_identity_ethnic_category = eth.bk_ethnicity_code

left join gender_codes as gen
    on core.appointment_patient_identity_gender = gen.gender_code

where
    core.appointment_identifier is not null
    and core.sk_patient_id is not null