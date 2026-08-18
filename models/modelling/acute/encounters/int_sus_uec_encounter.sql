
/*
Emergency care encounters from SUS

Clinical Purpose:
- Establishing demand for emergency care services
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
    ),
commissioner_codes as (
    select commissioner_code, commissioner_name
    from {{ ref('stg_dictionary_dbo_commissioner') }}
    qualify row_number() over (
        partition by commissioner_code
        order by coalesce(end_date, '9999-12-31'::date) desc, start_date desc
    ) = 1
    ),
organisation_codes as (
    select organisation_code, organisation_name
    from {{ ref('stg_dictionary_dbo_organisation') }}
    qualify row_number() over (
        partition by organisation_code
        order by coalesce(end_date, '9999-12-31'::date) desc, start_date desc
    ) = 1
    ),
treatment_function_codes as (
    select bk_specialty_code, treatment_function_description
    from {{ ref('stg_dictionary_dbo_specialties') }}
    where is_treatment_function = true
    qualify row_number() over (
        partition by bk_specialty_code
        order by date_updated desc nulls last, date_created desc nulls last
    ) = 1
    )

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , 'SUS_ECDS' as source
    , core.local_patient_identifier
    , core.system_transaction_cds_unique_identifier as cds_unique_identifier
    , core.commissioning_service_agreement_provider_reference_number as provider_reference_number

    /* Location */
    ,  {{ clean_organisation_id('attendance_location_hes_provider_3') }} as organisation_id
    , dict_org.organisation_name as organisation_name
    , core.attendance_location_site as site_id
    , dict_site.organisation_name as site_name
    , case
        when core.attendance_location_department_type = '01' then 'AE-T1'
        when core.attendance_location_department_type = '02' then 'AE-Other'
        when core.attendance_location_department_type = '03' then 'UCC'
        when core.attendance_location_department_type = '04' then 'WiC'
        when core.attendance_location_department_type = '05' then 'SDEC'
        else 'Others' end as pod
    , core.attendance_location_department_type as department_type

    /* Time & date */
    , core.attendance_arrival_date as start_date
    , core.attendance_arrival_time as start_time
    , core.attendance_departure_date as end_date
    , core.attendance_departure_time as end_time
    , core.attendance_departure_time_since_arrival as duration
    , core.attendance_initial_assessment_date as initial_assessment_date
    , core.attendance_initial_assessment_time as initial_assessment_time
    , core.attendance_initial_assessment_time_since_arrival as initial_assessment_time_since_arrival
    , core.attendance_seen_for_treatment_date as seen_for_treatment_date
    , core.attendance_seen_for_treatment_time as seen_for_treatment_time
    , core.attendance_seen_for_treatment_time_since_arrival as seen_for_treatment_time_since_arrival
    , core.attendance_conclusion_date as conclusion_date
    , core.attendance_conclusion_time as conclusion_time
    , core.attendance_conclusion_time_since_arrival as conclusion_time_since_arrival
    , core.attendance_decision_to_admit_date as decided_to_admit_date
    , core.attendance_decision_to_admit_time as decided_to_admit_time
    , core.attendance_decision_to_admit_time_since_arrival as decided_to_admit_time_since_arrival
    , core.attendance_clinically_ready_to_proceed_timestamp as clinically_ready_to_proceed_at
    , core.attendance_clinically_ready_to_proceed_time_since_arrival
        as clinically_ready_to_proceed_time_since_arrival

    /* Clinical information */
    -- complaint information
    , core.clinical_chief_complaint_code as chief_complaint_code
    , dict_complaint.snomed_uk_preferred_term as chief_complaint_desc
    , dict_complaint.ecds_group1 as chief_complaint_ecds_group1
    , core.clinical_chief_complaint_is_injury_related as is_injury_related
    , core.clinical_acuity_code as acuity

    -- injury information is kept with the complaint fields for maintainability
    , core.clinical_injury_intent_code as injury_intent_code
    , injury_intent.snomed_uk_preferred_term as injury_intent_desc
    , core.clinical_injury_mechanism_code as injury_mechanism_code
    , injury_mechanism.snomed_uk_preferred_term as injury_mechanism_desc
    , core.clinical_injury_place_type as place_of_injury_code
    , place_of_injury.snomed_uk_preferred_term as place_of_injury_desc
    , core.clinical_injury_date as injury_date
    , core.clinical_injury_time as injury_time
    , core.clinical_disease_notification_code as disease_notification_code

    -- diagnosis information
    , diagnosis.code as primary_diagnosis_code_snomed
    , diag_dict.snomed_uk_preferred_term as primary_diagnosis_desc_snomed
    , diag_dict.icd10_mapping as primary_diagnosis_code_icd10
    , diag_dict.icd10_description as primary_diagnosis_desc_icd10
    , diag_dict.ecds_group1 as primary_diagnosis_desc_ecds_group1
    
    -- treatment 
    , treatments.code as primary_treatment
    , treat_dict.snomed_uk_preferred_term as primary_treatment_desc_snomed
    , treat_dict.ecds_group1 as primary_treatment_desc_ecds_group1
    
    -- investigation 
    , investigations.code as primary_investigation
    , inv_dict.snomed_uk_preferred_term as primary_investigation_desc_snomed
    , inv_dict.ecds_group1 as primary_investigation_desc_ecds_group1
    
    /* Arrival information */
    -- arrival mode and desc
    , core.attendance_arrival_arrival_mode_code as arrival_mode_code
    , dict_arrival.snomed_uk_preferred_term as arrival_mode_desc
    , core.attendance_arrival_attendance_category as attendance_category_code
    , case core.attendance_arrival_attendance_category
        when '1' then 'Unplanned first attendance for a new or deteriorating condition'
        when '2' then 'Unplanned follow-up within 7 days at this emergency care service'
        when '3' then 'Unplanned follow-up within 7 days at another emergency care service'
        when '4' then 'Planned follow-up within 7 days at this emergency care service'
        when 'X' then 'Not applicable - patient dead on arrival'
      end as attendance_category_desc
    , core.attendance_arrival_planned as is_arrival_planned
    , core.attendance_arrival_ambulance_incident_number as ambulance_incident_number
    , core.attendance_arrival_conveying_ambulance_trust as conveying_ambulance_trust_code
    , ambulance_trust.organisation_name as conveying_ambulance_trust_name
    , core.attendance_arrival_ambulance_care_contact_identifier
        as ambulance_care_contact_identifier
    , core.attendance_arrival_attendance_source_code as attendance_source_code
    , attendance_source.snomed_uk_preferred_term as attendance_source_desc
    , core.attendance_arrival_attendance_source_organisation
        as attendance_source_organisation_site_identifier
    , attendance_source_site.organisation_name as attendance_source_organisation_site_name

    /* Discharge information */
    , core.attendance_discharge_destination_code as discharge_destination_code
    , dict_dist.snomed_uk_preferred_term as discharge_destination_desc
    , core.attendance_discharge_status_code as discharge_status_code
    , discharge_status.snomed_uk_preferred_term as discharge_status_desc
    , core.attendance_discharge_follow_up_code as discharge_follow_up_code
    , discharge_follow_up.snomed_uk_preferred_term as discharge_follow_up_desc
    , core.attendance_discharge_information_given_code as discharge_information_given_code
    , core.attendance_decision_to_admit_treatment_function_code
        as decided_to_admit_treatment_function_code
    , treatment_function.treatment_function_description
        as decided_to_admit_treatment_function_desc
    , core.attendance_decision_to_admit_receiving_site as receiving_site_id
    , receiving_site.organisation_name as receiving_site_name

    /* Clinician information */
    , '180' as main_specialty_code
    , 'Emergency Medicine' as main_specialty_name

    /* Commissioning information */
    , core.commissioning_grouping_health_resource_group as hrg_code
    , dict_hrg.hrg_description as core_hrg_desc
    , dict_hrg.hrg_chapter_key as core_hrg_chapter
    , dict_hrg.hrg_chapter as core_hrg_chapter_desc
    , core.commissioning_national_pricing_final_price as cost
    , core.commissioning_national_pricing_costing_period as applicable_costing_period
    , core.patient_residence_ccg_from_patient_postcode as residence_commissioner_code_at_event
    , residence_commissioner.commissioner_name as residence_commissioner_name_at_event
    , core.commissioning_service_agreement_commissioner as registrant_commissioner_code_at_event
    , registrant_commissioner.commissioner_name as registrant_commissioner_name_at_event

    /* patient information at time of event */
    , core.patient_age_at_arrival as age_at_event
    , core.patient_patient_type as patient_type_code
    , case
        when core.patient_patient_type = 'ADU' then 'Adult'
        when core.patient_patient_type = 'CHI' then 'Child'
        else core.patient_patient_type
      end as patient_type_desc
    , core.patient_stated_gender as gender_at_event
    , gen.gender as gender_desc_at_event
    , core.patient_ethnic_category as ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event
    , core.patient_usual_address_postcode_district as postcode_district_at_event
    , core.patient_usual_address_lsoa_11 as lsoa_11_at_event
    , core.patient_usual_address_local_authority_district as lad_at_event
    , core.patient_usual_address_index_of_multiple_deprivation_decile as imd_at_event
    , core.patient_gp_registration_general_practice as reg_practice_at_event
    , core.patient_gp_registration_general_practitioner as general_practitioner_code
    , 'AE_ATTENDANCE' as visit_occurrence_type

    /* Referral to treatment information */
    , core.referral_patient_pathway_identifier_value_pseudo as patient_pathway_identifier
    , core.referral_period_status as referral_to_treatment_status_code
    , core.referral_period_start_date as referral_to_treatment_period_start_date
    , core.referral_period_end_date as referral_to_treatment_period_end_date
    , core.referral_waiting_time_measurement_type as waiting_time_measurement_type_code

from {{ ref('stg_sus_ecds_emergency_care')}} as core

/* Diagnosis code for infering reason */ -- ADD DEDUP LOGIC?
left join {{ref('stg_sus_ecds_clinical_diagnoses_snomed')}} diagnosis
    on core.primarykey_id = diagnosis.primarykey_id and diagnosis.is_primary = TRUE

left join {{ref('stg_dictionary_ecds_diagnosis')}} as diag_dict
    on diagnosis.code = diag_dict.snomed_code

/* First investigation code for infering reason */ 
left join {{ref('stg_sus_ecds_clinical_investigations_snomed')}} as investigations
    on core.primarykey_id = investigations.primarykey_id 
    and investigations.snomed_id = 1

left join
    {{ ref('stg_dictionary_ecds_investigation') }} as inv_dict
    on investigations.code = inv_dict.snomed_code

/* First treatment for infering reason  */ 
left join {{ref('stg_sus_ecds_clinical_treatments_snomed')}} as treatments
    on core.primarykey_id = treatments.primarykey_id 
    and treatments.snomed_id = 1

left join
    {{ ref('stg_dictionary_ecds_treatment') }} as treat_dict
    on treatments.code = treat_dict.snomed_code

/* context dictionaries  */
left join 
    {{ref('stg_dictionary_ecds_arrivalmode')}} as dict_arrival
    on core.attendance_arrival_arrival_mode_code = dict_arrival.snomed_code
left join 
    {{ref('stg_dictionary_ecds_dischargedestination')}} as dict_dist
    on core.attendance_discharge_destination_code = dict_dist.snomed_code

left join 
    {{ref('stg_dictionary_ecds_chiefcomplaint')}} as dict_complaint
    on core.clinical_chief_complaint_code = dict_complaint.snomed_code

left join {{ ref('stg_dictionary_ecds_injuryintent') }} as injury_intent
    on core.clinical_injury_intent_code = injury_intent.snomed_code

left join {{ ref('stg_dictionary_ecds_injurymechanism') }} as injury_mechanism
    on core.clinical_injury_mechanism_code = injury_mechanism.snomed_code

left join {{ ref('stg_dictionary_ecds_placeofinjury') }} as place_of_injury
    on core.clinical_injury_place_type = place_of_injury.snomed_code

left join {{ ref('stg_dictionary_ecds_attendancesource') }} as attendance_source
    on core.attendance_arrival_attendance_source_code = attendance_source.snomed_code

left join {{ ref('stg_dictionary_ecds_dischargestatus') }} as discharge_status
    on core.attendance_discharge_status_code = discharge_status.snomed_code

left join {{ ref('stg_dictionary_ecds_dischargefollowup') }} as discharge_follow_up
    on core.attendance_discharge_follow_up_code = discharge_follow_up.snomed_code

/* Demographic dictionaries */
left join ethnicity_codes as eth
    on core.patient_ethnic_category = eth.bk_ethnicity_code
    
left join gender_codes as gen
    on core.patient_stated_gender = gen.gender_code

-- Organisation descriptions. organisation_codes is deduplicated to protect encounter grain.
left join organisation_codes as dict_site on
    core.attendance_location_site = dict_site.organisation_code

left join organisation_codes as ambulance_trust on
    core.attendance_arrival_conveying_ambulance_trust = ambulance_trust.organisation_code

left join organisation_codes as attendance_source_site on
    core.attendance_arrival_attendance_source_organisation = attendance_source_site.organisation_code

left join organisation_codes as receiving_site on
    core.attendance_decision_to_admit_receiving_site = receiving_site.organisation_code

-- provider name
left join {{ ref('organisation_nhs_provider') }} as dict_org on
    core.attendance_location_hes_provider_3 = dict_org.organisation_code

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg
    on core.commissioning_grouping_health_resource_group = dict_hrg.hrg_code

left join commissioner_codes as residence_commissioner
    on core.patient_residence_ccg_from_patient_postcode = residence_commissioner.commissioner_code

left join commissioner_codes as registrant_commissioner
    on core.commissioning_service_agreement_commissioner = registrant_commissioner.commissioner_code

left join treatment_function_codes as treatment_function
    on core.attendance_decision_to_admit_treatment_function_code = treatment_function.bk_specialty_code
