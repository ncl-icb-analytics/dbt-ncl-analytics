
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
    )

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , 'SUS_ECDS' as source
    , core.local_patient_identifier

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
    , core.attendance_departure_time_since_arrival as duration
    -- bed days cc and excess bed days to be added later
    -- other time related fields to be added later

    /* Clinical information */
    -- complaint information
    , core.clinical_chief_complaint_code as chief_complaint_code
    , dict_complaint.snomed_uk_preferred_term as chief_complaint_desc
    , dict_complaint.ecds_group1 as chief_complaint_ecds_group1
    , core.clinical_chief_complaint_is_injury_related as is_injury_related
    , core.clinical_acuity_code as acuity

    -- diagnosis information
    , diagnosis.code as primary_diagnosis_code_snomed
    , diag_dict.snomed_uk_preferred_term as primary_diagnosis_desc_snomed
    , {{clean_icd10_code("diag_dict.icd10_mapping")}} as primary_diagnosis_code_icd10
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

    -- referral source and desc to add later

    /* Discharge information */
    , core.attendance_discharge_destination_code as discharge_destination_code
    , dict_dist.snomed_uk_preferred_term as discharge_destination_desc

    /* Clinician information */
    , '180' as main_specialty_code
    , 'Emergency Medicine' as main_specialty_name

    /* Commissioning information */
    , core.commissioning_grouping_health_resource_group as hrg_code
    , dict_hrg.hrg_description as core_hrg_desc
    , dict_hrg.hrg_chapter_key as core_hrg_chapter
    , dict_hrg.hrg_chapter as core_hrg_chapter_desc
    , core.commissioning_national_pricing_final_price as cost

    /* patient information at time of event */
    , core.patient_age_at_arrival as age_at_event
    , core.patient_stated_gender as gender_at_event
    , gen.gender as gender_desc_at_event
    , core.patient_ethnic_category as ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event
    , core.patient_usual_address_postcode_district as postcode_district_at_event
    , core.patient_usual_address_lsoa_11 as lsoa_11_at_event
    , core.patient_usual_address_local_authority_district as lad_at_event
    , core.patient_usual_address_index_of_multiple_deprivation_decile as imd_at_event
    , core.patient_gp_registration_general_practice as reg_practice_at_event
    , 'AE_ATTENDANCE' as visit_occurrence_type

from {{ ref('stg_sus_ae_emergency_care')}} as core

/* Diagnosis code for infering reason */ -- ADD DEDUP LOGIC?
left join {{ref('stg_sus_ae_clinical_diagnoses_snomed')}} diagnosis
    on core.primarykey_id = diagnosis.primarykey_id and diagnosis.is_primary = TRUE

left join {{ref('stg_dictionary_ecds_diagnosis')}} as diag_dict
    on diagnosis.code = diag_dict.snomed_code

/* First investigation code for infering reason */ 
left join {{ref('stg_sus_ae_clinical_investigations_snomed')}} as investigations
    on core.primarykey_id = investigations.primarykey_id 
    and investigations.snomed_id = 1

left join
    {{ ref('stg_dictionary_ecds_investigation') }} as inv_dict
    on investigations.code = inv_dict.snomed_code

/* First treatment for infering reason  */ 
left join {{ref('stg_sus_ae_clinical_treatments_snomed')}} as treatments
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

/* Demographic dictionaries */
left join ethnicity_codes as eth
    on core.patient_ethnic_category = eth.bk_ethnicity_code
    
left join gender_codes as gen
    on core.patient_stated_gender = gen.gender_code

-- provider name
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_site ON 
    core.attendance_location_site = dict_site.organisation_code

-- site name
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON
    core.attendance_location_hes_provider_3 = dict_org.organisation_code

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg 
    on core.commissioning_grouping_health_resource_group = dict_hrg.hrg_code