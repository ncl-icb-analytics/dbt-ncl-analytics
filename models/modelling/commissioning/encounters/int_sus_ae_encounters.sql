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
    ,  {{ clean_organisation_id('attendance_location_hes_provider_3') }} as organisation_id
    , dict_org.organisation_name as organisation_name
    , core.attendance_location_site as site_id
    , dict_site.organisation_name as site_name
    , core.attendance_arrival_date as start_date
    , core.attendance_departure_time_since_arrival as duration
    , core.clinical_chief_complaint_code as primary_reason_for_encounter
    , core.clinical_acuity_code as acuity
    , diagnosis.flat_diagnosis_codes
    , treatments.code as primary_treatment
    , investigations.code as primary_investigation
    , 'SUS_ECDS' as source
    , case
        when core.attendance_location_department_type = '01' then 'AE-T1'
        when core.attendance_location_department_type = '02' then 'AE-Other'
        when core.attendance_location_department_type = '03' then 'UCC'
        when core.attendance_location_department_type = '04' then 'WiC'
        when core.attendance_location_department_type = '05' then 'SDEC'
        else 'Others' end as pod
    , core.attendance_location_department_type as department_type
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

/* Diagnosis code for infering reason */
left join (
    select diag.primarykey_id
    , listagg(diag.code, ', ') within group (order by diag.snomed_id) as flat_diagnosis_codes
    from {{ref('stg_sus_ae_clinical_diagnoses_snomed')}} diag
    inner join {{ ref('stg_sus_ae_emergency_care')}} fc on diag.primarykey_id = fc.primarykey_id
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