/*
Inpatient encounters from SUS

Clinical Purpose:
- Establishing use of inpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/
with ethnicity_codes as (
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
    , core.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER as organisation_id
    , dict_provider.service_provider_name  as organisation_name
    , core.spell_care_location_site_code_of_treatment as site_id
    , dict_org.organisation_name as site_name  
    , core.spell_admission_date as start_date
    , dict_adm_method.admission_method_name as admission_method
    , dict_adm_method.admission_method_group as admission_method_group
    , dict_patient_class.patient_classification_name as admission_patient_classification
    , core.spell_discharge_date as end_date
    , core.spell_discharge_length_of_hospital_stay as duration
    , datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) as duration_to_date
    , core.spell_commissioning_grouping_core_hrg as acuity_proxy
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis  || ', ' || core.spell_clinical_coding_grouper_derived_secondary_diagnosis  as flat_diagnosis_codes
    , core.spell_clinical_coding_grouper_derived_dominant_procedure as primary_treatment
    , 'SUS_APC' as source
    , case 
        when core.spell_admission_method in ('11', '12', '13') -- dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification = '2' -- dict_patient_class.patient_classification_name = 'Day case admission'
            then 'DC' -- Day Case
        when core.spell_admission_method in ('11', '12', '13') -- dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification = '1' -- dict_patient_class.patient_classification_name = 'Ordinary admission'
            then 'EL' -- Elective
        when core.spell_admission_method in ('11', '12', '13') --  dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification in ('3', '4') -- dict_patient_class.patient_classification_name in ('Regular day admission', 'Regular night admission')
            then 'RA' -- Regular Attender (day & night)
        when core.spell_admission_method in ('21', '22', '23', '24', '25', '28','2A','2B','2C','2D') -- dict_adm_method.admission_method_group = 'Non-elective - emergency'
            and datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) = 0 
            then 'NEL-ZLOS'
        when core.spell_admission_method in ('21', '22', '23', '24', '25', '28','2A','2B','2C','2D') -- dict_adm_method.admission_method_group = 'Non-elective - emergency'
            and datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) >= 1 
            then 'NEL-LOS+1'
        when core.spell_admission_method in ('31', '32','82', '83') -- dict_adm_method.admission_method_group in ('Non-elective - Maternity') or dict_adm_method.admission_method_name in ('The birth of a baby', 'Baby born outside the Provider')
            then 'NELNE'
        when core.spell_admission_method = '81' -- dict_adm_method.admission_method_name = 'Transfer'
            then 'TRANSFERS'
        else 'OTHER' end as pod

     -- Adding Spec_comm   
    ,iff(core.spec_comm is null, 'N','Y') as spec_comm_flag
    ,core.spec_comm as spec_comm

    , iff(core.spell_admission_admission_sub_type = 'NON', core.spell_admission_admission_type, core.spell_admission_admission_sub_type) as type
    , core.spell_commissioning_tariff_calculation_final_price as cost
    
    /* patient info at time of event  */
    , core.spell_patient_identity_spell_age as age_at_event
    , core.spell_patient_identity_gender as gender_at_event
    , gen.gender as gender_desc_at_event
    , core.spell_patient_identity_ethnic_category as ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event 
    , core.spell_patient_residence_derived_postcode_district as postcode_district_at_event
    , core.spell_patient_residence_derived_lsoa_11 as lsoa_11_at_event
    , core.spell_patient_residence_derived_local_authority_district as lad_at_event
    , core.spell_patient_residence_derived_index_of_multiple_deprivation_decile as imd_at_event
    , core.spell_patient_registration_general_practice as reg_practice_at_event
    , 'APC_SPELL' as visit_occurrence_type
from {{ ref('stg_sus_apc_spell')}} as core

left join {{ ref('stg_dictionary_ip_admissionmethods')}} as dict_adm_method
    ON core.spell_admission_method = dict_adm_method.bk_admission_method_code

left join {{ ref('stg_dictionary_dbo_patientclassification')}} as dict_patient_class
    ON core.spell_admission_patient_classification = dict_patient_class.bk_patient_classification_code

left join ethnicity_codes as eth
    on core.spell_patient_identity_ethnic_category = eth.bk_ethnicity_code

left join gender_codes as gen
    on core.spell_patient_identity_gender = gen.gender_code

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON core.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER = dict_provider.service_provider_full_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org 
    ON core.spell_care_location_site_code_of_treatment = dict_org.organisation_code 
