{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    final_icd_codes as (
        select primarykey_id
            ,icd_id 
            ,rownumber_id 
            , {{ clean_icd10_code("CODE") }} as concept_code 
            ,code
        from {{ ref("stg_sus_op_appointment_clinical_coding_diagnosis_icd") }}
)
    
select
    sa.appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    appointment_date as date,
    'OP_ATTENDANCE' as visit_occurrence_type,
    f.icd_id,
    f.rownumber_id,
    sa.appointment_commissioning_service_agreement_provider as organisation_id,
    dict_provider.service_provider_name as organisation_name,  
    sa.appointment_care_location_site_code_of_treatment as site_id,
    dict_org.organisation_name as site_name,
    appointment_date as activity_date,
    f.concept_code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f

left join
    {{ source('phenolab', 'BASE_ATHENA__CONCEPT') }} c
    on c.concept_code = f.concept_code
    and c.vocabulary_id = 'ICD10'

left join {{ ref("stg_sus_op_appointment") }} sa on sa.primarykey_id = f.primarykey_id

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON sa.appointment_commissioning_service_agreement_provider = dict_provider.service_provider_full_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    sa.appointment_care_location_site_code_of_treatment = dict_org.organisation_code 

where sa.appointment_patient_identity_nhs_number_value_pseudo is not null
