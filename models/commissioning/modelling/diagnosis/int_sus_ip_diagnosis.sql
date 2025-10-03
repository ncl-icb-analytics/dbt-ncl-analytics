{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    final_icd_codes as (
        select primarykey_id
            ,icd_id 
            ,rownumber_id 
            ,episodes_id
            , {{ clean_icd10_code("CODE") }} as concept_code 
            ,code
        from {{ ref("stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd") }}
)

select 
    {{dbt_utils.generate_surrogate_key( ["f.primarykey_id", "f.rownumber_id", "f.episodes_id", "f.icd_id"])}} as diagnosis_id,
    se.sk_patient_id,
    se.spell_admission_date as date,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    f.episodes_id,
    icd_id,
    f.rownumber_id,
    se.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER as organisation_id,
    dict_provider.service_provider_name  as organisation_name,  -- join to reference
    se.spell_care_location_site_code_of_treatment as site_id,
    dict_org.organisation_name as site_name,  -- join to reference
    f.concept_code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f
left join  {{ source('phenolab', 'BASE_ATHENA__CONCEPT') }} c
    on c.concept_code = f.concept_code
    and c.vocabulary_id = 'ICD10'
left join {{ ref("stg_sus_apc_spell") }} se on se.primarykey_id = f.primarykey_id

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON se.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER = dict_provider.service_provider_full_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    se.spell_care_location_site_code_of_treatment = dict_org.organisation_code 

where se.spell_patient_identity_nhs_number_value_pseudo is not null
