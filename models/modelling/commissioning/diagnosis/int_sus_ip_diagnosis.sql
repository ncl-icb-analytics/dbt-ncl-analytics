{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    final_icd_codes as (
        select  diagnosis_id
            ,primarykey_id
            ,icd_id 
            ,rownumber_id 
            ,episodes_id
            , {{ clean_icd10_code("CODE") }} as concept_code 
            ,code
        from {{ ref("stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd") }}
)

select 
    f.diagnosis_id,
    se.sk_patient_id,
    se.start_date as date,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    f.episodes_id,
    icd_id,
    f.rownumber_id,
    se.organisation_id,
    se.organisation_name,  -- join to reference
    se.site_id,
    se.site_name,  -- join to reference
    f.concept_code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f
left join  {{ ref('stg_aic_base_athena_concept') }} c
    on c.concept_code = f.concept_code
    and c.vocabulary_id = 'ICD10'
left join {{ ref("int_sus_ip_encounters") }} se on se.visit_occurrence_id = f.primarykey_id

where se.sk_patient_id is not null
