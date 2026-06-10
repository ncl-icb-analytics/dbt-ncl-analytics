{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    final_icd_codes as (
        select primarykey_id
            , code 
            , count(*) as diagnosis_count
            , array_agg(distinct episodes_id) as episodes_ids
        from {{ ref("stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd") }}
        group by primarykey_id, code
)

select 
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.code"]) }} as diagnosis_id,
    se.sk_patient_id,
    se.start_date as date,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    se.organisation_id,
    se.organisation_name,  -- join to reference
    se.site_id,
    se.site_name,  -- join to reference
    f.code as source_concept_code,
    f.diagnosis_count,
    f.episodes_ids,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f
left join  {{ ref('stg_aic_base_athena_concept') }} c
    on c.concept_code = f.code
    and c.vocabulary_id = 'ICD10'
left join {{ ref("int_sus_apc_encounter") }} se on se.visit_occurrence_id = f.primarykey_id

where se.sk_patient_id is not null and se.sk_patient_id != '1'
