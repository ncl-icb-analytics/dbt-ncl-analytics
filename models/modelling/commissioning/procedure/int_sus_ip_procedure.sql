{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

select
    {{ dbt_utils.generate_surrogate_key( ["f.primarykey_id", "f.episodes_id", "f.rownumber_id", "f.opcs_id"])}} as procedure_id,
    se.sk_patient_id,
    se.start_date as date,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    f.episodes_id,
    f.opcs_id as problem_order,
    f.rownumber_id,
    se.organisation_id,
    se.organisation_name,  -- join to reference
    se.site_id,
    se.site_name,  
    f.code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from {{ ref("stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs") }} f
left join
    {{ source('aic', 'BASE_ATHENA__CONCEPT') }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'
left join {{ ref("int_sus_ip_encounters") }} se on se.visit_occurrence_id = f.primarykey_id

where se.sk_patient_id is not null
