{{ config(materialized="table", enabled=false) }}

-- note: using sk_patient_id as person_id

select
    {{ dbt_utils.generate_surrogate_key( ["f.primarykey_id", "f.episodes_id", "f.rownumber_id", "f.opcs_id"])}} as procedure_id,
    se.sk_patient_id,
    se.spell_admission_date as date,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    f.episodes_id,
    f.opcs_id as problem_order,
    f.rownumber_id,
    se.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER as organisation_id,
    dict_provider.service_provider_name  as organisation_name,  -- join to reference
    se.spell_care_location_site_code_of_treatment as site_id,
    dict_org.organisation_name as site_name,  
    f.code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from {{ ref("stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs") }} f
left join
    {{ ref('raw_phenolab_base_athena_concept') }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'
left join {{ ref("stg_sus_apc_spell") }} se on se.primarykey_id = f.primarykey_id

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON se.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER = dict_provider.service_provider_full_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    se.spell_care_location_site_code_of_treatment = dict_org.organisation_code 

where se.sk_patient_id is not null
