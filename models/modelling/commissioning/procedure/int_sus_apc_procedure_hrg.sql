{{ config(materialized="view") }}

-- note: using sk_patient_id as person_id
with unbundled as(
    select {{ dbt_utils.generate_surrogate_key(["primarykey_id", "episodes_id"]) }} as episode_key_id
        , primarykey_id
        , episodes_id
        , unbundled_hrg_id as problem_order
        , code
    from {{ref('stg_sus_apc_spell_episodes_commissioning_grouping_unbundled_hrg')}}
),
core as (
    select {{ dbt_utils.generate_surrogate_key(["primarykey_id", "episodes_id"]) }} as episode_key_id
        , primarykey_id
        , episodes_id
        ,0::number as problem_order 
        ,COMMISSIONING_GROUPING_CORE_HRG as code
    from {{ ref('stg_sus_apc_spell_episodes')}} 
),
hrg_list as(
    select *
    from unbundled
    union 
    select *
    from core
)
select {{ dbt_utils.generate_surrogate_key(["hgl.episode_key_id", "hgl.problem_order", "hgl.code"]) }} as procedure_id,
    see.start_date as date,
    hgl.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    see.episodes_id,
    hgl.problem_order,
    se.sk_patient_id,
    se.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER as organisation_id,
    dict_provider.service_provider_name  as organisation_name, 
    se.spell_care_location_site_code_of_treatment as site_id,
    dict_org.organisation_name as site_name,  
    hgl.code as source_concept_code,
    hg.hrg_description as concept_name,  -- mapped concept name from the vocabulary
    'HRG' as concept_vocabulary,
    hg.hrg_chapter,
    hg.hrg_subchapter
from hrg_list hgl

left join {{ ref("stg_sus_apc_spell_episodes") }} see on hgl.primarykey_id = see.primarykey_id and hgl.episodes_id = see.episodes_id 
left join {{ ref("stg_sus_apc_spell") }} se on hgl.primarykey_id = se.primarykey_id

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON se.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER = dict_provider.service_provider_full_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    se.spell_care_location_site_code_of_treatment = dict_org.organisation_code 

left join {{ ref("stg_dictionary_dbo_hrg") }} hg on hgl.code = hg.hrg_code

where se.sk_patient_id is not null and se.sk_patient_id != 1 