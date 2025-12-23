{{ config(materialized="view") }}

-- note: using sk_patient_id as person_id
with unbundled as(
    select primarykey_id
        , episodes_id
        , unbundled_hrg_id as problem_order
        , 'unbundled' as type
        , code
    from {{ref('stg_sus_apc_spell_episodes_commissioning_grouping_unbundled_hrg')}}
    qualify row_number() over (
        partition by primarykey_id, episodes_id, code
        order by unbundled_hrg_id
    ) = 1
),
core as (
    select distinct primarykey_id
        ,episodes_id
        ,0::number as problem_order 
        , 'core' as type
        ,COMMISSIONING_GROUPING_CORE_HRG as code
    from {{ ref('stg_sus_apc_spell_episodes')}} 
),
hrg_list as(
    select *
    from unbundled
    union 
    select *
    from core)

select {{ dbt_utils.generate_surrogate_key(["hgl.primarykey_id", "hgl.episodes_id", "hgl.type", "hgl.code"]) }} as procedure_id,
    see.start_date as date, -- using episode start date as procedure date
    hgl.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    hgl.episodes_id,
    hgl.problem_order,
    se.sk_patient_id,
    se.organisation_id,
    se.organisation_name, 
    se.site_id,
    se.site_name,  
    hgl.code as source_concept_code,
    hg.hrg_description as concept_name,  -- mapped concept name from the vocabulary
    'HRG' as concept_vocabulary,
    hg.hrg_chapter,
    hg.hrg_subchapter
from hrg_list hgl
left join {{ ref("stg_sus_apc_spell_episodes") }} see on hgl.primarykey_id = see.primarykey_id and hgl.episodes_id = see.episodes_id 
left join {{ ref("int_sus_ip_encounters") }} se on hgl.primarykey_id = se.visit_occurrence_id
left join {{ ref("stg_dictionary_dbo_hrg") }} hg on hgl.code = hg.hrg_code
where se.sk_patient_id is not null and se.sk_patient_id != 1 