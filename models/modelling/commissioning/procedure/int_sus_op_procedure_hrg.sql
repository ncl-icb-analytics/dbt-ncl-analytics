{{ config(materialized="view") }}

-- note: using sk_patient_id as person_id
with unbundled as(
    select primarykey_id
        , unbundled_hrg_id as problem_order
        , code
    from {{ref('stg_sus_op_appointment_commissioning_grouping_unbundled_hrg')}}
    where code is not null
),
core as (
    select
        primarykey_id, 
        0::number as problem_order, 
        appointment_commissioning_grouping_core_hrg as code
    from {{ ref('stg_sus_op_appointment')}} 
    where appointment_commissioning_grouping_core_hrg is not null
),
hrg_list as(
    select *
    from unbundled
    union 
    select *
    from core
)
select
    {{ dbt_utils.generate_surrogate_key(["hgl.primarykey_id", "hgl.problem_order", "hgl.code"]) }}
    as procedure_id,
    sa.start_date as date,
    hgl.primarykey_id as visit_occurrence_id,
    'OP_ATTENDANCE' as visit_occurrence_type,
    hgl.problem_order,
    sa.sk_patient_id,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,  -- join to reference
    hgl.code as source_concept_code,
    hg.hrg_description as concept_name,  -- mapped concept name from the vocabulary
    'HRG' as concept_vocabulary,
    hg.hrg_chapter,
    hg.hrg_subchapter
from hrg_list hgl

left join {{ ref("int_sus_op_appointments") }} sa on hgl.primarykey_id = sa.visit_occurrence_id

left join {{ ref("stg_dictionary_dbo_hrg") }} hg on hgl.code = hg.hrg_code

where sa.sk_patient_id is not null