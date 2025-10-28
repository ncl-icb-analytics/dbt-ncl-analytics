{{ config(materialized='view', enabled=false) }}

-- note: using sk_patient_id as person_id

with
    -- Filter out measurement definitions from DEFINITIONSTORE
    -- definitionstore_filtered as (
    --     select *
    --     from {{ source("phenolab", "DEFINITION_STORE") }}
    --     where not lower(definition_name) like 'measurement_%'
    -- ),
    
    apc_diagnosis as (
        select diagnosis_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,icd_id as problem_order
            ,concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
            -- ,ds.definition_id
            -- ,ds.definition_name
            -- ,ds.definition_source
        from {{ ref("int_sus_ip_diagnosis") }} apc
        -- inner join
        --     definitionstore_filtered ds
        --     on apc.concept_code = ds.code
        --     and apc.concept_vocabulary = ds.vocabulary
    ),

    apc_procedure as (
        select procedure_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,problem_order
            ,concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
        from {{ ref("int_sus_ip_procedure") }} apc
    ),
    apc_procedure_hrg as (
        select procedure_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,problem_order
            ,source_concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
    from {{ ref("int_sus_apc_procedure_hrg") }}
    ),
    op_diagnosis as (
        select diagnosis_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,icd_id as problem_order
            ,concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
            -- ,ds.definition_id
            -- ,ds.definition_name
            -- ,ds.definition_source
        from {{ ref("int_sus_op_diagnosis") }} op
        -- inner join
        --     definitionstore_filtered ds
        --     on op.concept_code = ds.code
        --     and op.concept_vocabulary = ds.vocabulary
    ),
    op_procedure as (
    select procedure_id as event_id
        ,sk_patient_id
        ,visit_occurrence_id
        ,visit_occurrence_type
        ,organisation_id
        ,organisation_name
        ,date
        ,null as clinical_end_date
        ,problem_order
        ,concept_code::varchar  as observation_concept_code
        ,concept_name as observation_concept_name
        ,concept_vocabulary as observation_vocabulary
    from {{ ref("int_sus_op_procedure") }}
),
    op_procedure_hrg as (
    select procedure_id as event_id
        ,sk_patient_id
        ,visit_occurrence_id
        ,visit_occurrence_type
        ,organisation_id
        ,organisation_name
        ,date
        ,null as clinical_end_date
        ,problem_order
        ,source_concept_code::varchar  as observation_concept_code
        ,concept_name as observation_concept_name
        ,concept_vocabulary as observation_vocabulary
    from {{ ref("int_sus_op_procedure_hrg") }}
),

   ae_diagnosis as (
        select diagnosis_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,snomed_id as problem_order
            ,source_concept_code::varchar  as observation_concept_code
            ,source_concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
            -- ,ds.definition_id
            -- ,ds.definition_name
            -- ,ds.definition_source
        from {{ ref("int_sus_ae_diagnosis") }} ae
        -- inner join
        --     definitionstore_filtered ds
        --     on ae.concept_code = ds.code
        --     and ae.concept_vocabulary = ds.vocabulary
   ),

   ae_procedure as(
    select event_id
        ,sk_patient_id
        ,visit_occurrence_id
        ,visit_occurrence_type
        ,organisation_id
        ,organisation_name
        ,date
        , null as clinical_end_date
        , null as problem_order
        , snomed_code::varchar  as concept_code
        , snomed_decription as concept_name
        , 'SNOMED' as observation_vocabulary
        from {{ ref("int_sus_ae_procedure") }} ae
   ),

    all_observations as (
        select *
        from apc_diagnosis
        union 
        select *
        from apc_procedure
        union
        select *
        from apc_procedure_hrg
        union
        select *
        from op_diagnosis
        union 
        select *
        from op_procedure
        union
        select * 
        from op_procedure_hrg
        union
        select *
        from ae_diagnosis
        union
        select *
        from ae_procedure
    )

select
    -- changed key as removing definition store join
    {{dbt_utils.generate_surrogate_key( ["event_id", "observation_concept_code", "visit_occurrence_id"] )}} as record_id,
    sk_patient_id,
    visit_occurrence_id,
    visit_occurrence_type,
    date,
    clinical_end_date,
    problem_order,
    observation_concept_code,
    observation_concept_name,
    observation_vocabulary
    -- definition_id,
    -- definition_name as condition_definition_name,
    -- definition_source,
-- observation_concept_code,
-- observation_concept_name,
from all_observations