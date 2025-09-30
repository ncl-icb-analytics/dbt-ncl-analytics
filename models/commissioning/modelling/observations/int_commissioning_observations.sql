{{ config(materialized='view') }}

-- note: using sk_patient_id as person_id

with
    -- Filter out measurement definitions from DEFINITIONSTORE
    -- definitionstore_filtered as (
    --     select *
    --     from {{ source("phenolab", "DEFINITION_STORE") }}
    --     where not lower(definition_name) like 'measurement_%'
    -- ),
    
    apc_diagnosis as (
        select diagnosis_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,icd_id as problem_order
            ,concept_code as observation_concept_code
            ,concept_name as observation_concept_name
            -- ,ds.definition_id
            -- ,ds.definition_name
            -- ,ds.definition_source
        from {{ ref("int_sus_ip_diagnosis") }} apc
        -- inner join
        --     definitionstore_filtered ds
        --     on apc.concept_code = ds.code
        --     and apc.concept_vocabulary = ds.vocabulary
    ),

    op_diagnosis as (
        select diagnosis_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,icd_id as problem_order
            ,concept_code as observation_concept_code
            ,concept_name as observation_concept_name
            -- ,ds.definition_id
            -- ,ds.definition_name
            -- ,ds.definition_source
        from {{ ref("int_sus_op_diagnosis") }} op
        -- inner join
        --     definitionstore_filtered ds
        --     on op.concept_code = ds.code
        --     and op.concept_vocabulary = ds.vocabulary
    ),

   ae_diagnosis as (
        select diagnosis_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,snomed_id as problem_order
            ,concept_code as observation_concept_code
            ,concept_name as observation_concept_name
            -- ,ds.definition_id
            -- ,ds.definition_name
            -- ,ds.definition_source
        from {{ ref("int_sus_ae_diagnosis") }} ae
        -- inner join
        --     definitionstore_filtered ds
        --     on ae.concept_code = ds.code
        --     and ae.concept_vocabulary = ds.vocabulary
   ),

    all_observations as (
        select *
        from apc_diagnosis
        -- union all
        -- select *
        -- from apc_procedure
        union
        select *
        from op_diagnosis
        -- union all
        -- select *
        -- from op_procedure
        union
        select *
        from ae_diagnosis
    )

select
    -- changed key as removing definition store join
    {{dbt_utils.generate_surrogate_key( ["diagnosis_id", "observation_concept_code", "visit_occurrence_id"] )}} as diagnosis_id,
    sk_patient_id,
    visit_occurrence_id,
    visit_occurrence_type,
    date,
    clinical_end_date,
    problem_order,
    observation_concept_code,
    observation_concept_name,
    -- definition_id,
    -- definition_name as condition_definition_name,
    -- definition_source,
-- observation_concept_code,
-- observation_concept_name,
from all_observations