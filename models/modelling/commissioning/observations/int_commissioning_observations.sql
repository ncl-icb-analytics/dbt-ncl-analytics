{{ config(materialized='view') }}

-- note: using sk_patient_id as person_id

with
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
        from {{ ref("int_sus_ip_diagnosis") }} apc
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
        from {{ ref("int_sus_op_diagnosis") }} op
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
 
        from {{ ref("int_sus_ae_diagnosis") }} ae

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
        from op_diagnosis
        union 
        select *
        from op_procedure
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
from all_observations