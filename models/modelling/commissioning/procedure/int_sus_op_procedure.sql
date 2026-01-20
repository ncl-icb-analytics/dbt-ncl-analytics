{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

select
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.rownumber_id", "f.opcs_id"]) }}
    as procedure_id,
    sa.start_date as date,
    f.primarykey_id as visit_occurrence_id,
    'OP_ATTENDANCE' as visit_occurrence_type,
    null::number as episodes_id,
    f.opcs_id as problem_order,
    f.rownumber_id,
    sa.sk_patient_id,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,  -- join to reference
    f.code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from {{ ref("stg_sus_op_appointment_clinical_coding_procedure_opcs") }} f

left join
     {{ ref('stg_aic_base_athena_concept') }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'

left join {{ ref("int_sus_op_encounters") }} sa on sa.visit_occurrence_id = f.primarykey_id