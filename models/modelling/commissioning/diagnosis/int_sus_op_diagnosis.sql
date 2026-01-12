{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    final_icd_codes as (
        select primarykey_id
            ,icd_id 
            ,rownumber_id 
            , {{ clean_icd10_code("CODE") }} as concept_code 
            ,code
        from {{ ref("stg_sus_op_appointment_clinical_coding_diagnosis_icd") }}
)
    
select
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.rownumber_id", "f.icd_id"]) }} as diagnosis_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    sa.start_date as date,
    'OP_ATTENDANCE' as visit_occurrence_type,
    f.icd_id,
    f.rownumber_id,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,
    sa.start_date as activity_date,
    f.concept_code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f

left join
    {{ ref('stg_aic_base_athena_concept') }} c
    on c.concept_code = f.concept_code
    and c.vocabulary_id = 'ICD10'

left join {{ ref("int_sus_op_encounters") }} sa on sa.visit_occurrence_id = f.primarykey_id

where sa.sk_patient_id is not null
