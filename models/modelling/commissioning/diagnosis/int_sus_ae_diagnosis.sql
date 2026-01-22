{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    diag_codes as (
        select diagnosis_id
        , primarykey_id
        , snomed_id
        , rownumber_id 
        , code
        from {{ ref("stg_sus_ae_clinical_diagnoses_snomed") }} 
        where code is not null 
        qualify row_number() over (partition by primarykey_id, snomed_id, code order by rownumber_id) = 1
),
    final_icd_codes as (
        select snomed_code as code
            ,snomed_uk_preferred_term as snomed_description
            ,ecds_group1
            ,ecds_group3 
            , {{ clean_icd10_code("ICD10_MAPPING") }} as concept_code 
            , icd10_description as concept_name
        from {{ ref('stg_dictionary_ecds_diagnosis')}} )
    

select f.diagnosis_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    sa.start_date as date,
    sa.visit_occurrence_type,
    f.snomed_id,
    f.rownumber_id,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,
    sa.department_type,
    f.code as source_concept_code,
    d.concept_code as mapped_icd10_code,
    d.snomed_description as source_concept_name,
    'SNOMED' as concept_vocabulary,
    d.snomed_description,
    d.ecds_group1,
    d.ecds_group3

from diag_codes as f

/* Diagnosis code for infering reason */
left join {{ref('int_sus_ae_encounters')}} as sa on sa.visit_occurrence_id = f.primarykey_id

left join final_icd_codes as d on d.code = f.code

left join
    {{ ref('stg_aic_base_athena_concept') }} c
    on c.concept_code = d.concept_code
    and c.vocabulary_id = 'ICD10'

where sa.sk_patient_id is not null