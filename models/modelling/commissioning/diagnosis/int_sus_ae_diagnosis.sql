{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    diag_codes as (
        select primarykey_id
            ,snomed_id
            ,rownumber_id 
            ,code
        from {{ ref("stg_sus_ae_clinical_diagnoses_snomed") }} 
        where code is not null 
),
    final_icd_codes as (
        select snomed_code as code
            ,snomed_uk_preferred_term as snomed_description
            ,ecds_group1
            ,ecds_group3 
            , {{ clean_icd10_code("ICD10_MAPPING") }} as concept_code 
            , icd10_description as concept_name
        from {{ ref('stg_dictionary_ecds_diagnosis')}} )
    

select 
 {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.rownumber_id", "f.snomed_id"]) }} as diagnosis_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    sa.attendance_arrival_date as date,
    'AE_ATTENDANCE' as visit_occurrence_type,
    f.snomed_id,
    f.rownumber_id,
    sa.attendance_location_hes_provider_3 as organisation_id,
    dict_org.organisation_name as organisation_name,  
    sa.attendance_location_site as site_id,
    dict_site.organisation_name as site_name,
    sa.attendance_location_department_type as department_type,
    f.code as source_concept_code,
    d.concept_code as mapped_icd10_code,
    d.snomed_description as source_concept_name,
    'SNOMED' as concept_vocabulary,
    d.snomed_description,
    d.ecds_group1,
    d.ecds_group3

from diag_codes as f

/* Diagnosis code for infering reason */
left join {{ref('stg_sus_ae_emergency_care')}} as sa on sa.primarykey_id = f.primarykey_id

left join final_icd_codes as d on d.code = f.code

left join
    {{ ref('raw_phenolab_base_athena_concept') }} c
    on c.concept_code = d.concept_code
    and c.vocabulary_id = 'ICD10'

-- provider name
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_site ON 
    sa.attendance_location_site = dict_site.organisation_code

-- site name
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON
    sa.attendance_location_hes_provider_3 = dict_org.organisation_code

where sa.sk_patient_id is not null