{{ config(materialized="view") }}


-- note: using sk_patient_id as person_id
with investigations as (
    select primarykey_id
        ,code
        -- ,snomed_id  as problem_order
        -- ,rownumber_id
        ,'investigation' as observation_type
        -- ,date 
    from {{ref('stg_sus_ae_clinical_investigations_snomed')}}
),
inv_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        -- cds_investigation_mapping_that_is_used_for_hrg_grouping,
        from {{ref('stg_dictionary_ecds_investigation')}}
),
treatments as (
    select primarykey_id
        ,code
        -- ,snomed_id  as problem_order
        -- ,rownumber_id
        ,'treatment' as observation_type
        -- ,date 
    from {{ref('stg_sus_ae_clinical_treatments_snomed')}}
),
treat_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        -- cds_investigation_mapping_that_is_used_for_hrg_grouping,
        from {{ref('stg_dictionary_ecds_treatment')}}
),
comorbs as (
    select primarykey_id
        ,code
        -- ,comorbidities_id as problem_order
        -- ,rownumber_id
        ,'comorbs' as observation_type
        -- ,null::timestamp as date
    from {{ref('stg_sus_ae_clinical_comorbidities')}}
),
comorb_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        -- null as cds_investigation_mapping_that_is_used_for_hrg_grouping,
        from {{ref('stg_dictionary_ecds_comorbidity')}}
),
findings as (
    select distinct primarykey_id
        ,code
        -- ,coded_findings_id as problem_order
        -- ,rownumber_id
        -- ,"TIMESTAMP" as date 
        ,'findings' as observation_type
    from {{ref('stg_sus_ae_clinical_coded_findings')}}
), 
find_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        -- null as cds_investigation_mapping_that_is_used_for_hrg_grouping,
        from {{ref('stg_dictionary_ecds_codedfinding')}}
),
all_obs as(
    select *
    from investigations
    left join inv_dict on inv_dict.snomed_code = investigations.code
    union 
    select *
    from treatments
    left join treat_dict on treat_dict.snomed_code = treatments.code
    union 
    select *
    from comorbs
    left join comorb_dict on comorb_dict.snomed_code = comorbs.code
    union 
    select *
    from findings
    left join find_dict on find_dict.snomed_code = findings.code
)

select 
 {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.code", "sa.attendance_arrival_date"]) }} as event_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    f.observation_type,
    sa.attendance_arrival_date as date,
    'AE_ATTENDANCE' as visit_occurrence_type,
    -- f.problem_order,
    -- f.rownumber_id,
    sa.attendance_location_hes_provider_3 as organisation_id,
    dict_org.organisation_name as organisation_name,  
    sa.attendance_location_site as site_id,
    dict_site.organisation_name as site_name,
    sa.attendance_location_department_type as department_type,
    f.code as snomed_code,
    f.ecds_description,
    f.ecds_group1,
    f.snomed_uk_preferred_term as snomed_decription

from all_obs as f

/* Diagnosis code for infering reason */
left join {{ref('stg_sus_ae_emergency_care')}} as sa on sa.primarykey_id = f.primarykey_id

-- provider name
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_site ON 
    sa.attendance_location_site = dict_site.organisation_code

-- site name
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_org ON 
    sa.attendance_location_hes_provider_3 = dict_org.organisation_code 

where sa.sk_patient_id is not null
and f.code is not null