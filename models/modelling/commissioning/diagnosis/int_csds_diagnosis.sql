{{ config(materialized="table") }}

with
    primary_diagnosis as (
        {{ make_csds_diagnosis('stg_csds_cyp607primdiag')}}
    ),

    secondary_diagnosis as (
        {{ make_csds_diagnosis('stg_csds_cyp608secdiag')}}
    ),

    all_diagnoses as (
        select *
        from primary_diagnosis 
        union
        select *
        from secondary_diagnosis
    )

select

    {{dbt_utils.generate_surrogate_key( ['sk_patient_id', 'referral_id', 'diagnosis_date', 'diagnostic_hierarchy', 'source_concept_code'] )}} as record_id,
    *
from all_diagnoses