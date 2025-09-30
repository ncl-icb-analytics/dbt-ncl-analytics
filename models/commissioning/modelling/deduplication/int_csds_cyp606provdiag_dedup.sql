{{
    config(materialized = 'table')
}}

{{
    deduplicate_csds(
        csds_table = 'dev__modelling.dbt_staging.stg_csds_cyp606provdiag',
        partition_cols = ['provisional_diagnosis_coded_clinical_entry', 'diagnosis_scheme_in_use_community_care', 'provisional_diagnosis_date']
    )
}}