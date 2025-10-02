{{
    config(materialized = 'table')
}}

{{
    deduplicate_csds(
        csds_table = 'dev__modelling.dbt_staging.stg_csds_cyp202careactivity',
        partition_cols = ['unique_care_activity_identifier']
    )
}}