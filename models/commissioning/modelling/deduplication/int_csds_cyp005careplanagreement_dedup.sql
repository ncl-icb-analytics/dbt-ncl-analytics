{{
    config(materialized = 'table')
}}

{{
    deduplicate_csds(
        csds_table = 'dev__modelling.dbt_staging.stg_csds_cyp005careplanagreement',
        partition_cols = ['unique_care_plan_identifier']
    )
}}