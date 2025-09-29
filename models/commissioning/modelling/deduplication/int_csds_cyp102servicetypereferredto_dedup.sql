{{
    config(materialized = 'table')
}}

{{
    deduplicate_csds(
        csds_table = 'dev__modelling.dbt_staging.stg_csds_cyp102servicetypereferredto',
        partition_cols = ['service_or_team_type_referred_to_community_care']
    )
}}