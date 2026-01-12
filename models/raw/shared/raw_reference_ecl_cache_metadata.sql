{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.ECL_CACHE_METADATA \ndbt: source(''reference_terminology'', ''ECL_CACHE_METADATA'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  LAST_SUCCESSFUL_REFRESH -> last_successful_refresh\n  LAST_ATTEMPTED_REFRESH -> last_attempted_refresh\n  LAST_ERROR_MESSAGE -> last_error_message\n  ECL_EXPRESSION_HASH -> ecl_expression_hash\n  RECORD_COUNT -> record_count\n  LAST_REFRESHED_BY -> last_refreshed_by\n  LAST_ATTEMPTED_BY -> last_attempted_by"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "LAST_SUCCESSFUL_REFRESH" as last_successful_refresh,
    "LAST_ATTEMPTED_REFRESH" as last_attempted_refresh,
    "LAST_ERROR_MESSAGE" as last_error_message,
    "ECL_EXPRESSION_HASH" as ecl_expression_hash,
    "RECORD_COUNT" as record_count,
    "LAST_REFRESHED_BY" as last_refreshed_by,
    "LAST_ATTEMPTED_BY" as last_attempted_by
from {{ source('reference_terminology', 'ECL_CACHE_METADATA') }}
