{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.ECL_CACHE \ndbt: source(''reference_terminology'', ''ECL_CACHE'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  CODE -> code\n  DISPLAY -> display\n  SYSTEM -> system\n  LAST_REFRESHED -> last_refreshed\n  ECL_EXPRESSION_HASH -> ecl_expression_hash"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "LAST_REFRESHED" as last_refreshed,
    "ECL_EXPRESSION_HASH" as ecl_expression_hash
from {{ source('reference_terminology', 'ECL_CACHE') }}
