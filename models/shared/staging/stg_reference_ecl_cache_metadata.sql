-- Staging model for reference_terminology.ECL_CACHE_METADATA
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets

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
