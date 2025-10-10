-- Raw layer model for reference_terminology.ECL_CACHE
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CLUSTER_ID" as cluster_id,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "LAST_REFRESHED" as last_refreshed,
    "ECL_EXPRESSION_HASH" as ecl_expression_hash
from {{ source('reference_terminology', 'ECL_CACHE') }}
