-- Raw layer model for reference_terminology.LTC_LCS_VALUESETS
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "VALUESET_ID" as valueset_id,
    "REPORT_ID" as report_id,
    "VALUESET_INDEX" as valueset_index,
    "VALUESET_HASH" as valueset_hash,
    "VALUESET_FRIENDLY_NAME" as valueset_friendly_name,
    "CODE_SYSTEM" as code_system,
    "EXPANSION_ERROR" as expansion_error,
    "EXPANDED_AT" as expanded_at
from {{ source('reference_terminology', 'LTC_LCS_VALUESETS') }}
