-- Raw layer model for reference_terminology.LTC_LCS_FAILED_CODES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "FAILED_CODE_ID" as failed_code_id,
    "VALUESET_ID" as valueset_id,
    "ORIGINAL_CODE" as original_code,
    "DISPLAY_NAME" as display_name,
    "CODE_SYSTEM" as code_system,
    "REASON" as reason
from {{ source('reference_terminology', 'LTC_LCS_FAILED_CODES') }}
