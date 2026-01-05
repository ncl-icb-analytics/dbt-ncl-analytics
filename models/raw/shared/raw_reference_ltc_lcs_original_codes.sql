-- Raw layer model for reference_terminology.LTC_LCS_ORIGINAL_CODES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "ORIGINAL_CODE_ID" as original_code_id,
    "VALUESET_ID" as valueset_id,
    "ORIGINAL_CODE" as original_code,
    "DISPLAY_NAME" as display_name,
    "CODE_SYSTEM" as code_system,
    "INCLUDE_CHILDREN" as include_children,
    "IS_REFSET" as is_refset,
    "TRANSLATED_TO_SNOMED_CODE" as translated_to_snomed_code,
    "TRANSLATED_TO_DISPLAY" as translated_to_display
from {{ source('reference_terminology', 'LTC_LCS_ORIGINAL_CODES') }}
