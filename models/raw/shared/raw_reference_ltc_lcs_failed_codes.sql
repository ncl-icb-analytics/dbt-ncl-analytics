{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_FAILED_CODES \ndbt: source(''reference_terminology'', ''LTC_LCS_FAILED_CODES'') \nColumns:\n  FAILED_CODE_ID -> failed_code_id\n  VALUESET_ID -> valueset_id\n  ORIGINAL_CODE -> original_code\n  DISPLAY_NAME -> display_name\n  CODE_SYSTEM -> code_system\n  REASON -> reason"
    )
}}
select
    "FAILED_CODE_ID" as failed_code_id,
    "VALUESET_ID" as valueset_id,
    "ORIGINAL_CODE" as original_code,
    "DISPLAY_NAME" as display_name,
    "CODE_SYSTEM" as code_system,
    "REASON" as reason
from {{ source('reference_terminology', 'LTC_LCS_FAILED_CODES') }}
