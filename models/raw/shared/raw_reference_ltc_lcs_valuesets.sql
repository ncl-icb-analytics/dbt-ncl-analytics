{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_VALUESETS \ndbt: source(''reference_terminology'', ''LTC_LCS_VALUESETS'') \nColumns:\n  VALUESET_ID -> valueset_id\n  REPORT_ID -> report_id\n  VALUESET_INDEX -> valueset_index\n  VALUESET_HASH -> valueset_hash\n  VALUESET_FRIENDLY_NAME -> valueset_friendly_name\n  CODE_SYSTEM -> code_system\n  EXPANSION_ERROR -> expansion_error\n  EXPANDED_AT -> expanded_at"
    )
}}
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
