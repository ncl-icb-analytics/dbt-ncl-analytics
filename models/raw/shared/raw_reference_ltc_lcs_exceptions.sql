-- Raw layer model for reference_terminology.LTC_LCS_EXCEPTIONS
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "EXCEPTION_ID" as exception_id,
    "VALUESET_ID" as valueset_id,
    "ORIGINAL_EXCLUDED_CODE" as original_excluded_code,
    "TRANSLATED_TO_SNOMED_CODE" as translated_to_snomed_code,
    "INCLUDED_IN_ECL" as included_in_ecl,
    "TRANSLATION_ERROR" as translation_error
from {{ source('reference_terminology', 'LTC_LCS_EXCEPTIONS') }}
