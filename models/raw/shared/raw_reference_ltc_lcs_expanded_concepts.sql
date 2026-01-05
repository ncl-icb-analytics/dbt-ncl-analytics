-- Raw layer model for reference_terminology.LTC_LCS_EXPANDED_CONCEPTS
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "CONCEPT_ID" as concept_id,
    "VALUESET_ID" as valueset_id,
    "SNOMED_CODE" as snomed_code,
    "DISPLAY" as display,
    "SOURCE" as source,
    "EXCLUDE_CHILDREN" as exclude_children,
    "IS_REFSET" as is_refset
from {{ source('reference_terminology', 'LTC_LCS_EXPANDED_CONCEPTS') }}
