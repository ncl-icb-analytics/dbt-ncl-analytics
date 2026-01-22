{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_EXPANDED_CONCEPTS \ndbt: source(''reference_terminology'', ''LTC_LCS_EXPANDED_CONCEPTS'') \nColumns:\n  CONCEPT_ID -> concept_id\n  VALUESET_ID -> valueset_id\n  SNOMED_CODE -> snomed_code\n  DISPLAY -> display\n  SOURCE -> source\n  EXCLUDE_CHILDREN -> exclude_children\n  IS_REFSET -> is_refset"
    )
}}
select
    "CONCEPT_ID" as concept_id,
    "VALUESET_ID" as valueset_id,
    "SNOMED_CODE" as snomed_code,
    "DISPLAY" as display,
    "SOURCE" as source,
    "EXCLUDE_CHILDREN" as exclude_children
from {{ source('reference_terminology', 'LTC_LCS_EXPANDED_CONCEPTS') }}
