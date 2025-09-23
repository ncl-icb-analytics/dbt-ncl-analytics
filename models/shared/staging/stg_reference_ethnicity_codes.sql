-- Staging model for reference_terminology.ETHNICITY_CODES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets

select
    "CODE" as code,
    "TERM" as term,
    "CATEGORY" as category,
    "SUBCATEGORY" as subcategory,
    "GRANULAR" as granular,
    "DEPRIORITISE_FLAG" as deprioritise_flag,
    "PREFERENCE_RANK" as preference_rank,
    "CATEGORY_SORT" as category_sort,
    "DISPLAY_SORT_KEY" as display_sort_key
from {{ source('reference_terminology', 'ETHNICITY_CODES') }}
