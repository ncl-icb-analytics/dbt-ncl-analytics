{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.ETHNICITY_CODES \ndbt: source(''reference_terminology'', ''ETHNICITY_CODES'') \nColumns:\n  CODE -> code\n  TERM -> term\n  CATEGORY -> category\n  SUBCATEGORY -> subcategory\n  GRANULAR -> granular\n  DEPRIORITISE_FLAG -> deprioritise_flag\n  PREFERENCE_RANK -> preference_rank\n  CATEGORY_SORT -> category_sort\n  DISPLAY_SORT_KEY -> display_sort_key"
    )
}}
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
