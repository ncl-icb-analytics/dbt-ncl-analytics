{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.VALPROATE_PROG_CODES \ndbt: source(''reference_terminology'', ''VALPROATE_PROG_CODES'') \nColumns:\n  CODE -> code\n  CODE_CATEGORY -> code_category\n  LOOKBACK_YEARS_OFFSET -> lookback_years_offset\n  VALPROATE_PRODUCT_TERM -> valproate_product_term"
    )
}}
select
    "CODE" as code,
    "CODE_CATEGORY" as code_category,
    "LOOKBACK_YEARS_OFFSET" as lookback_years_offset,
    "VALPROATE_PRODUCT_TERM" as valproate_product_term
from {{ source('reference_terminology', 'VALPROATE_PROG_CODES') }}
