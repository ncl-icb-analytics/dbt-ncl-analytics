-- Staging model for reference_terminology.VALPROATE_PROG_CODES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets

select
    "CODE" as code,
    "CODE_CATEGORY" as code_category,
    "LOOKBACK_YEARS_OFFSET" as lookback_years_offset,
    "VALPROATE_PRODUCT_TERM" as valproate_product_term
from {{ source('reference_terminology', 'VALPROATE_PROG_CODES') }}
