select
    code,
    code_category,
    lookback_years_offset,
    valproate_product_term
from {{ ref('raw_reference_valproate_prog_codes') }}
