select
    code,
    term,
    category,
    subcategory,
    granular,
    deprioritise_flag,
    preference_rank
    -- Excluded (display/sorting only):
    -- category_sort,
    -- display_sort_key
from {{ ref('raw_reference_ethnicity_codes') }}
