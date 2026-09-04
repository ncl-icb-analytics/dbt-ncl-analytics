select
    code
    , description
    , valid_from_date
    , valid_to_date
    , is_currently_valid
    , source_code_set_name
    , source_effective_from_at as definition_updated_at
from {{ ref('diagnosis_scheme_history') }}
qualify row_number() over (
    partition by code
    order by source_effective_from_at desc nulls last, source_imported_at desc nulls last
) = 1
