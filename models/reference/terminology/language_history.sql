select
    code
    , description
    , source_code_set_name
    , source_effective_from_at
    , source_effective_to_at
    , is_current
from {{ ref('stg_ukhfd_other_iso_language_codes') }}
union all
select
    code
    , description
    , source_code_set_name
    , source_effective_from_at
    , source_effective_to_at
    , is_current
from {{ ref('stg_ukhfd_data_dictionary_language_code') }}
