select
    'child_protection_plan_status' as terminology_name
    , code
    , description
    , source_code_set_name
    , source_effective_from_at
    , source_effective_to_at
    , is_current
from {{ ref('stg_ukhfd_data_dictionary_child_protection_plan_indicator') }}

union all

select
    'looked_after_child_indicator' as terminology_name
    , code
    , description
    , source_code_set_name
    , source_effective_from_at
    , source_effective_to_at
    , is_current
from {{ ref('stg_ukhfd_data_dictionary_looked_after_child_indicator') }}
