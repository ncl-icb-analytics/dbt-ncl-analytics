select
    -- Primary key
    id,

    -- Business columns
    lds_id,
    concept_map_id,
    source_code_id,
    target_code_id,
    is_primary,
    equivalence

from {{ ref('raw_olids_concept_map') }}
