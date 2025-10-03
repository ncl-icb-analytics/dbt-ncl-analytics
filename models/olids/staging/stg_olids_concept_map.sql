select
    -- Primary key
    id,

    -- Business columns
    lds_id,
    concept_map_id,
    source_code_id,
    target_code_id,
    is_primary,
    equivalence,

    -- Metadata
    lds_start_date_time,
    lds_record_id

from {{ ref('raw_olids_concept_map') }}
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
