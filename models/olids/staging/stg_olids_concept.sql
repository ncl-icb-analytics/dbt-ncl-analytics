select
    -- Primary key
    id,

    -- Business columns
    lds_id,
    system,
    code,
    display,
    is_mapped,
    use_count

from {{ ref('raw_olids_concept') }}
