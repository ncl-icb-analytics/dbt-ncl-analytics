select
    -- Primary key
    id,

    -- Business columns
    lds_id,
    system,
    code,
    display,
    is_mapped,
    use_count,

    -- Metadata
    lds_start_date_time,
    lds_record_id

from {{ ref('raw_olids_concept') }}
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
