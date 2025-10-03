select
    -- Primary key
    id,

    -- Business columns
    location_id,
    is_primary_contact,
    contact_type,
    contact_type_concept_id,
    value,
    lds_id,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_location_contact') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
