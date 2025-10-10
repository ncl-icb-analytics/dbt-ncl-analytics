select
    -- Primary key
    id,

    -- Business columns
    location_id,
    location,
    practitioner_id,
    start_date,
    end_date,
    type,
    name,
    is_private,
    is_deleted,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_schedule') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
