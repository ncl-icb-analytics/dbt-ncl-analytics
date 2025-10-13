select
    -- Primary key
    id,

    -- Business columns
    gmc_code,
    title,
    first_name,
    last_name,
    name,
    is_obsolete,
    lds_end_date_time,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_practitioner') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
