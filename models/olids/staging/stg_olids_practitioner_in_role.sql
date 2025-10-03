select
    -- Primary key
    id,

    -- Business columns
    practitioner_id,
    organisation_id,
    role_code,
    role,
    date_employment_start,
    date_employment_end,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_practitioner_in_role') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
