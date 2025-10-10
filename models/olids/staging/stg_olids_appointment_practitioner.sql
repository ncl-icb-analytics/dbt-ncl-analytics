select
    -- Primary key
    id,

    -- Business columns
    appointment_id,
    practitioner_id,
    lds_end_date_time,
    lds_id,
    lds_record_id_user,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_appointment_practitioner') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
