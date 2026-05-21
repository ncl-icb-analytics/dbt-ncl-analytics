select
    -- Primary key
    id,

    -- Business columns
    appointment_id,
    practitioner_id,
    lds_end_date_time,
    lds_id,
    lds_record_id_user,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_appointment_practitioner') }}
where coalesce(lds_is_deleted, false) = false
