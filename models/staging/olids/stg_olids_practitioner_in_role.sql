select
    -- Primary key
    id,

    -- Business columns
    practitioner_id,
    employer_organisation_id,
    role_code,
    role,
    date_employment_start,
    date_employment_end,
    lds_id,
    -- practitioner_in_role doesn't have publisher_organisation_code in the new
    -- schema (the source column has been removed for this table).
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_practitioner_in_role') }}
where coalesce(lds_is_deleted, false) = false
