select
    -- Primary key
    id,

    -- Business columns
    gmc_code,
    title,
    first_name,
    surname,
    name,
    is_obsolete,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_practitioner') }}
where coalesce(lds_is_deleted, false) = false
