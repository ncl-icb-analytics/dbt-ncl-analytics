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
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_practitioner') }}
where coalesce(lds_is_deleted, false) = false
