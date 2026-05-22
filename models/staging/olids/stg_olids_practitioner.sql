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
    -- practitioner doesn't have publisher_organisation_code in the new schema —
    -- the cross-cutting record_owner_organisation_code rename doesn't apply to
    -- this table because the source column has been removed entirely.
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_practitioner') }}
where coalesce(lds_is_deleted, false) = false
