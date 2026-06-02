select
    -- Primary key
    id,

    -- Business columns
    location_id,
    is_primary_contact,
    contact_type,
    contact_type_source_concept_id,
    value,
    lds_id,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_location_contact') }}
where coalesce(lds_is_deleted, false) = false
