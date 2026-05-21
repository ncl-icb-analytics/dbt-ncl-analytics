select
    -- Primary key
    id,

    -- Business columns
    organisation_code,
    organisation_code_assinging_authority,
    name,
    postcode,
    parent_organisation_id,
    open_date,
    close_date,
    is_obsolete,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

    -- TODO(olids-2026): expose new upstream columns
    -- description,
    -- location_type_source_concept_id,
from {{ ref('raw_olids_organisation') }}
where coalesce(lds_is_deleted, false) = false
