select
    -- Primary key
    id,

    -- Business columns
    organisation_code,
    -- NB: 'assinging' (sic) is the upstream column name in DATA_LAKE.OLIDS
    -- (issue #747). Alias here is the project's clean boundary — downstream
    -- consumers see the corrected spelling. Raw layer keeps the typo so it
    -- stays a 1:1 source passthrough.
    organisation_code_assinging_authority as organisation_code_assigning_authority,
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
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    description,
    location_type_source_concept_id
from {{ ref('raw_olids_organisation') }}
where coalesce(lds_is_deleted, false) = false
