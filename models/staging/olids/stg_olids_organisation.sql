select
    -- Primary key
    id,

    -- Business columns
    organisation_code,
    assigning_authority_code,
    name,
    type_code,
    type_desc,
    postcode,
    parent_organisation_id,
    open_date,
    close_date,
    is_obsolete,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_organisation') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
