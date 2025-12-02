select
    -- Primary key
    id,

    -- Business columns
    name,
    type_code,
    type_desc,
    is_primary_location,
    house_name,
    house_number,
    house_name_flat_number,
    street,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    postcode,
    managing_organisation_id,
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

from {{ ref('raw_olids_location') }}
where coalesce(lds_is_deleted, false) = false
