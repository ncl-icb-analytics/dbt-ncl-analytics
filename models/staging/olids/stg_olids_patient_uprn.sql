select
    -- Primary key
    id,

    -- Business columns
    lds_registrar_event_id,
    TO_VARCHAR(masked_uprn) AS masked_uprn,
    TO_VARCHAR(masked_usrn) AS masked_usrn,
    TO_VARCHAR(masked_postcode) AS masked_postcode,
    address_format_quality,
    postcode_quality,
    matched_with_assign,
    qualifier,
    classification,
    algorithm,
    match_pattern,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

    -- TODO(olids-2026): expose new upstream columns
    -- patient_address_id,
from {{ ref('raw_olids_patient_uprn') }}
where coalesce(lds_is_deleted, false) = false
