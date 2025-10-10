select
    -- Primary key
    id,

    -- Business columns
    registrar_event_id,
    masked_uprn,
    masked_usrn,
    masked_postcode,
    address_format_quality,
    post_code_quality,
    matched_with_assign,
    qualifier,
    uprn_property_classification,
    algorithm,
    match_pattern,
    lds_id,
    lds_registrar_event_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_patient_uprn') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
