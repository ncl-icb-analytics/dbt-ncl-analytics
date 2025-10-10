select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    address_type_concept_id,
    postcode_hash,
    start_date,
    end_date,
    person_id,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_date_received_date,

    -- Metadata
    lds_start_date_time,
    lds_record_id

from {{ ref('raw_olids_patient_address') }}
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
