select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    address_type_source_concept_id,
    postcode_hash,
    start_date,
    end_date,
    person_id,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_source_record_id

from {{ ref('raw_olids_patient_address') }}
where person_id is not null
