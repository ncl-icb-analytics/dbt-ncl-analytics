select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    description,
    contact_type_concept_id,
    start_date,
    end_date,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_patient_contact') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
