select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    organisation_id,
    practitioner_id,
    episode_of_care_id,
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

from {{ ref('raw_olids_patient_registered_practitioner_in_role') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
