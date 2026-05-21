select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    publisher_organisation_id,
    episode_of_care_id,
    start_date,
    end_date,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_patient_registered_practitioner_in_role') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
