select
    -- Primary key
    id,

    -- Business columns
    organisation_id,
    patient_id,
    person_id,
    episode_type_source_concept_id,
    episode_status_source_concept_id,
    episode_of_care_start_date,
    episode_of_care_end_date,
    care_manager_practitioner_id,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_episode_of_care') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
