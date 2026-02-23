select
    -- Primary key
    id,

    -- Business columns
    organisation_id_publisher,
    organisation_id_managing,
    patient_id,
    person_id,
    episode_type_source_concept_id,
    episode_type_code,
    episode_type_display,
    episode_type_source_code,
    episode_type_source_display,
    episode_status_source_concept_id,
    episode_status_code,
    episode_status_display,
    episode_status_source_code,
    episode_status_source_display,
    episode_of_care_start_date,
    episode_of_care_end_date,
    care_manager_practitioner_id,
    lds_id,
    organisation_code_publisher,
    organisation_code_managing,
    lds_datetime_first_acquired,
    lds_datetime_update_acquired,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_episode_of_care') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
