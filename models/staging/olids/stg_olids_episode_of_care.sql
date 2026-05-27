select
    -- Primary key
    id,

    -- Business columns
    publisher_organisation_id,
    provider_organisation_id,
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
    care_manager_practitioner_in_role_id,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,
    lds_datetime_update_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    care_manager_organisation_id,
    care_manager_organisation_code,
    author_organisation_id
from {{ ref('raw_olids_episode_of_care') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
