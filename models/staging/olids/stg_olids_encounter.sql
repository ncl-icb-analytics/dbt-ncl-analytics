select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    practitioner_id,
    appointment_id,
    episode_of_care_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    location,
    encounter_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    type,
    sub_type,
    admission_method,
    end_date,
    date_recorded,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

    -- TODO(olids-2026): expose new upstream columns
    -- provider_organisation_id,
    -- author_organisation_id,
from {{ ref('raw_olids_encounter') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
