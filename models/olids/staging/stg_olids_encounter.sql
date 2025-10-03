select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    practitioner_id,
    appointment_id,
    episode_of_care_id,
    service_provider_organisation_id,
    clinical_effective_date,
    date_precision_concept_id,
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
    is_deleted,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_encounter') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
