select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    practitioner_id,
    encounter_id,
    clinical_status,
    verification_status,
    category,
    clinical_effective_date,
    date_precision_concept_id,
    is_review,
    medication_name,
    multi_lex_action,
    allergy_intolerance_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    date_recorded,
    is_confidential,
    person_id,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_allergy_intolerance') }}
where coalesce(lds_is_deleted, false) = false
