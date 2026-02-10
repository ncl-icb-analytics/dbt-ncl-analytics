select
    -- Primary key
    id,

    -- Business columns
    organisation_id,
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    unique_booking_reference_number,
    clinical_effective_date,
    date_precision_concept_id,
    requester_organisation_id,
    recipient_organisation_id,
    referral_request_priority_concept_id,
    referral_request_type_concept_id,
    referral_request_specialty_concept_id,
    mode,
    is_outgoing_referral,
    is_review,
    referral_request_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    date_recorded,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_referral_request') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
