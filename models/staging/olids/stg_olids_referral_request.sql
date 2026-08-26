select
    -- Primary key
    id,

    -- Business columns
    -- stable_referral_request exposes publisher_organisation_id (not provider_).
    publisher_organisation_id,
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    unique_booking_reference_number,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    requester_organisation_id,
    recipient_organisation_id,
    referral_request_priority_source_concept_id,
    referral_request_type_source_concept_id,
    referral_request_specialty_source_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    mode,
    is_outgoing_referral,
    is_review,
    referral_request_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    recorded_datetime as date_recorded,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_referral_request') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
