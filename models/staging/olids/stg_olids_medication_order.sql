select
    -- Primary key
    id,

    -- Business columns
    organisation_id,
    person_id,
    patient_id,
    medication_statement_id,
    encounter_id,
    practitioner_id,
    observation_id,
    allergy_intolerance_id,
    diagnostic_order_id,
    referral_request_id,
    clinical_effective_date,
    date_precision_concept_id,
    dose,
    quantity_value,
    quantity_unit,
    duration_days,
    estimated_cost,
    medication_name,
    medication_order_source_concept_id,
    bnf_reference,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    issue_method,
    date_recorded,
    is_confidential,
    issue_method_description,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,
    medication_statement_source_concept_id,
    statement_medication_name,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_medication_order') }}
where coalesce(lds_is_deleted, false) = false
