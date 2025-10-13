select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    encounter_id,
    practitioner_id,
    parent_observation_id,
    clinical_effective_date,
    date_precision_concept_id,
    result_value,
    result_value_units,
    result_date,
    result_text,
    is_problem,
    is_review,
    problem_end_date,
    diagnostic_order_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    episodicity_concept_id,
    is_primary,
    date_recorded,
    is_deleted,
    person_id,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_diagnostic_order') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
