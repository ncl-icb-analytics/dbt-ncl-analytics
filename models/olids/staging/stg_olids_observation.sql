select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    person_id,
    encounter_id,
    practitioner_id,
    parent_observation_id,
    clinical_effective_date,
    date_precision_concept_id,
    result_value,
    result_value_unit_concept_id,
    result_date,
    result_text,
    is_problem,
    is_review,
    problem_end_date,
    observation_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    episodicity_concept_id,
    is_primary,
    date_recorded,
    is_deleted,
    is_problem_deleted,
    is_confidential,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    result_unit_code,
    result_unit_display,

    -- Metadata
    lds_start_date_time,
    lds_record_id

from {{ ref('raw_olids_observation') }}
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
