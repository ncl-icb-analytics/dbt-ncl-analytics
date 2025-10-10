select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    clinical_effective_date,
    date_precision_concept_id,
    date_recorded,
    description,
    procedure_source_concept_id,
    status_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    is_confidential,
    is_deleted,
    lds_end_date_time,
    lds_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_procedure_request') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
