select
    -- Primary key
    id,

    -- Business columns
    person_id,
    patient_id,
    contact_type_source_concept_id,
    start_date,
    end_date,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    contact_type
from {{ ref('raw_olids_patient_contact') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
