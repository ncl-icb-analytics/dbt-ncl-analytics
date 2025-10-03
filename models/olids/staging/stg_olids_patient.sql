select
    -- Primary key
    id,

    -- Business columns
    nhs_number_hash,
    sk_patient_id,
    title,
    gender_concept_id,
    registered_practice_id,
    birth_year,
    birth_month,
    death_year,
    death_month,
    is_spine_sensitive,
    is_confidential,
    is_dummy_patient,
    record_owner_organisation_code,
    lds_id,
    lds_datetime_data_acquired,
    lds_initial_date_received_date,

    -- Metadata
    lds_start_date_time,
    lds_is_deleted,
    lds_record_id

from {{ ref('raw_olids_patient') }}
where lds_is_deleted = false
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
