select
    -- Primary key
    id,

    -- Business columns
    lds_id,
    lds_datetime_data_acquired,
    lds_dataset_id,
    patient_id,
    person_id,

    -- Metadata
    lds_start_date_time,
    lds_record_id

from {{ ref('raw_olids_patient_person') }}
qualify row_number() over (
    partition by id
    order by lds_start_date_time desc
) = 1
