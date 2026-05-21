{{
    config(materialized = 'table')
}}

select
    -- Primary key

    -- Business columns
    lds_id,
    lds_datetime_first_acquired,
    lds_source_dataset_id,
    patient_id,
    person_id,
    person_uuid,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated,
    lds_source_record_id,
    lds_record_id_person

    -- TODO(olids-2026): expose new upstream columns
    -- lds_end_datetime,
    -- lds_registrar_event_id,
    -- lds_datetime_update_acquired_person,
from {{ ref('raw_olids_patient_person') }}
where person_id is not null
qualify row_number() over (
    partition by id
    order by lds_start_datetime desc
) = 1
