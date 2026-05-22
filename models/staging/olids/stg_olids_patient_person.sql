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
    lds_record_id_person,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    lds_end_datetime,
    lds_registrar_event_id,
    lds_datetime_update_acquired_person
from {{ ref('raw_olids_patient_person') }}
-- OLIDS 2026: source-side `id` removed; the natural key is (patient_id, person_uuid).
-- Filter every partitioning column so null components can't silently collapse
-- unrelated rows into the same partition.
where patient_id is not null
  and person_uuid is not null
  and person_id is not null
qualify row_number() over (
    partition by patient_id, person_uuid
    -- lds_source_record_id is a deterministic tiebreaker when two rows
    -- share an lds_start_datetime — keeps the dedup output stable across runs.
    order by lds_start_datetime desc, lds_source_record_id desc
) = 1
