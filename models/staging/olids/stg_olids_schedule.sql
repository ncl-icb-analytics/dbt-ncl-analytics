{{ config(materialized='table') }}

-- Materialised as a table because the qualify row_number() dedup below would
-- otherwise re-run on every downstream query (default for staging is view).
select
    -- Primary key
    id,

    -- Business columns
    location_id,
    location_name,
    practitioner_id,
    start_date,
    end_date,
    type,
    name,
    is_private,
    lds_id,
    publisher_organisation_code,
    lds_datetime_first_acquired,

    -- Metadata
    lds_start_datetime,
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_schedule') }}
where coalesce(lds_is_deleted, false) = false
qualify row_number() over (
    partition by id
    order by lds_start_datetime desc
) = 1
