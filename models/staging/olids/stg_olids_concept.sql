{{ config(
    materialized='table',
    tags=['staging', 'olids', 'reference']
) }}

select
    -- Primary key

    -- Business columns
    system,
    code,
    display,
    is_mapped,
    use_count

    -- TODO(olids-2026): expose new upstream columns
    -- concept_id,
    -- lds_is_deleted,
    -- lds_start_datetime,
from {{ ref('raw_olids_concept') }}
