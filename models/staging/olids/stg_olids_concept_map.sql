{{ config(
    materialized='table',
    tags=['staging', 'olids', 'reference'],
    cluster_by=['source_concept_id']
) }}

select distinct
    source_concept_id,
    target_concept_id,
    is_primary,
    equivalence,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    is_active,
    source_code,
    source_display,
    source_system,
    target_code,
    target_display,
    target_system

    -- TODO(olids-2026): expose new upstream columns
    -- mapped_item_id,
    -- lakehouse_datetime_updated,
    -- lakehouse_date_processed,
    -- lds_is_deleted,
    -- lds_start_datetime,
from {{ ref('raw_olids_concept_map') }}
