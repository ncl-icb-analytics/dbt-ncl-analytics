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
    target_system,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    mapped_item_id,
    concept_map_id,
    lds_start_datetime
from {{ ref('raw_olids_concept_map') }}
