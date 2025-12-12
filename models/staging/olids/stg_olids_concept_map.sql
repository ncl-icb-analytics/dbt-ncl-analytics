{{ config(
    materialized='table',
    tags=['staging', 'olids', 'reference'],
    cluster_by=['source_code_id']
) }}

select distinct
    source_code_id,
    target_code_id,
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

from {{ ref('raw_olids_concept_map') }}
