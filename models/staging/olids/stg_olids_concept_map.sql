{{ config(
    materialized='table',
    tags=['staging', 'olids', 'reference'],
    cluster_by=['source_code_id']
) }}

select distinct
    source_code_id,
    target_code_id,
    is_primary,
    equivalence

from {{ ref('raw_olids_concept_map') }}
