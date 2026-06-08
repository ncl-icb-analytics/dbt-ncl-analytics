-- Trimmed to match the actual ENRICHED_CONCEPT_MAP column set in
-- DATA_LAKE.OLIDS post-2026 realignment (issue #747). The old SELECT
-- listed id / lds_id / lds_source_dataset_id, none of which exist on
-- the new stable model — only the columns below are present.
select
    mapped_item_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_concept_id,
    source_system,
    source_code,
    source_display,
    target_concept_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lds_start_datetime
from {{ ref('raw_olids_enriched_concept_map') }}
