{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.CONCEPT_MAP \ndbt: source(''olids'', ''CONCEPT_MAP'') \nColumns:\n  MAPPED_ITEM_ID -> mapped_item_id\n  CONCEPT_MAP_ID -> concept_map_id\n  CONCEPT_MAP_RESOURCE_ID -> concept_map_resource_id\n  CONCEPT_MAP_URL -> concept_map_url\n  CONCEPT_MAP_VERSION -> concept_map_version\n  SOURCE_CONCEPT_ID -> source_concept_id\n  SOURCE_SYSTEM -> source_system\n  SOURCE_CODE -> source_code\n  SOURCE_DISPLAY -> source_display\n  TARGET_CONCEPT_ID -> target_concept_id\n  TARGET_SYSTEM -> target_system\n  TARGET_CODE -> target_code\n  TARGET_DISPLAY -> target_display\n  IS_PRIMARY -> is_primary\n  IS_ACTIVE -> is_active\n  EQUIVALENCE -> equivalence\n  LDS_START_DATETIME -> lds_start_datetime"
    )
}}
select
    "MAPPED_ITEM_ID" as mapped_item_id,
    "CONCEPT_MAP_ID" as concept_map_id,
    "CONCEPT_MAP_RESOURCE_ID" as concept_map_resource_id,
    "CONCEPT_MAP_URL" as concept_map_url,
    "CONCEPT_MAP_VERSION" as concept_map_version,
    "SOURCE_CONCEPT_ID" as source_concept_id,
    "SOURCE_SYSTEM" as source_system,
    "SOURCE_CODE" as source_code,
    "SOURCE_DISPLAY" as source_display,
    "TARGET_CONCEPT_ID" as target_concept_id,
    "TARGET_SYSTEM" as target_system,
    "TARGET_CODE" as target_code,
    "TARGET_DISPLAY" as target_display,
    "IS_PRIMARY" as is_primary,
    "IS_ACTIVE" as is_active,
    "EQUIVALENCE" as equivalence,
    "LDS_START_DATETIME" as lds_start_datetime
from {{ source('olids', 'CONCEPT_MAP') }}
