{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.CONCEPT_MAP \ndbt: source(''olids'', ''CONCEPT_MAP'') \nColumns:\n  ID -> id\n  LDS_ID -> lds_id\n  LDS_BUSINESS_KEY -> lds_business_key\n  LDS_DATASET_ID -> lds_dataset_id\n  CONCEPT_MAP_ID -> concept_map_id\n  CONCEPT_MAP_RESOURCE_ID -> concept_map_resource_id\n  CONCEPT_MAP_URL -> concept_map_url\n  CONCEPT_MAP_VERSION -> concept_map_version\n  SOURCE_CODE_ID -> source_code_id\n  SOURCE_SYSTEM -> source_system\n  SOURCE_CODE -> source_code\n  SOURCE_DISPLAY -> source_display\n  TARGET_CODE_ID -> target_code_id\n  TARGET_SYSTEM -> target_system\n  TARGET_CODE -> target_code\n  TARGET_DISPLAY -> target_display\n  IS_PRIMARY -> is_primary\n  IS_ACTIVE -> is_active\n  EQUIVALENCE -> equivalence\n  LDS_START_DATE_TIME -> lds_start_date_time"
    )
}}
select
    "ID" as id,
    "LDS_ID" as lds_id,
    "LDS_BUSINESS_KEY" as lds_business_key,
    "LDS_DATASET_ID" as lds_dataset_id,
    "CONCEPT_MAP_ID" as concept_map_id,
    "CONCEPT_MAP_RESOURCE_ID" as concept_map_resource_id,
    "CONCEPT_MAP_URL" as concept_map_url,
    "CONCEPT_MAP_VERSION" as concept_map_version,
    "SOURCE_CODE_ID" as source_code_id,
    "SOURCE_SYSTEM" as source_system,
    "SOURCE_CODE" as source_code,
    "SOURCE_DISPLAY" as source_display,
    "TARGET_CODE_ID" as target_code_id,
    "TARGET_SYSTEM" as target_system,
    "TARGET_CODE" as target_code,
    "TARGET_DISPLAY" as target_display,
    "IS_PRIMARY" as is_primary,
    "IS_ACTIVE" as is_active,
    "EQUIVALENCE" as equivalence,
    "LDS_START_DATE_TIME" as lds_start_date_time
from {{ source('olids', 'CONCEPT_MAP') }}
